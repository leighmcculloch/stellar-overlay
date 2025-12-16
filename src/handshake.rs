//! Peer handshake implementation for the Stellar overlay protocol.
//!
//! The handshake consists of:
//! 1. Exchange HELLO messages
//! 2. Derive MAC keys from ECDH
//! 3. Exchange AUTH messages

use crate::crypto::{
    create_auth_cert, derive_receiving_mac_key, derive_sending_mac_key, ecdh_shared_secret,
    generate_nonce, EcdhKeypair, NodeIdentity,
};
use crate::session::{recv_unauthenticated, send_unauthenticated, PeerSession};
use anyhow::{bail, Context, Result};
use stellar_xdr::curr::{Auth, Hash, Hello, NodeId, StellarMessage};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

/// Current overlay protocol version.
const OVERLAY_PROTOCOL_VERSION: u32 = 36;

/// Minimum overlay protocol version we support.
const OVERLAY_PROTOCOL_MIN_VERSION: u32 = 35;

/// Auth message flag indicating flow control in bytes is requested.
const AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED: i32 = 200;

/// Auth certificate expiration (1 hour).
const AUTH_CERT_EXPIRATION_SECONDS: u64 = 3600;

/// Perform the peer handshake and return an authenticated session.
pub async fn handshake(
    mut stream: TcpStream,
    network_id: Hash,
    listening_port: i32,
) -> Result<PeerSession> {
    // Generate crypto material
    let node_identity = NodeIdentity::generate();
    let ecdh_keypair = EcdhKeypair::generate();
    let local_nonce = generate_nonce();

    // Calculate expiration time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System time before UNIX epoch")?
        .as_secs();
    let expiration = now + AUTH_CERT_EXPIRATION_SECONDS;

    // Create auth certificate
    let auth_cert = create_auth_cert(&network_id, &node_identity, &ecdh_keypair, expiration)?;

    // Build HELLO message
    let hello = Hello {
        ledger_version: 22, // Current ledger protocol version
        overlay_version: OVERLAY_PROTOCOL_VERSION,
        overlay_min_version: OVERLAY_PROTOCOL_MIN_VERSION,
        network_id: network_id.clone(),
        version_str: "txsub/0.1.0".to_string().try_into().unwrap(),
        listening_port,
        peer_id: NodeId(node_identity.to_public_key()),
        cert: auth_cert,
        nonce: local_nonce.clone(),
    };

    // Send HELLO
    let hello_msg = StellarMessage::Hello(hello);
    eprintln!(
        "→ HELLO: ledger_version={}, overlay_version={}, version_str=txsub/0.1.0",
        22, OVERLAY_PROTOCOL_VERSION
    );
    send_unauthenticated(&mut stream, hello_msg).await?;

    // Receive HELLO from peer
    let peer_hello = recv_unauthenticated(&mut stream).await?;
    let peer_hello = match peer_hello {
        StellarMessage::Hello(h) => {
            eprintln!(
                "← HELLO: ledger_version={}, overlay_version={}, version_str={}",
                h.ledger_version,
                h.overlay_version,
                String::from_utf8_lossy(&h.version_str.to_vec())
            );
            h
        }
        StellarMessage::ErrorMsg(e) => {
            eprintln!(
                "❌ ERROR_MSG: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
            bail!(
                "Peer sent ERROR: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
        }
        other => {
            bail!("Expected HELLO, got {:?}", other);
        }
    };

    // Verify peer's overlay version
    if peer_hello.overlay_version < OVERLAY_PROTOCOL_MIN_VERSION {
        bail!(
            "Peer overlay version {} is too old (min: {})",
            peer_hello.overlay_version,
            OVERLAY_PROTOCOL_MIN_VERSION
        );
    }

    // Verify network ID matches
    if peer_hello.network_id != network_id {
        bail!("Network ID mismatch");
    }

    // Derive shared secret and MAC keys
    let shared_key = ecdh_shared_secret(
        &ecdh_keypair.secret,
        &ecdh_keypair.public,
        &peer_hello.cert.pubkey,
        true, // We are the initiator (WE_CALLED_REMOTE)
    );

    let send_mac_key =
        derive_sending_mac_key(&shared_key, &local_nonce, &peer_hello.nonce, true);
    let recv_mac_key =
        derive_receiving_mac_key(&shared_key, &local_nonce, &peer_hello.nonce, true);

    // Create session with MAC keys
    let mut session = PeerSession::new(stream, send_mac_key, recv_mac_key);

    // Send AUTH message
    let auth = Auth {
        flags: AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED,
    };
    let auth_msg = StellarMessage::Auth(auth);
    eprintln!("→ AUTH: flags={}", AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED);
    session.send_message(auth_msg).await?;

    // Receive response (could be AUTH, SEND_MORE_EXTENDED, or ERROR)
    let response = session.recv().await?;
    match response {
        StellarMessage::Auth(a) => {
            eprintln!("← AUTH: flags={}", a.flags);
            Ok(session)
        }
        StellarMessage::SendMoreExtended(s) => {
            eprintln!(
                "← SEND_MORE_EXTENDED: num_messages={}, num_bytes={}",
                s.num_messages, s.num_bytes
            );
            // Peer sent SEND_MORE_EXTENDED before AUTH, which is valid
            // Wait for AUTH
            let auth_response = session.recv().await?;
            match auth_response {
                StellarMessage::Auth(a) => {
                    eprintln!("← AUTH: flags={}", a.flags);
                    Ok(session)
                }
                StellarMessage::ErrorMsg(e) => {
                    eprintln!(
                        "❌ ERROR_MSG: {:?} - {}",
                        e.code,
                        String::from_utf8_lossy(&e.msg.to_vec())
                    );
                    bail!(
                        "Auth failed: {:?} - {}",
                        e.code,
                        String::from_utf8_lossy(&e.msg.to_vec())
                    );
                }
                other => {
                    bail!(
                        "Expected AUTH after SEND_MORE_EXTENDED, got {:?}",
                        message_type(&other)
                    );
                }
            }
        }
        StellarMessage::ErrorMsg(e) => {
            eprintln!(
                "❌ ERROR_MSG: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
            bail!(
                "Auth failed: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
        }
        other => {
            bail!(
                "Expected AUTH or SEND_MORE_EXTENDED, got {:?}",
                message_type(&other)
            );
        }
    }
}

/// Get the message type for error reporting.
fn message_type(msg: &StellarMessage) -> &'static str {
    match msg {
        StellarMessage::ErrorMsg(_) => "ERROR_MSG",
        StellarMessage::Hello(_) => "HELLO",
        StellarMessage::Auth(_) => "AUTH",
        StellarMessage::DontHave(_) => "DONT_HAVE",
        StellarMessage::Peers(_) => "PEERS",
        StellarMessage::GetTxSet(_) => "GET_TX_SET",
        StellarMessage::TxSet(_) => "TX_SET",
        StellarMessage::GeneralizedTxSet(_) => "GENERALIZED_TX_SET",
        StellarMessage::Transaction(_) => "TRANSACTION",
        StellarMessage::GetScpQuorumset(_) => "GET_SCP_QUORUMSET",
        StellarMessage::ScpQuorumset(_) => "SCP_QUORUMSET",
        StellarMessage::ScpMessage(_) => "SCP_MESSAGE",
        StellarMessage::GetScpState(_) => "GET_SCP_STATE",
        StellarMessage::SendMore(_) => "SEND_MORE",
        StellarMessage::SendMoreExtended(_) => "SEND_MORE_EXTENDED",
        StellarMessage::FloodAdvert(_) => "FLOOD_ADVERT",
        StellarMessage::FloodDemand(_) => "FLOOD_DEMAND",
        _ => "UNKNOWN",
    }
}
