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
const OVERLAY_PROTOCOL_VERSION: u32 = 38;

/// Minimum overlay protocol version we support.
const OVERLAY_PROTOCOL_MIN_VERSION: u32 = 35;

/// Current ledger protocol version.
const LEDGER_PROTOCOL_VERSION: u32 = 25;

/// Auth message flag indicating flow control in bytes is requested.
const AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED: i32 = 200;

/// Auth certificate expiration (1 hour).
const AUTH_CERT_EXPIRATION_SECONDS: u64 = 3600;

/// Version string for HELLO message.
const VERSION_STR: &str = concat!("stellar-overlay ", env!("CARGO_PKG_VERSION"));

/// Events emitted during the handshake process.
#[derive(Debug, Clone)]
pub enum Event {
    /// A message is being sent to the peer.
    Sending(String),
    /// A message was received from the peer.
    Received(String),
    /// An error message was received from the peer.
    Error(String),
}

/// Perform the peer handshake and return an authenticated session.
///
/// The `log` callback is invoked with events during the handshake process.
pub async fn handshake(
    mut stream: TcpStream,
    network_id: Hash,
    listening_port: i32,
    mut log: impl FnMut(Event),
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
        ledger_version: LEDGER_PROTOCOL_VERSION,
        overlay_version: OVERLAY_PROTOCOL_VERSION,
        overlay_min_version: OVERLAY_PROTOCOL_MIN_VERSION,
        network_id: network_id.clone(),
        version_str: VERSION_STR.to_string().try_into().unwrap(),
        listening_port,
        peer_id: NodeId(node_identity.to_public_key()),
        cert: auth_cert,
        nonce: local_nonce.clone(),
    };

    // Send HELLO
    let hello_msg = StellarMessage::Hello(hello);
    log(Event::Sending(format!(
        "Hello: ledger_version={}, overlay_version={}, version_str={}",
        LEDGER_PROTOCOL_VERSION, OVERLAY_PROTOCOL_VERSION, VERSION_STR
    )));
    send_unauthenticated(&mut stream, hello_msg).await?;

    // Receive HELLO from peer
    let peer_hello = recv_unauthenticated(&mut stream).await?;
    let peer_hello = match peer_hello {
        StellarMessage::Hello(h) => {
            log(Event::Received(format!(
                "Hello: ledger_version={}, overlay_version={}, version_str={}",
                h.ledger_version,
                h.overlay_version,
                String::from_utf8_lossy(&h.version_str.to_vec())
            )));
            h
        }
        StellarMessage::ErrorMsg(e) => {
            let msg = format!(
                "ErrorMsg: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
            log(Event::Error(msg.clone()));
            bail!("Peer sent {}", msg);
        }
        other => {
            bail!("Expected Hello, got {}", other.name());
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
    log(Event::Sending(format!(
        "Auth: flags={}",
        AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED
    )));
    session.send_message(auth_msg).await?;

    // Receive response (could be AUTH, SEND_MORE_EXTENDED, or ERROR)
    let response = session.recv().await?;
    match response {
        StellarMessage::Auth(a) => {
            log(Event::Received(format!("Auth: flags={}", a.flags)));
            Ok(session)
        }
        StellarMessage::SendMoreExtended(s) => {
            log(Event::Received(format!(
                "SendMoreExtended: num_messages={}, num_bytes={}",
                s.num_messages, s.num_bytes
            )));
            // Peer sent SEND_MORE_EXTENDED before AUTH, which is valid
            // Wait for AUTH
            let auth_response = session.recv().await?;
            match auth_response {
                StellarMessage::Auth(a) => {
                    log(Event::Received(format!("Auth: flags={}", a.flags)));
                    Ok(session)
                }
                StellarMessage::ErrorMsg(e) => {
                    let msg = format!(
                        "ErrorMsg: {:?} - {}",
                        e.code,
                        String::from_utf8_lossy(&e.msg.to_vec())
                    );
                    log(Event::Error(msg.clone()));
                    bail!("Auth failed: {}", msg);
                }
                other => {
                    bail!(
                        "Expected Auth after SendMoreExtended, got {}",
                        other.name()
                    );
                }
            }
        }
        StellarMessage::ErrorMsg(e) => {
            let msg = format!(
                "ErrorMsg: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            );
            log(Event::Error(msg.clone()));
            bail!("Auth failed: {}", msg);
        }
        other => {
            bail!("Expected Auth or SendMoreExtended, got {}", other.name());
        }
    }
}
