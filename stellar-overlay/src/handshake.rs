//! Peer handshake implementation for the Stellar overlay protocol.

use crate::crypto::{
    create_auth_cert, derive_receiving_mac_key, derive_sending_mac_key, ecdh_shared_secret,
    generate_nonce, EcdhKeypair, NodeIdentity,
};
use crate::session::{recv_unauthenticated, send_unauthenticated, PeerSession};
use stellar_xdr::curr::{Auth, ErrorCode, Hash, Hello, NodeId, StellarMessage};
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

/// Errors that can occur during handshake or session operations.
///
/// This error type covers failures during the initial handshake as well as
/// errors that occur when sending or receiving messages on an established session.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error during session operations (sending/receiving messages).
    #[error(transparent)]
    Session(#[from] crate::session::Error),

    /// Peer sent an error message.
    #[error("peer sent error: {code:?} - {message}")]
    PeerError {
        /// The error code from the peer.
        code: ErrorCode,
        /// The error message from the peer.
        message: String,
    },

    /// Received unexpected message type.
    #[error("expected {expected}, got {got}")]
    UnexpectedMessage {
        /// The expected message type.
        expected: &'static str,
        /// The actual message type received.
        got: String,
    },

    /// Peer's overlay protocol version is too old.
    #[error("peer overlay version {version} is too old (min: {min})")]
    OverlayVersionTooOld {
        /// The peer's overlay version.
        version: u32,
        /// The minimum supported version.
        min: u32,
    },

    /// Network ID does not match expected value.
    #[error("network ID mismatch")]
    NetworkIdMismatch,

    /// System time is before UNIX epoch.
    #[error("system time before UNIX epoch")]
    SystemTime,
}

/// Log entries emitted during the handshake process.
///
/// Pass a callback to [`handshake`] to receive these log entries for
/// debugging or status reporting.
///
/// # Example
///
/// ```no_run
/// use stellar_overlay::{handshake, Log, network_id};
/// use tokio::net::TcpStream;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = TcpStream::connect("core-testnet1.stellar.org:11625").await?;
/// let network = network_id("Test SDF Network ; September 2015");
///
/// let session = handshake(stream, network, 11625, |log| {
///     match log {
///         Log::Sending(msg) => println!("-> {}", msg),
///         Log::Received(msg) => println!("<- {}", msg),
///         Log::Error(msg) => eprintln!("Error: {}", msg),
///     }
/// }).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub enum Log {
    /// A protocol message is being sent to the peer.
    Sending(String),
    /// A protocol message was received from the peer.
    Received(String),
    /// An error message was received from the peer.
    Error(String),
}

/// Perform an authenticated handshake with a Stellar Core peer.
///
/// This function performs the Stellar overlay protocol handshake:
/// 1. Exchange HELLO messages to negotiate protocol versions
/// 2. Derive shared MAC keys using ECDH key exchange
/// 3. Exchange AUTH messages to complete authentication
///
/// On success, returns a [`PeerSession`] that can be used to send and receive
/// protocol messages.
///
/// # Arguments
///
/// * `stream` - A TCP connection to a Stellar Core node
/// * `network_id` - The network ID (hash of network passphrase). Use [`network_id`](crate::network_id) to compute this.
/// * `listening_port` - The port to advertise in the HELLO message (typically 11625)
/// * `log` - Callback invoked with log entries during the handshake
///
/// # Errors
///
/// Returns an error if:
/// - The peer sends an error message
/// - The peer's protocol version is incompatible
/// - The network ID doesn't match
/// - A network or protocol error occurs
///
/// # Example
///
/// ```no_run
/// use stellar_overlay::{handshake, Log, network_id, PeerSession};
/// use tokio::net::TcpStream;
///
/// async fn connect_to_testnet() -> Result<PeerSession, Box<dyn std::error::Error>> {
///     let stream = TcpStream::connect("core-testnet1.stellar.org:11625").await?;
///     let network = network_id("Test SDF Network ; September 2015");
///
///     let session = handshake(stream, network, 11625, |log| {
///         println!("{:?}", log);
///     }).await?;
///
///     Ok(session)
/// }
/// ```
pub async fn handshake(
    mut stream: TcpStream,
    network_id: Hash,
    listening_port: i32,
    mut log: impl FnMut(Log),
) -> Result<PeerSession, Error> {
    // Generate crypto material
    let node_identity = NodeIdentity::generate();
    let ecdh_keypair = EcdhKeypair::generate();
    let local_nonce = generate_nonce();

    // Calculate expiration time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::SystemTime)?
        .as_secs();
    let expiration = now + AUTH_CERT_EXPIRATION_SECONDS;

    // Create auth certificate
    let auth_cert = create_auth_cert(&network_id, &node_identity, &ecdh_keypair, expiration);

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
    log(Log::Sending(format!(
        "Hello: ledger_version={}, overlay_version={}, version_str={}",
        LEDGER_PROTOCOL_VERSION, OVERLAY_PROTOCOL_VERSION, VERSION_STR
    )));
    send_unauthenticated(&mut stream, hello_msg)
        .await
        .map_err(Error::Session)?;

    // Receive HELLO from peer
    let peer_hello = recv_unauthenticated(&mut stream)
        .await
        .map_err(Error::Session)?;
    let peer_hello = match peer_hello {
        StellarMessage::Hello(h) => {
            log(Log::Received(format!(
                "Hello: ledger_version={}, overlay_version={}, version_str={}",
                h.ledger_version,
                h.overlay_version,
                String::from_utf8_lossy(&h.version_str.to_vec())
            )));
            h
        }
        StellarMessage::ErrorMsg(e) => {
            let message = String::from_utf8_lossy(&e.msg.to_vec()).to_string();
            log(Log::Error(format!("ErrorMsg: {:?} - {}", e.code, message)));
            return Err(Error::PeerError {
                code: e.code,
                message,
            });
        }
        other => {
            return Err(Error::UnexpectedMessage {
                expected: "Hello",
                got: other.name().to_string(),
            });
        }
    };

    // Verify peer's overlay version
    if peer_hello.overlay_version < OVERLAY_PROTOCOL_MIN_VERSION {
        return Err(Error::OverlayVersionTooOld {
            version: peer_hello.overlay_version,
            min: OVERLAY_PROTOCOL_MIN_VERSION,
        });
    }

    // Verify network ID matches
    if peer_hello.network_id != network_id {
        return Err(Error::NetworkIdMismatch);
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
    log(Log::Sending(format!(
        "Auth: flags={}",
        AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED
    )));
    session.send_message(auth_msg).await.map_err(Error::Session)?;

    // Receive response (could be AUTH, SEND_MORE_EXTENDED, or ERROR)
    let response = session.recv().await.map_err(Error::Session)?;
    match response {
        StellarMessage::Auth(a) => {
            log(Log::Received(format!("Auth: flags={}", a.flags)));
            Ok(session)
        }
        StellarMessage::SendMoreExtended(s) => {
            log(Log::Received(format!(
                "SendMoreExtended: num_messages={}, num_bytes={}",
                s.num_messages, s.num_bytes
            )));
            // Peer sent SEND_MORE_EXTENDED before AUTH, which is valid
            // Wait for AUTH
            let auth_response = session.recv().await.map_err(Error::Session)?;
            match auth_response {
                StellarMessage::Auth(a) => {
                    log(Log::Received(format!("Auth: flags={}", a.flags)));
                    Ok(session)
                }
                StellarMessage::ErrorMsg(e) => {
                    let message = String::from_utf8_lossy(&e.msg.to_vec()).to_string();
                    log(Log::Error(format!("ErrorMsg: {:?} - {}", e.code, message)));
                    Err(Error::PeerError {
                        code: e.code,
                        message,
                    })
                }
                other => Err(Error::UnexpectedMessage {
                    expected: "Auth",
                    got: other.name().to_string(),
                }),
            }
        }
        StellarMessage::ErrorMsg(e) => {
            let message = String::from_utf8_lossy(&e.msg.to_vec()).to_string();
            log(Log::Error(format!("ErrorMsg: {:?} - {}", e.code, message)));
            Err(Error::PeerError {
                code: e.code,
                message,
            })
        }
        other => Err(Error::UnexpectedMessage {
            expected: "Auth or SendMoreExtended",
            got: other.name().to_string(),
        }),
    }
}
