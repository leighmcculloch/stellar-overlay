//! Peer session state management.

use crate::crypto::hmac_sha256;
use crate::framing::{read_message, write_message};
use stellar_xdr::curr::{
    AuthenticatedMessage, AuthenticatedMessageV0, HmacSha256Key, HmacSha256Mac, NodeId,
    StellarMessage, WriteXdr,
};
use tokio::net::TcpStream;

/// Information about a connected peer.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The peer's node ID.
    pub node_id: NodeId,
    /// The peer's ledger protocol version.
    pub ledger_version: u32,
    /// The peer's overlay protocol version.
    pub overlay_version: u32,
    /// The peer's version string.
    pub version_str: String,
}

/// Errors that can occur during session operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error during message framing.
    #[error(transparent)]
    Framing(#[from] crate::framing::Error),

    /// Received message has unexpected sequence number.
    #[error("unexpected sequence number: expected {expected}, got {got}")]
    UnexpectedSequence { expected: u64, got: u64 },

    /// MAC verification failed on received message.
    #[error("MAC verification failed")]
    MacVerificationFailed,
}

/// An authenticated session with a Stellar Core peer.
///
/// After a successful [`connect`](crate::connect), you receive a `PeerSession`
/// that can be used to send and receive protocol messages.
///
/// # Example
///
/// ```no_run
/// use stellar_overlay::PeerSession;
/// use stellar_xdr::curr::StellarMessage;
///
/// async fn communicate(session: &mut PeerSession) -> Result<(), stellar_overlay::Error> {
///     // Receive a message from the peer
///     let msg = session.recv().await?;
///     println!("Received: {:?}", msg);
///
///     // Send a message to the peer (e.g., a transaction)
///     // session.send_message(StellarMessage::Transaction(tx)).await?;
///
///     Ok(())
/// }
/// ```
pub struct PeerSession {
    local_node_id: NodeId,
    stream: TcpStream,
    send_mac_key: HmacSha256Key,
    recv_mac_key: HmacSha256Key,
    send_sequence: u64,
    recv_sequence: u64,
    peer_info: PeerInfo,
}

impl PeerSession {
    /// Create a new peer session with the given local node ID, MAC keys, and peer info.
    pub(crate) fn new(
        local_node_id: NodeId,
        stream: TcpStream,
        send_mac_key: HmacSha256Key,
        recv_mac_key: HmacSha256Key,
        peer_info: PeerInfo,
    ) -> Self {
        Self {
            local_node_id,
            stream,
            send_mac_key,
            recv_mac_key,
            send_sequence: 0,
            recv_sequence: 0,
            peer_info,
        }
    }

    /// Get the local node ID (randomly generated for this session).
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Get information about the connected peer.
    pub fn peer_info(&self) -> &PeerInfo {
        &self.peer_info
    }

    /// Wrap a StellarMessage in an AuthenticatedMessage with proper sequence and MAC.
    fn wrap_authenticated(&mut self, msg: StellarMessage) -> AuthenticatedMessage {
        let sequence = self.send_sequence;
        self.send_sequence += 1;

        // Compute MAC over (sequence || message)
        let mut data_to_mac = Vec::new();
        data_to_mac.extend_from_slice(&sequence.to_xdr(stellar_xdr::curr::Limits::none()).unwrap());
        data_to_mac.extend_from_slice(&msg.to_xdr(stellar_xdr::curr::Limits::none()).unwrap());

        let mac = hmac_sha256(&self.send_mac_key, &data_to_mac);

        AuthenticatedMessage::V0(AuthenticatedMessageV0 {
            sequence,
            message: msg,
            mac,
        })
    }

    /// Send an authenticated message to the peer.
    async fn send(&mut self, msg: AuthenticatedMessage) -> Result<(), Error> {
        write_message(&mut self.stream, &msg)
            .await
            .map_err(Error::Framing)
    }

    /// Send a message to the peer.
    ///
    /// The message is automatically wrapped in an authenticated envelope with
    /// the proper sequence number and MAC.
    ///
    /// # Arguments
    ///
    /// * `msg` - The Stellar protocol message to send
    ///
    /// # Example
    ///
    /// ```no_run
    /// use stellar_overlay::PeerSession;
    /// use stellar_xdr::curr::{StellarMessage, TransactionEnvelope};
    ///
    /// async fn send_tx(session: &mut PeerSession, tx: TransactionEnvelope) -> Result<(), Box<dyn std::error::Error>> {
    ///     session.send_message(StellarMessage::Transaction(tx)).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn send_message(&mut self, msg: StellarMessage) -> Result<(), Error> {
        let auth_msg = self.wrap_authenticated(msg);
        self.send(auth_msg).await
    }

    /// Receive a message from the peer.
    ///
    /// Waits for the next message from the peer, verifies its sequence number
    /// and MAC, and returns the unwrapped message.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use stellar_overlay::PeerSession;
    /// use stellar_xdr::curr::StellarMessage;
    /// use std::time::Duration;
    /// use tokio::time::timeout;
    ///
    /// async fn recv_with_timeout(session: &mut PeerSession) -> Option<StellarMessage> {
    ///     match timeout(Duration::from_secs(5), session.recv()).await {
    ///         Ok(Ok(msg)) => Some(msg),
    ///         _ => None,
    ///     }
    /// }
    /// ```
    pub async fn recv(&mut self) -> Result<StellarMessage, Error> {
        let auth_msg = read_message(&mut self.stream, true)
            .await
            .map_err(Error::Framing)?;

        match auth_msg {
            AuthenticatedMessage::V0(v0) => {
                // Verify sequence number
                if v0.sequence != self.recv_sequence {
                    return Err(Error::UnexpectedSequence {
                        expected: self.recv_sequence,
                        got: v0.sequence,
                    });
                }

                // Verify MAC
                let mut data_to_mac = Vec::new();
                data_to_mac.extend_from_slice(
                    &v0.sequence
                        .to_xdr(stellar_xdr::curr::Limits::none())
                        .unwrap(),
                );
                data_to_mac.extend_from_slice(
                    &v0.message
                        .to_xdr(stellar_xdr::curr::Limits::none())
                        .unwrap(),
                );
                let expected_mac = hmac_sha256(&self.recv_mac_key, &data_to_mac);

                if v0.mac.mac != expected_mac.mac {
                    return Err(Error::MacVerificationFailed);
                }

                self.recv_sequence += 1;
                Ok(v0.message)
            }
        }
    }
}

/// Send an unauthenticated message (HELLO or ERROR_MSG).
///
/// These messages are sent before authentication is complete,
/// so they use sequence 0 and a zero MAC.
pub(crate) async fn send_unauthenticated(stream: &mut TcpStream, msg: StellarMessage) -> Result<(), Error> {
    let auth_msg = AuthenticatedMessage::V0(AuthenticatedMessageV0 {
        sequence: 0,
        message: msg,
        mac: HmacSha256Mac { mac: [0u8; 32] },
    });
    write_message(stream, &auth_msg)
        .await
        .map_err(Error::Framing)
}

/// Receive an unauthenticated message (HELLO).
///
/// These messages are received before authentication is complete,
/// so we don't verify the MAC.
pub(crate) async fn recv_unauthenticated(stream: &mut TcpStream) -> Result<StellarMessage, Error> {
    let auth_msg = read_message(stream, false)
        .await
        .map_err(Error::Framing)?;

    match auth_msg {
        AuthenticatedMessage::V0(v0) => Ok(v0.message),
    }
}
