//! Peer session state management.
//!
//! This module provides the PeerSession struct that tracks:
//! - MAC keys for message authentication
//! - Sequence numbers for replay protection
//! - Connection state

use crate::crypto::hmac_sha256;
use crate::framing::{read_message, write_message};
use anyhow::{bail, Result};
use stellar_xdr::curr::{
    AuthenticatedMessage, AuthenticatedMessageV0, HmacSha256Key, HmacSha256Mac, StellarMessage,
    WriteXdr,
};
use tokio::net::TcpStream;

/// A session with an authenticated peer.
pub struct PeerSession {
    /// The TCP connection to the peer.
    pub stream: TcpStream,
    /// MAC key for outgoing messages.
    pub send_mac_key: HmacSha256Key,
    /// MAC key for incoming messages.
    pub recv_mac_key: HmacSha256Key,
    /// Sequence number for outgoing messages.
    pub send_sequence: u64,
    /// Expected sequence number for incoming messages.
    pub recv_sequence: u64,
}

impl PeerSession {
    /// Create a new peer session with the given MAC keys.
    pub fn new(
        stream: TcpStream,
        send_mac_key: HmacSha256Key,
        recv_mac_key: HmacSha256Key,
    ) -> Self {
        Self {
            stream,
            send_mac_key,
            recv_mac_key,
            send_sequence: 0,
            recv_sequence: 0,
        }
    }

    /// Wrap a StellarMessage in an AuthenticatedMessage with proper sequence and MAC.
    pub fn wrap_authenticated(&mut self, msg: StellarMessage) -> AuthenticatedMessage {
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
    pub async fn send(&mut self, msg: AuthenticatedMessage) -> Result<()> {
        write_message(&mut self.stream, &msg).await
    }

    /// Send a StellarMessage (automatically wrapping it in AuthenticatedMessage).
    pub async fn send_message(&mut self, msg: StellarMessage) -> Result<()> {
        let auth_msg = self.wrap_authenticated(msg);
        self.send(auth_msg).await
    }

    /// Receive and verify an authenticated message from the peer.
    pub async fn recv(&mut self) -> Result<StellarMessage> {
        let auth_msg = read_message(&mut self.stream, true).await?;

        match auth_msg {
            AuthenticatedMessage::V0(v0) => {
                // Verify sequence number
                if v0.sequence != self.recv_sequence {
                    bail!(
                        "Unexpected sequence number: expected {}, got {}",
                        self.recv_sequence,
                        v0.sequence
                    );
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
                    bail!("MAC verification failed");
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
pub async fn send_unauthenticated(stream: &mut TcpStream, msg: StellarMessage) -> Result<()> {
    let auth_msg = AuthenticatedMessage::V0(AuthenticatedMessageV0 {
        sequence: 0,
        message: msg,
        mac: HmacSha256Mac { mac: [0u8; 32] },
    });
    write_message(stream, &auth_msg).await
}

/// Receive an unauthenticated message (HELLO).
///
/// These messages are received before authentication is complete,
/// so we don't verify the MAC.
pub async fn recv_unauthenticated(stream: &mut TcpStream) -> Result<StellarMessage> {
    let auth_msg = read_message(stream, false).await?;

    match auth_msg {
        AuthenticatedMessage::V0(v0) => Ok(v0.message),
    }
}
