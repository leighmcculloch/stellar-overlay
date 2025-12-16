//! XDR message framing for the Stellar overlay protocol.
//!
//! Messages are framed with a 4-byte big-endian length prefix.
//! The MSB (continuation bit) is always 0 for complete messages.

use anyhow::{bail, Context, Result};
use stellar_xdr::curr::{AuthenticatedMessage, ReadXdr, WriteXdr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum message size (16 MB as per stellar-core).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum unauthenticated message size (limited before auth complete).
const MAX_UNAUTH_MESSAGE_SIZE: usize = 256 * 1024;

/// Read a framed message from the stream.
///
/// Returns the deserialized AuthenticatedMessage.
pub async fn read_message(
    stream: &mut TcpStream,
    authenticated: bool,
) -> Result<AuthenticatedMessage> {
    // Read the 4-byte length header
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .context("Failed to read message header")?;

    // Parse length (clear continuation bit in MSB)
    let length = u32::from_be_bytes(header) & 0x7FFFFFFF;
    let length = length as usize;

    // Validate length
    let max_size = if authenticated {
        MAX_MESSAGE_SIZE
    } else {
        MAX_UNAUTH_MESSAGE_SIZE
    };
    if length == 0 || length > max_size {
        bail!(
            "Invalid message size: {} (max: {}, authenticated: {})",
            length,
            max_size,
            authenticated
        );
    }

    // Read the message body
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .await
        .context("Failed to read message body")?;

    // Deserialize the XDR message
    let msg = AuthenticatedMessage::from_xdr(&body, stellar_xdr::curr::Limits::none())
        .context("Failed to deserialize XDR message")?;

    Ok(msg)
}

/// Write a framed message to the stream.
///
/// Serializes the message to XDR and prepends a 4-byte length header.
pub async fn write_message(stream: &mut TcpStream, msg: &AuthenticatedMessage) -> Result<()> {
    // Serialize to XDR
    let body = msg.to_xdr(stellar_xdr::curr::Limits::none())?;

    // Validate length
    if body.len() > MAX_MESSAGE_SIZE {
        bail!("Message too large: {} bytes", body.len());
    }

    // Create length header (MSB continuation bit = 0)
    let length = body.len() as u32;
    let header = length.to_be_bytes();

    // Write header and body
    stream
        .write_all(&header)
        .await
        .context("Failed to write message header")?;
    stream
        .write_all(&body)
        .await
        .context("Failed to write message body")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_length_encoding() {
        let length: u32 = 0x1234;
        let header = length.to_be_bytes();
        let decoded = u32::from_be_bytes(header) & 0x7FFFFFFF;
        assert_eq!(length, decoded);
    }
}
