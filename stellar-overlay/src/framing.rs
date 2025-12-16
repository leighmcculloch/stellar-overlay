//! XDR message framing for the Stellar overlay protocol.
//!
//! Messages are framed with a 4-byte big-endian length prefix.
//! The MSB (continuation bit) is always 0 for complete messages.

use stellar_xdr::curr::{AuthenticatedMessage, ReadXdr, WriteXdr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum message size (16 MB as per stellar-core).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum unauthenticated message size (limited before auth complete).
const MAX_UNAUTH_MESSAGE_SIZE: usize = 256 * 1024;

/// Errors that can occur during message framing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to read message header from stream.
    #[error("failed to read message header")]
    ReadHeader(std::io::Error),

    /// Failed to read message body from stream.
    #[error("failed to read message body")]
    ReadBody(std::io::Error),

    /// Failed to write message header to stream.
    #[error("failed to write message header")]
    WriteHeader(std::io::Error),

    /// Failed to write message body to stream.
    #[error("failed to write message body")]
    WriteBody(std::io::Error),

    /// Failed to deserialize XDR message.
    #[error("failed to deserialize XDR message")]
    XdrDeserialize(stellar_xdr::curr::Error),

    /// Failed to serialize XDR message.
    #[error("failed to serialize XDR message")]
    XdrSerialize(stellar_xdr::curr::Error),

    /// Message size is invalid (zero or exceeds maximum).
    #[error("invalid message size: {size} bytes (max: {max})")]
    InvalidMessageSize { size: usize, max: usize },

    /// Message is too large to send.
    #[error("message too large: {size} bytes")]
    MessageTooLarge { size: usize },
}

/// Read a framed message from the stream.
///
/// Returns the deserialized AuthenticatedMessage.
pub(crate) async fn read_message(
    stream: &mut TcpStream,
    authenticated: bool,
) -> Result<AuthenticatedMessage, Error> {
    // Read the 4-byte length header
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(Error::ReadHeader)?;

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
        return Err(Error::InvalidMessageSize {
            size: length,
            max: max_size,
        });
    }

    // Read the message body
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .await
        .map_err(Error::ReadBody)?;

    // Deserialize the XDR message
    let msg = AuthenticatedMessage::from_xdr(&body, stellar_xdr::curr::Limits::none())
        .map_err(Error::XdrDeserialize)?;

    Ok(msg)
}

/// Write a framed message to the stream.
///
/// Serializes the message to XDR and prepends a 4-byte length header.
pub(crate) async fn write_message(stream: &mut TcpStream, msg: &AuthenticatedMessage) -> Result<(), Error> {
    // Serialize to XDR
    let body = msg
        .to_xdr(stellar_xdr::curr::Limits::none())
        .map_err(Error::XdrSerialize)?;

    // Validate length
    if body.len() > MAX_MESSAGE_SIZE {
        return Err(Error::MessageTooLarge { size: body.len() });
    }

    // Create length header (MSB continuation bit = 0)
    let length = body.len() as u32;
    let header = length.to_be_bytes();

    // Write header and body
    stream
        .write_all(&header)
        .await
        .map_err(Error::WriteHeader)?;
    stream
        .write_all(&body)
        .await
        .map_err(Error::WriteBody)?;

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
