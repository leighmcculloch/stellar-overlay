//! txsub - Submit transactions to the Stellar overlay network.
//!
//! This CLI tool connects to a Stellar Core node and submits transactions
//! directly via the peer-to-peer overlay protocol.
//!
//! Usage:
//!   echo "BASE64_ENCODED_TX" | txsub
//!
//! The tool reads a base64-encoded transaction envelope from stdin,
//! connects to the Stellar Testnet, performs the peer handshake,
//! and sends the transaction.

mod crypto;
mod framing;
mod handshake;
mod session;

use anyhow::{bail, Context, Result};
use std::io::{self, Read};
use stellar_xdr::curr::{ReadXdr, StellarMessage, TransactionEnvelope};
use tokio::net::TcpStream;

use crate::crypto::{network_id, TESTNET_PASSPHRASE};
use crate::handshake::handshake;

/// Default peer to connect to (Stellar Testnet).
const DEFAULT_PEER: &str = "core-testnet1.stellar.org:11625";

/// Local listening port to advertise (not actually listening).
const LOCAL_LISTENING_PORT: i32 = 11625;

#[tokio::main]
async fn main() -> Result<()> {
    // Read transaction from stdin
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .context("Failed to read from stdin")?;

    let input = input.trim();
    if input.is_empty() {
        bail!("No transaction provided on stdin");
    }

    // Decode base64
    let tx_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, input)
        .context("Failed to decode base64")?;

    // Parse as TransactionEnvelope
    let tx_envelope =
        TransactionEnvelope::from_xdr(&tx_bytes, stellar_xdr::curr::Limits::none())
            .context("Failed to parse transaction envelope")?;

    // Connect to peer
    eprintln!("Connecting to {}...", DEFAULT_PEER);
    let stream = TcpStream::connect(DEFAULT_PEER)
        .await
        .context("Failed to connect to peer")?;

    // Compute network ID
    let net_id = network_id(TESTNET_PASSPHRASE);

    // Perform handshake
    eprintln!("Performing handshake...");
    let mut session = handshake(stream, net_id, LOCAL_LISTENING_PORT).await?;

    eprintln!("Authenticated successfully");

    // Send transaction
    eprintln!("Sending transaction...");
    let tx_msg = StellarMessage::Transaction(tx_envelope);
    session.send_message(tx_msg).await?;

    eprintln!("Transaction sent successfully");

    // Clean exit - transaction was accepted (no ERROR_MSG received during handshake)
    Ok(())
}
