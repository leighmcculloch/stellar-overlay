//! stellar-txsub - Submit transactions to the Stellar overlay network.
//!
//! This CLI tool connects to a Stellar Core node and submits transactions
//! directly via the peer-to-peer overlay protocol.

mod network;

use anyhow::{bail, Context, Result};
use clap::Parser;
use network::Network;
use std::io::{self, IsTerminal, Read};
use std::time::Duration;
use stellar_overlay::{handshake, Log};
use stellar_xdr::curr::{ReadXdr, StellarMessage, TransactionEnvelope};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Submit transactions to the Stellar overlay network.
///
/// Reads a base64-encoded transaction envelope from stdin,
/// connects to a Stellar Core node, and submits it via the
/// peer-to-peer overlay protocol.
#[derive(Parser, Debug)]
#[command(name = "stellar-txsub", version, about)]
struct Args {
    /// Peer address to connect to (host:port)
    #[arg(short, long)]
    peer: Option<String>,

    /// Network passphrase (or "testnet" / "mainnet")
    #[arg(short, long, default_value = "testnet")]
    network: String,

    /// Timeout in seconds for waiting for responses
    #[arg(short, long, default_value = "5")]
    timeout: u64,
}

/// Local listening port to advertise (not actually listening).
const LOCAL_LISTENING_PORT: i32 = 11625;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Check if stdin is a TTY (no input piped)
    if io::stdin().is_terminal() {
        bail!("No transaction provided. Pipe a base64-encoded transaction to stdin.\n\nExample: echo <BASE64_TX> | stellar-txsub");
    }

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

    // Resolve network
    let network = Network::from_str(&args.network);
    let peer = args.peer.as_deref().unwrap_or(network.default_peer());

    // Connect to peer
    eprintln!("ℹ️ Connecting to {}", peer);
    let stream = TcpStream::connect(peer)
        .await
        .context("Failed to connect to peer")?;
    eprintln!("✅ Connected");

    // Perform handshake
    eprintln!("ℹ️ Performing handshake");
    let net_id = network.id();
    let mut session = handshake(stream, net_id.clone(), LOCAL_LISTENING_PORT, log_event).await?;
    eprintln!("✅ Authenticated");

    // Send transaction
    let tx_hash = tx_envelope.hash(net_id.0).expect("Failed to hash transaction");
    eprintln!("➡️ Transaction: hash={}", hex::encode(tx_hash));
    let tx_msg = StellarMessage::Transaction(tx_envelope);
    session.send_message(tx_msg).await?;

    // Wait for responses with timeout
    let response_timeout = Duration::from_secs(args.timeout);
    loop {
        match timeout(response_timeout, session.recv()).await {
            Ok(Ok(msg)) => {
                log_incoming(&msg);
                // If we got an error, exit with failure
                if matches!(msg, StellarMessage::ErrorMsg(_)) {
                    std::process::exit(1);
                }
            }
            Ok(Err(e)) => {
                eprintln!("❌ Connection closed: {}", e);
                break;
            }
            Err(_) => {
                eprintln!("ℹ️ Done (timeout)");
                break;
            }
        }
    }

    eprintln!("⚠️ This tool does not confirm if the transaction was successful.");
    eprintln!("⚠️ Use the hash to check the status with a block explorer.");

    Ok(())
}

/// Log a handshake log entry.
fn log_event(log: Log) {
    match log {
        Log::Sending(msg) => eprintln!("➡️ {}", msg),
        Log::Received(msg) => eprintln!("⬅️ {}", msg),
        Log::Error(msg) => eprintln!("❌ {}", msg),
    }
}

/// Log an incoming message.
fn log_incoming(msg: &StellarMessage) {
    let prefix = if matches!(msg, StellarMessage::ErrorMsg(_)) {
        "❌"
    } else {
        "⬅️"
    };
    eprintln!("{} {}", prefix, format_message(msg));
}

/// Format a StellarMessage for display.
fn format_message(msg: &StellarMessage) -> String {
    match msg {
        StellarMessage::ErrorMsg(e) => {
            format!(
                "{}: {:?} - {}",
                msg.name(),
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            )
        }
        StellarMessage::Auth(a) => format!("{}: flags={}", msg.name(), a.flags),
        StellarMessage::SendMore(s) => format!("{}: num_messages={}", msg.name(), s.num_messages),
        StellarMessage::SendMoreExtended(s) => {
            format!(
                "{}: num_messages={}, num_bytes={}",
                msg.name(),
                s.num_messages,
                s.num_bytes
            )
        }
        StellarMessage::Peers(peers) => format!("{}: count={}", msg.name(), peers.len()),
        _ => msg.name().to_string(),
    }
}
