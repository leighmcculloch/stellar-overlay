//! stellar-txsub - Submit transactions to the Stellar overlay network.
//!
//! This CLI tool connects to a Stellar Core node and submits transactions
//! directly via the peer-to-peer overlay protocol.

mod crypto;
mod framing;
mod handshake;
mod session;

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::io::{self, IsTerminal, Read};
use std::time::Duration;
use stellar_xdr::curr::{ReadXdr, StellarMessage, TransactionEnvelope};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::crypto::{network_id, LOCAL_PASSPHRASE, MAINNET_PASSPHRASE, TESTNET_PASSPHRASE};
use crate::handshake::handshake;

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

    // Resolve network passphrase and default peer
    let network_lower = args.network.to_lowercase();
    let (passphrase, default_peer) = match network_lower.as_str() {
        "testnet" | "test" => (TESTNET_PASSPHRASE, "core-testnet1.stellar.org:11625"),
        "mainnet" | "main" | "pubnet" | "public" => {
            (MAINNET_PASSPHRASE, "core-live-a.stellar.org:11625")
        }
        "local" => (LOCAL_PASSPHRASE, "localhost:11625"),
        _ => (&*args.network, "localhost:11625"),
    };

    // Use specified peer or default for the network
    let peer = if args.peer.is_some() {
        args.peer.as_ref().unwrap()
    } else {
        default_peer
    };

    // Connect to peer
    eprintln!("ℹ️ Connecting to {}", peer);
    let stream = TcpStream::connect(peer)
        .await
        .context("Failed to connect to peer")?;
    eprintln!("✅ Connected");

    // Compute network ID
    let net_id = network_id(passphrase);

    // Perform handshake
    eprintln!("ℹ️ Performing handshake");
    let mut session = handshake(stream, net_id.clone(), LOCAL_LISTENING_PORT).await?;
    eprintln!("✅ Authenticated");

    // Send transaction
    let tx_hash = tx_envelope.hash(net_id.0).expect("Failed to hash transaction");
    eprintln!("➡️ TRANSACTION: hash={}", hex::encode(tx_hash));
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
                "ERROR_MSG: {:?} - {}",
                e.code,
                String::from_utf8_lossy(&e.msg.to_vec())
            )
        }
        StellarMessage::Hello(h) => {
            format!(
                "HELLO: ledger_version={}, overlay_version={}, version_str={}",
                h.ledger_version,
                h.overlay_version,
                String::from_utf8_lossy(&h.version_str.to_vec())
            )
        }
        StellarMessage::Auth(a) => {
            format!("AUTH: flags={}", a.flags)
        }
        StellarMessage::SendMore(s) => {
            format!("SEND_MORE: num_messages={}", s.num_messages)
        }
        StellarMessage::SendMoreExtended(s) => {
            format!(
                "SEND_MORE_EXTENDED: num_messages={}, num_bytes={}",
                s.num_messages, s.num_bytes
            )
        }
        StellarMessage::Transaction(_) => "TRANSACTION".to_string(),
        StellarMessage::DontHave(dh) => {
            format!("DONT_HAVE: type={:?}", dh.type_)
        }
        StellarMessage::Peers(peers) => {
            format!("PEERS: count={}", peers.len())
        }
        StellarMessage::GetTxSet(_) => "GET_TX_SET".to_string(),
        StellarMessage::TxSet(_) => "TX_SET".to_string(),
        StellarMessage::GeneralizedTxSet(_) => "GENERALIZED_TX_SET".to_string(),
        StellarMessage::GetScpQuorumset(_) => "GET_SCP_QUORUMSET".to_string(),
        StellarMessage::ScpQuorumset(qs) => {
            format!("SCP_QUORUMSET: threshold={}", qs.threshold)
        }
        StellarMessage::ScpMessage(scp) => {
            format!("SCP_MESSAGE: slot={}", scp.statement.slot_index)
        }
        StellarMessage::GetScpState(ledger) => {
            format!("GET_SCP_STATE: ledger={}", ledger)
        }
        StellarMessage::FloodAdvert(advert) => {
            format!("FLOOD_ADVERT: tx_hashes={}", advert.tx_hashes.len())
        }
        StellarMessage::FloodDemand(demand) => {
            format!("FLOOD_DEMAND: tx_hashes={}", demand.tx_hashes.len())
        }
        StellarMessage::TimeSlicedSurveyRequest(_) => "TIME_SLICED_SURVEY_REQUEST".to_string(),
        StellarMessage::TimeSlicedSurveyResponse(_) => "TIME_SLICED_SURVEY_RESPONSE".to_string(),
        StellarMessage::TimeSlicedSurveyStartCollecting(_) => {
            "TIME_SLICED_SURVEY_START_COLLECTING".to_string()
        }
        StellarMessage::TimeSlicedSurveyStopCollecting(_) => {
            "TIME_SLICED_SURVEY_STOP_COLLECTING".to_string()
        }
    }
}
