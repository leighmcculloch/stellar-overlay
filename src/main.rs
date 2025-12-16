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

use anyhow::{Context, Result};
use std::io::{self, Read};
use std::time::Duration;
use stellar_xdr::curr::{ReadXdr, StellarMessage, TransactionEnvelope};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::crypto::{network_id, TESTNET_PASSPHRASE};
use crate::handshake::handshake;

/// Default peer to connect to (Stellar Testnet).
const DEFAULT_PEER: &str = "core-testnet1.stellar.org:11625";

/// Local listening port to advertise (not actually listening).
const LOCAL_LISTENING_PORT: i32 = 11625;

/// Timeout for waiting for responses after sending transaction.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    // Read transaction from stdin
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .context("Failed to read from stdin")?;

    let input = input.trim();
    if input.is_empty() {
        eprintln!("txsub - Submit transactions to the Stellar overlay network");
        eprintln!();
        eprintln!("Usage: echo <BASE64_TX> | txsub");
        eprintln!();
        eprintln!("Reads a base64-encoded transaction envelope from stdin,");
        eprintln!("connects to the Stellar Testnet, and submits it via the");
        eprintln!("peer-to-peer overlay protocol.");
        std::process::exit(1);
    }

    // Decode base64
    let tx_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, input)
        .context("Failed to decode base64")?;

    // Parse as TransactionEnvelope
    let tx_envelope =
        TransactionEnvelope::from_xdr(&tx_bytes, stellar_xdr::curr::Limits::none())
            .context("Failed to parse transaction envelope")?;

    // Connect to peer
    eprintln!("ℹ️  Connecting to {}", DEFAULT_PEER);
    let stream = TcpStream::connect(DEFAULT_PEER)
        .await
        .context("Failed to connect to peer")?;
    eprintln!("✅ Connected");

    // Compute network ID
    let net_id = network_id(TESTNET_PASSPHRASE);

    // Perform handshake
    eprintln!("ℹ️  Performing handshake");
    let mut session = handshake(stream, net_id, LOCAL_LISTENING_PORT).await?;
    eprintln!("✅ Authenticated");

    // Send transaction
    let tx_msg = StellarMessage::Transaction(tx_envelope);
    log_outgoing(&tx_msg);
    session.send_message(tx_msg).await?;

    // Wait for responses with timeout
    loop {
        match timeout(RESPONSE_TIMEOUT, session.recv()).await {
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
                eprintln!("ℹ️  Done (timeout)");
                break;
            }
        }
    }

    Ok(())
}

/// Log an outgoing message.
fn log_outgoing(msg: &StellarMessage) {
    eprintln!("→ {}", format_message(msg));
}

/// Log an incoming message.
fn log_incoming(msg: &StellarMessage) {
    let prefix = if matches!(msg, StellarMessage::ErrorMsg(_)) {
        "❌"
    } else {
        "←"
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
