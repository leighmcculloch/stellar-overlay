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
use stellar_overlay::connect;
use std::collections::HashMap;
use sha2::{Digest, Sha256};
use stellar_xdr::curr::{
    DontHave, GeneralizedTransactionSet, Hash, Limits, MessageType, ReadXdr, ScpStatementPledges,
    SendMoreExtended, StellarMessage, StellarValue, TransactionEnvelope, TransactionPhase,
    TxSetComponent, WriteXdr,
};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

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

    /// Timeout in seconds for waiting for confirmation
    #[arg(short, long, default_value = "30")]
    timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up tracing subscriber with custom formatting
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(false)
                .with_span_events(FmtSpan::NONE)
                .event_format(EventFormatter)
                .with_writer(std::io::stderr)
                .with_filter(LevelFilter::DEBUG),
        )
        .init();

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
    let mut session = connect(stream, net_id.clone()).await?;
    eprintln!("✅ Authenticated");

    // Request to receive messages from peer (flow control)
    // Without this, the peer won't send us SCP messages
    eprintln!("➡️ SendMoreExtended: num_messages=1000, num_bytes=10000000");
    let send_more = StellarMessage::SendMoreExtended(SendMoreExtended {
        num_messages: 1000,
        num_bytes: 10_000_000,
    });
    session.send_message(send_more).await?;

    // Send transaction
    let tx_hash = tx_envelope.hash(net_id.0).expect("Failed to hash transaction");
    eprintln!("➡️ Transaction: hash={}", hex::encode(tx_hash));
    let tx_msg = StellarMessage::Transaction(tx_envelope);
    session.send_message(tx_msg).await?;

    // Wait for confirmation with timeout
    // Track which txset hashes we've already requested to avoid duplicates
    let response_timeout = Duration::from_secs(args.timeout);
    let mut requested_txsets: HashMap<[u8; 32], u64> = HashMap::new();

    eprintln!("ℹ️ Waiting for confirmation (timeout={}s)...", args.timeout);

    loop {
        match timeout(response_timeout, session.recv()).await {
            Ok(Ok(msg)) => {
                log_incoming(&msg);

                match &msg {
                    // Error messages are already logged by log_incoming, just continue
                    StellarMessage::ErrorMsg(_) => {}

                    // Handle SCP messages - look for externalize to get txset hash
                    StellarMessage::ScpMessage(_) => {
                        if let Some((ledger_seq, txset_hash)) = extract_txset_hash_from_scp_message(&msg) {
                            // Only request each txset once
                            if !requested_txsets.contains_key(&txset_hash.0) {
                                requested_txsets.insert(txset_hash.0, ledger_seq);
                                eprintln!("➡️ GetTxSet: ledger={}, hash={}", ledger_seq, hex::encode(txset_hash.0));
                                let get_txset_msg = StellarMessage::GetTxSet(txset_hash.0.into());
                                if let Err(e) = session.send_message(get_txset_msg).await {
                                    eprintln!("❌ Failed to request txset: {}", e);
                                }
                            }
                        }
                    }

                    // Handle GeneralizedTxSet - check if our transaction is included
                    StellarMessage::GeneralizedTxSet(txset) => {
                        if txset_contains_tx(txset, &tx_hash, &net_id) {
                            // Look up which ledger this txset belongs to
                            let txset_hash = hash_txset(txset);
                            if let Some(&ledger_seq) = requested_txsets.get(&txset_hash) {
                                eprintln!("✅ Transaction found in ledger {}", ledger_seq);
                            } else {
                                eprintln!("✅ Transaction found in ledger");
                            }
                            return Ok(());
                        }
                    }

                    // Handle legacy TxSet for older protocol versions
                    StellarMessage::TxSet(txset) => {
                        // Check if our transaction is in the legacy txset
                        for tx in txset.txs.iter() {
                            if let Ok(hash) = tx.hash(net_id.0) {
                                if hash == tx_hash {
                                    eprintln!("✅ Transaction found in ledger");
                                    return Ok(());
                                }
                            }
                        }
                    }

                    // Respond to peer requests with DontHave (we're a lightweight client)
                    StellarMessage::GetScpState(_) => {
                        // We don't have SCP state to share - just ignore
                        // (responding here might cause issues as peer expects specific format)
                    }
                    StellarMessage::GetScpQuorumset(hash) => {
                        // Respond with DontHave
                        eprintln!("➡️ DontHave: ScpQuorumset");
                        let dont_have = StellarMessage::DontHave(DontHave {
                            type_: MessageType::ScpQuorumset,
                            req_hash: hash.clone(),
                        });
                        let _ = session.send_message(dont_have).await;
                    }

                    // Replenish flow control when peer sends us flow control messages
                    StellarMessage::SendMore(_) | StellarMessage::SendMoreExtended(_) => {
                        // Peer is indicating flow control - send our own to keep receiving
                        eprintln!("➡️ SendMoreExtended: num_messages=1000, num_bytes=10000000");
                        let send_more = StellarMessage::SendMoreExtended(SendMoreExtended {
                            num_messages: 1000,
                            num_bytes: 10_000_000,
                        });
                        let _ = session.send_message(send_more).await;
                    }

                    _ => {}
                }
            }
            Ok(Err(e)) => {
                eprintln!("❌ Connection closed: {}", e);
                break;
            }
            Err(_) => {
                eprintln!("ℹ️ Timeout reached ({}s)", args.timeout);
                break;
            }
        }
    }

    eprintln!("⚠️ Transaction was not found in a ledger within the timeout period.");
    eprintln!("⚠️ Use the hash to check the status: {}", hex::encode(tx_hash));
    std::process::exit(2)
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
        StellarMessage::ScpMessage(env) => {
            let slot = env.statement.slot_index;
            let pledge_type = match &env.statement.pledges {
                ScpStatementPledges::Prepare(_) => "Prepare",
                ScpStatementPledges::Confirm(_) => "Confirm",
                ScpStatementPledges::Externalize(_) => "Externalize",
                ScpStatementPledges::Nominate(_) => "Nominate",
            };
            format!("{}: slot={}, type={}", msg.name(), slot, pledge_type)
        }
        StellarMessage::GeneralizedTxSet(txset) => {
            let tx_count = count_transactions_in_txset(txset);
            format!("{}: tx_count={}", msg.name(), tx_count)
        }
        StellarMessage::DontHave(dh) => {
            format!("{}: type={:?}", msg.name(), dh.type_)
        }
        _ => msg.name().to_string(),
    }
}

/// Compute the SHA256 hash of a GeneralizedTransactionSet.
fn hash_txset(txset: &GeneralizedTransactionSet) -> [u8; 32] {
    let xdr = txset.to_xdr(Limits::none()).expect("Failed to encode txset");
    let mut hasher = Sha256::new();
    hasher.update(&xdr);
    hasher.finalize().into()
}

/// Extract the transaction set hash from an externalize SCP message.
/// Returns None if the message is not an externalize or if parsing fails.
fn extract_txset_hash_from_scp_message(msg: &StellarMessage) -> Option<(u64, Hash)> {
    let env = match msg {
        StellarMessage::ScpMessage(env) => env,
        _ => return None,
    };

    let externalize = match &env.statement.pledges {
        ScpStatementPledges::Externalize(ext) => ext,
        _ => return None,
    };

    // The commit.value contains XDR-encoded StellarValue
    let stellar_value = StellarValue::from_xdr(&externalize.commit.value, Limits::none()).ok()?;

    Some((env.statement.slot_index, stellar_value.tx_set_hash))
}

/// Count total transactions in a GeneralizedTransactionSet
fn count_transactions_in_txset(txset: &GeneralizedTransactionSet) -> usize {
    let GeneralizedTransactionSet::V1(v1) = txset;
    v1.phases
        .iter()
        .map(|phase| match phase {
            TransactionPhase::V0(components) => components
                .iter()
                .map(|c| match c {
                    TxSetComponent::TxsetCompTxsMaybeDiscountedFee(comp) => comp.txs.len(),
                })
                .sum::<usize>(),
            TransactionPhase::V1(parallel) => parallel
                .execution_stages
                .iter()
                .flat_map(|stage| stage.iter())
                .map(|cluster| cluster.0.len())
                .sum(),
        })
        .sum()
}

/// Check if a transaction set contains the given transaction hash.
fn txset_contains_tx(txset: &GeneralizedTransactionSet, tx_hash: &[u8; 32], network_id: &Hash) -> bool {
    let GeneralizedTransactionSet::V1(v1) = txset;
    for phase in v1.phases.iter() {
        match phase {
            TransactionPhase::V0(components) => {
                for component in components.iter() {
                    let TxSetComponent::TxsetCompTxsMaybeDiscountedFee(comp) = component;
                    for tx in comp.txs.iter() {
                        if let Ok(hash) = tx.hash(network_id.0) {
                            if hash == *tx_hash {
                                return true;
                            }
                        }
                    }
                }
            }
            TransactionPhase::V1(parallel) => {
                for stage in parallel.execution_stages.iter() {
                    // Each stage is a DependentTxCluster which contains a VecM<TransactionEnvelope>
                    for cluster in stage.iter() {
                        for tx in cluster.0.iter() {
                            if let Ok(hash) = tx.hash(network_id.0) {
                                if hash == *tx_hash {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

/// Custom event formatter for tracing output.
struct EventFormatter;

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for EventFormatter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        // Extract fields from the event
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        // Format based on direction and level
        let prefix = if event.metadata().level() == &tracing::Level::ERROR {
            "❌"
        } else if visitor.direction.as_deref() == Some("send") {
            "➡️"
        } else {
            "⬅️"
        };

        // Build the message
        let mut parts = Vec::new();
        if let Some(msg) = &visitor.message {
            parts.push(msg.clone());
        }

        // Add relevant fields
        if let Some(v) = visitor.ledger_version {
            parts.push(format!("ledger_version={}", v));
        }
        if let Some(v) = visitor.overlay_version {
            parts.push(format!("overlay_version={}", v));
        }
        if let Some(v) = &visitor.version_str {
            parts.push(format!("version_str={}", v));
        }
        if let Some(v) = visitor.flags {
            parts.push(format!("flags={}", v));
        }
        if let Some(v) = visitor.num_messages {
            parts.push(format!("num_messages={}", v));
        }
        if let Some(v) = visitor.num_bytes {
            parts.push(format!("num_bytes={}", v));
        }
        if let Some(v) = &visitor.code {
            parts.push(format!("code={}", v));
        }
        if let Some(v) = &visitor.error_message {
            parts.push(v.clone());
        }

        writeln!(writer, "{} {}", prefix, parts.join(": "))
    }
}

/// Visitor to extract fields from tracing events.
#[derive(Default)]
struct FieldVisitor {
    direction: Option<String>,
    message: Option<String>,
    ledger_version: Option<u32>,
    overlay_version: Option<u32>,
    version_str: Option<String>,
    flags: Option<i32>,
    num_messages: Option<u32>,
    num_bytes: Option<u32>,
    code: Option<String>,
    error_message: Option<String>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "direction" => self.direction = Some(value.to_string()),
            "message" => self.message = Some(value.to_string()),
            "version_str" => self.version_str = Some(value.to_string()),
            "error_message" => self.error_message = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "ledger_version" => self.ledger_version = Some(value as u32),
            "overlay_version" => self.overlay_version = Some(value as u32),
            "num_messages" => self.num_messages = Some(value as u32),
            "num_bytes" => self.num_bytes = Some(value as u32),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        match field.name() {
            "flags" => self.flags = Some(value as i32),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "code" => self.code = Some(format!("{:?}", value)),
            _ => {}
        }
    }
}
