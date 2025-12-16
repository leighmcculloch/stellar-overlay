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
use stellar_overlay::handshake;
use stellar_xdr::curr::{ReadXdr, StellarMessage, TransactionEnvelope};
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

    /// Timeout in seconds for waiting for responses
    #[arg(short, long, default_value = "5")]
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
    let mut session = handshake(stream, net_id.clone()).await?;
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
