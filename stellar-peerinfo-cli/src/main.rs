//! stellar-peerinfo - Get peer information from the Stellar overlay network.
//!
//! This CLI tool connects to Stellar Core nodes, discovers peers recursively,
//! and outputs peer information or a network graph.

mod network;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use network::Network;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stellar_overlay::connect;
use stellar_xdr::curr::{NodeId, PeerAddress, PeerAddressIp, PublicKey, StellarMessage};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::timeout;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::*;

/// Get peer information from the Stellar overlay network.
///
/// Connects to a Stellar Core node, discovers known peers,
/// and collects information about each peer.
#[derive(Parser, Debug)]
#[command(name = "stellar-peerinfo", version, about)]
struct Args {
    /// Peer address to connect to (host:port)
    #[arg(short, long)]
    peer: Option<String>,

    /// Network passphrase (or "testnet" / "mainnet")
    #[arg(short, long, default_value = "testnet")]
    network: String,

    /// Timeout in seconds for waiting for responses
    #[arg(short, long, default_value = "10")]
    timeout: u64,

    /// Maximum number of concurrent peer connections
    #[arg(short = 'j', long, default_value = "10")]
    concurrency: usize,

    /// Recursion depth for peer discovery (0 = no recursion)
    #[arg(short, long, default_value = "0")]
    depth: usize,

    /// Output format
    #[arg(short, long, value_enum, default_value = "json")]
    output: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
enum OutputFormat {
    /// NDJSON output (one JSON object per line)
    Json,
    /// MermaidJS graph diagram
    Mermaid,
}

/// JSON output for peer info.
#[derive(Serialize, Clone)]
struct PeerOutput {
    #[serde(rename = "type")]
    output_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    overlay_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ledger_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Result from connecting to a peer.
struct PeerResult {
    info: Option<PeerInfo>,
    known_peers: Vec<String>,
    error: Option<String>,
}

/// Successful peer connection info.
#[derive(Clone)]
struct PeerInfo {
    peer_id: String,
    version: String,
    overlay_version: u32,
    ledger_version: u32,
}

/// Output message for the output handler.
enum OutputMessage {
    Json(PeerOutput),
    MermaidNode {
        id: usize,
        addr: String,
        info: Option<PeerInfo>,
    },
    MermaidEdges {
        from_id: usize,
        to_ids: Vec<usize>,
    },
}

/// Tracks node IDs for mermaid output.
struct NodeIdTracker {
    addr_to_id: HashMap<String, usize>,
    next_id: AtomicUsize,
}

impl NodeIdTracker {
    fn new() -> Self {
        Self {
            addr_to_id: HashMap::new(),
            next_id: AtomicUsize::new(0),
        }
    }

    fn get_or_assign(&mut self, addr: &str) -> usize {
        if let Some(&id) = self.addr_to_id.get(addr) {
            id
        } else {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            self.addr_to_id.insert(addr.to_string(), id);
            id
        }
    }

    fn get(&self, addr: &str) -> Option<usize> {
        self.addr_to_id.get(addr).copied()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up tracing subscriber for stderr
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(true)
                .with_writer(std::io::stderr)
                .with_filter(LevelFilter::INFO),
        )
        .init();

    let args = Args::parse();

    // Resolve network
    let network = Network::from_str(&args.network);
    let initial_peer = args.peer.as_deref().unwrap_or(network.default_peer());

    let net_id = Arc::new(network.id());
    let timeout_duration = Duration::from_secs(args.timeout);
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    // Track visited addresses and node IDs
    let visited = Arc::new(Mutex::new(HashSet::<String>::new()));
    let node_ids = Arc::new(Mutex::new(NodeIdTracker::new()));

    // Channel for output
    let (tx, mut rx) = mpsc::unbounded_channel::<OutputMessage>();

    // Print mermaid header if needed
    let output_format = args.output;
    if output_format == OutputFormat::Mermaid {
        println!("graph LR");
    }

    // Spawn output handler
    let output_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                OutputMessage::Json(output) => {
                    println!("{}", serde_json::to_string(&output).unwrap());
                }
                OutputMessage::MermaidNode { id, addr, info } => {
                    let label = if let Some(info) = info {
                        let short_id = if info.peer_id.len() > 8 {
                            &info.peer_id[..8]
                        } else {
                            &info.peer_id
                        };
                        format!(
                            "    N{}[\"{}\\n{}\\n{}\"]",
                            id, addr, short_id, info.version
                        )
                    } else {
                        format!("    N{}[\"{}\\n(unreachable)\"]", id, addr)
                    };
                    println!("{}", label);
                }
                OutputMessage::MermaidEdges { from_id, to_ids } => {
                    for to_id in to_ids {
                        println!("    N{} --> N{}", from_id, to_id);
                    }
                }
            }
        }
    });

    // BFS queue: (address, depth)
    let queue = Arc::new(Mutex::new(VecDeque::<(String, usize)>::new()));

    // Add initial peer and assign its ID
    {
        let mut q = queue.lock().await;
        q.push_back((initial_peer.to_string(), 0));
    }
    {
        let mut v = visited.lock().await;
        v.insert(initial_peer.to_string());
    }
    {
        let mut ids = node_ids.lock().await;
        ids.get_or_assign(initial_peer);
    }

    let max_depth = args.depth;

    // Process queue
    loop {
        // Get batch of addresses
        let batch: Vec<(String, usize)> = {
            let mut q = queue.lock().await;
            let mut batch = Vec::new();
            while let Some(item) = q.pop_front() {
                batch.push(item);
            }
            batch
        };

        if batch.is_empty() {
            break;
        }

        // Process batch concurrently
        let mut handles = Vec::new();

        for (addr, depth) in batch {
            let sem = semaphore.clone();
            let net_id = net_id.clone();
            let tx = tx.clone();
            let queue = queue.clone();
            let visited = visited.clone();
            let node_ids = node_ids.clone();
            let output_format = args.output;

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                eprintln!("Connecting to {} (depth {})...", addr, depth);

                let result = get_peer_with_peers(&addr, &net_id, timeout_duration).await;

                // Get this node's ID
                let node_id = {
                    let ids = node_ids.lock().await;
                    ids.get(&addr).unwrap()
                };

                // Send output
                match output_format {
                    OutputFormat::Json => {
                        let output = if let Some(ref info) = result.info {
                            PeerOutput {
                                output_type: "info".to_string(),
                                peer_id: Some(info.peer_id.clone()),
                                peer_address: Some(addr.clone()),
                                version: Some(info.version.clone()),
                                overlay_version: Some(info.overlay_version),
                                ledger_version: Some(info.ledger_version),
                                error: None,
                            }
                        } else {
                            PeerOutput {
                                output_type: "error".to_string(),
                                peer_id: None,
                                peer_address: Some(addr.clone()),
                                version: None,
                                overlay_version: None,
                                ledger_version: None,
                                error: result.error.clone(),
                            }
                        };
                        let _ = tx.send(OutputMessage::Json(output));
                    }
                    OutputFormat::Mermaid => {
                        // Output node definition
                        let _ = tx.send(OutputMessage::MermaidNode {
                            id: node_id,
                            addr: addr.clone(),
                            info: result.info.clone(),
                        });

                        // Assign IDs to known peers and output edges
                        if !result.known_peers.is_empty() {
                            let to_ids: Vec<usize> = {
                                let mut ids = node_ids.lock().await;
                                result
                                    .known_peers
                                    .iter()
                                    .map(|peer_addr| ids.get_or_assign(peer_addr))
                                    .collect()
                            };
                            let _ = tx.send(OutputMessage::MermaidEdges {
                                from_id: node_id,
                                to_ids,
                            });
                        }
                    }
                }

                // Add newly discovered peers to queue if within depth limit
                if depth < max_depth {
                    let mut v = visited.lock().await;
                    let mut q = queue.lock().await;
                    let mut ids = node_ids.lock().await;
                    for peer_addr in result.known_peers {
                        if !v.contains(&peer_addr) {
                            v.insert(peer_addr.clone());
                            ids.get_or_assign(&peer_addr);
                            q.push_back((peer_addr, depth + 1));
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for batch to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    // Close the channel
    drop(tx);

    // Wait for output handler
    output_handle.await?;

    Ok(())
}

/// Get peer info and their known peers list.
async fn get_peer_with_peers(
    addr: &str,
    network_id: &stellar_xdr::curr::Hash,
    timeout_duration: Duration,
) -> PeerResult {
    // Connect with timeout
    let stream = match timeout(timeout_duration, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return PeerResult {
                info: None,
                known_peers: vec![],
                error: Some(e.to_string()),
            }
        }
        Err(_) => {
            return PeerResult {
                info: None,
                known_peers: vec![],
                error: Some("Connection timeout".to_string()),
            }
        }
    };

    let mut session = match connect(stream, network_id.clone()).await {
        Ok(s) => s,
        Err(e) => {
            return PeerResult {
                info: None,
                known_peers: vec![],
                error: Some(e.to_string()),
            }
        }
    };

    let peer_info = session.peer_info();
    let info = PeerInfo {
        peer_id: format_node_id(&peer_info.node_id),
        version: peer_info.version_str.clone(),
        overlay_version: peer_info.overlay_version,
        ledger_version: peer_info.ledger_version,
    };

    // Wait for Peers message
    let known_peers = loop {
        match timeout(timeout_duration, session.recv()).await {
            Ok(Ok(StellarMessage::Peers(peers))) => {
                break peers
                    .iter()
                    .map(|p| format_peer_address(p))
                    .collect::<Vec<_>>();
            }
            Ok(Ok(_)) => {
                // Ignore other messages, keep waiting
                continue;
            }
            Ok(Err(_)) | Err(_) => {
                // Connection closed or timeout, return what we have
                break vec![];
            }
        }
    };

    PeerResult {
        info: Some(info),
        known_peers,
        error: None,
    }
}

/// Format a NodeId for display.
fn format_node_id(node_id: &NodeId) -> String {
    match &node_id.0 {
        PublicKey::PublicKeyTypeEd25519(key) => hex::encode(&key.0),
    }
}

/// Format a PeerAddress for display.
fn format_peer_address(peer: &PeerAddress) -> String {
    let ip: IpAddr = match &peer.ip {
        PeerAddressIp::IPv4(ip) => IpAddr::V4(Ipv4Addr::from(*ip)),
        PeerAddressIp::IPv6(ip) => IpAddr::V6(Ipv6Addr::from(*ip)),
    };
    let addr = SocketAddr::new(ip, peer.port as u16);
    addr.to_string()
}
