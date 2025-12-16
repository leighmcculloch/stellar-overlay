//! stellar-peerinfo - Get peer information from the Stellar overlay network.
//!
//! This CLI tool connects to Stellar Core nodes, discovers peers recursively,
//! and outputs peer information as NDJSON.

mod network;

use anyhow::Result;
use clap::Parser;
use network::Network;
use serde::Serialize;
use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use stellar_overlay::connect;
use stellar_xdr::curr::{PeerAddress, PeerAddressIp, PublicKey, StellarMessage};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

/// Get peer information from the Stellar overlay network.
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
}

#[derive(Serialize)]
struct PeerOutput {
    #[serde(rename = "type")]
    output_type: &'static str,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let network = Network::from_str(&args.network);
    let initial_peer = args.peer.as_deref().unwrap_or(network.default_peer());

    let net_id = Arc::new(network.id());
    let timeout_duration = Duration::from_secs(args.timeout);
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    let visited = Arc::new(Mutex::new(HashSet::<String>::new()));
    let queue = Arc::new(Mutex::new(VecDeque::<(String, usize)>::new()));

    // Add initial peer
    {
        let mut q = queue.lock().await;
        let mut v = visited.lock().await;
        q.push_back((initial_peer.to_string(), 0));
        v.insert(initial_peer.to_string());
    }

    let max_depth = args.depth;

    loop {
        let batch: Vec<(String, usize)> = {
            let mut q = queue.lock().await;
            q.drain(..).collect()
        };

        if batch.is_empty() {
            break;
        }

        let mut handles = Vec::new();

        for (addr, depth) in batch {
            let sem = semaphore.clone();
            let net_id = net_id.clone();
            let queue = queue.clone();
            let visited = visited.clone();

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                eprintln!("Connecting to {} (depth {})...", addr, depth);

                let (output, known_peers) =
                    get_peer_info(&addr, &net_id, timeout_duration).await;

                println!("{}", serde_json::to_string(&output).unwrap());

                if depth < max_depth {
                    let mut v = visited.lock().await;
                    let mut q = queue.lock().await;
                    for peer_addr in known_peers {
                        if v.insert(peer_addr.clone()) {
                            q.push_back((peer_addr, depth + 1));
                        }
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }
    }

    Ok(())
}

async fn get_peer_info(
    addr: &str,
    network_id: &stellar_xdr::curr::Hash,
    timeout_duration: Duration,
) -> (PeerOutput, Vec<String>) {
    let stream = match timeout(timeout_duration, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return (
                PeerOutput {
                    output_type: "error",
                    peer_id: None,
                    peer_address: Some(addr.to_string()),
                    version: None,
                    overlay_version: None,
                    ledger_version: None,
                    error: Some(e.to_string()),
                },
                vec![],
            )
        }
        Err(_) => {
            return (
                PeerOutput {
                    output_type: "error",
                    peer_id: None,
                    peer_address: Some(addr.to_string()),
                    version: None,
                    overlay_version: None,
                    ledger_version: None,
                    error: Some("Connection timeout".to_string()),
                },
                vec![],
            )
        }
    };

    let mut session = match connect(stream, network_id.clone()).await {
        Ok(s) => s,
        Err(e) => {
            return (
                PeerOutput {
                    output_type: "error",
                    peer_id: None,
                    peer_address: Some(addr.to_string()),
                    version: None,
                    overlay_version: None,
                    ledger_version: None,
                    error: Some(e.to_string()),
                },
                vec![],
            )
        }
    };

    let info = session.peer_info();
    let peer_id = match &info.node_id.0 {
        PublicKey::PublicKeyTypeEd25519(key) => hex::encode(&key.0),
    };
    let version = info.version_str.clone();
    let overlay_version = info.overlay_version;
    let ledger_version = info.ledger_version;

    let known_peers = loop {
        match timeout(timeout_duration, session.recv()).await {
            Ok(Ok(StellarMessage::Peers(peers))) => {
                break peers.iter().map(format_peer_address).collect();
            }
            Ok(Ok(_)) => continue,
            Ok(Err(_)) | Err(_) => break vec![],
        }
    };

    (
        PeerOutput {
            output_type: "info",
            peer_id: Some(peer_id),
            peer_address: Some(addr.to_string()),
            version: Some(version),
            overlay_version: Some(overlay_version),
            ledger_version: Some(ledger_version),
            error: None,
        },
        known_peers,
    )
}

fn format_peer_address(peer: &PeerAddress) -> String {
    let ip: IpAddr = match &peer.ip {
        PeerAddressIp::IPv4(ip) => IpAddr::V4(Ipv4Addr::from(*ip)),
        PeerAddressIp::IPv6(ip) => IpAddr::V6(Ipv6Addr::from(*ip)),
    };
    SocketAddr::new(ip, peer.port as u16).to_string()
}
