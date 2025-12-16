//! Stellar overlay network protocol implementation.
//!
//! This crate provides functionality for connecting to and communicating
//! with Stellar Core nodes via the peer-to-peer overlay protocol.
//!
//! # Overview
//!
//! The Stellar network uses a peer-to-peer overlay protocol for nodes to
//! communicate. This crate implements the client side of that protocol,
//! allowing you to:
//!
//! - Connect to Stellar Core nodes
//! - Perform authenticated handshakes
//! - Send and receive protocol messages (transactions, SCP messages, etc.)
//!
//! # Example
//!
//! ```no_run
//! use stellar_overlay::{handshake, Log};
//! use stellar_xdr::curr::{Hash, StellarMessage, TransactionEnvelope};
//! use tokio::net::TcpStream;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to a Stellar Core node
//!     let stream = TcpStream::connect("core-testnet1.stellar.org:11625").await?;
//!
//!     // Network ID is the SHA-256 hash of the network passphrase
//!     // This is the testnet network ID
//!     let network_id = Hash(bytes_lit::bytes!(
//!         0xcee0302d59844d32bdca915c8203dd44b33fbb7edc19051ea37abedf28ecd472
//!     ));
//!
//!     // Perform authenticated handshake
//!     let mut session = handshake(stream, network_id, 11625, |log| {
//!         match log {
//!             Log::Sending(msg) => println!("-> {}", msg),
//!             Log::Received(msg) => println!("<- {}", msg),
//!             Log::Error(msg) => eprintln!("Error: {}", msg),
//!         }
//!     }).await?;
//!
//!     // Send a transaction
//!     // let tx: TransactionEnvelope = ...;
//!     // session.send_message(StellarMessage::Transaction(tx)).await?;
//!
//!     // Receive messages from the peer
//!     let msg = session.recv().await?;
//!     println!("Received: {:?}", msg);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Networks
//!
//! See <https://developers.stellar.org/docs/networks> for network passphrases,
//! network IDs, and known peers.

mod crypto;
mod framing;
mod handshake;
mod session;

pub use handshake::{handshake, Error, Log};
pub use session::PeerSession;
