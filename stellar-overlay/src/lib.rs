//! Stellar overlay network protocol implementation.
//!
//! This crate provides functionality for connecting to and communicating
//! with Stellar Core nodes via the peer-to-peer overlay protocol.

pub mod crypto;
pub mod framing;
pub mod handshake;
pub mod session;

pub use handshake::{handshake, Event};
pub use session::PeerSession;
