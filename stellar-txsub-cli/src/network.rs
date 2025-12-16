//! Stellar network configuration.

use sha2::{Digest, Sha256};
use stellar_xdr::curr::Hash;

/// Stellar network configuration.
#[derive(Debug, Clone)]
pub enum Network {
    Testnet,
    Mainnet,
    Local,
    Custom(String),
}

impl Network {
    /// Parse a network from a string (name or custom passphrase).
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "testnet" | "test" => Network::Testnet,
            "mainnet" | "main" | "pubnet" | "public" => Network::Mainnet,
            "local" => Network::Local,
            _ => Network::Custom(s.to_string()),
        }
    }

    /// Get the network passphrase.
    pub fn passphrase(&self) -> &str {
        match self {
            Network::Testnet => "Test SDF Network ; September 2015",
            Network::Mainnet => "Public Global Stellar Network ; September 2015",
            Network::Local => "Standalone Network ; February 2017",
            Network::Custom(p) => p,
        }
    }

    /// Get the default peer for this network.
    pub fn default_peer(&self) -> &str {
        match self {
            Network::Testnet => "core-testnet1.stellar.org:11625",
            Network::Mainnet => "core-live-a.stellar.org:11625",
            Network::Local | Network::Custom(_) => "localhost:11625",
        }
    }

    /// Compute the network ID (SHA-256 hash of passphrase).
    pub fn id(&self) -> Hash {
        let hash: [u8; 32] = Sha256::digest(self.passphrase().as_bytes()).into();
        Hash(hash)
    }
}
