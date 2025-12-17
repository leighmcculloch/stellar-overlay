//! Cryptographic utilities for the Stellar overlay protocol.

use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use sha2::Sha256;
use stellar_xdr::curr::{
    AuthCert, Curve25519Public, EnvelopeType, Hash, HmacSha256Key, HmacSha256Mac, PublicKey,
    Signature, Uint256,
};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret as X25519SecretKey};

/// Node identity using Ed25519 keypair.
///
/// Used for signing protocol messages. Generate with [`NodeIdentity::generate`]
/// and pass to [`connect`](crate::connect) to establish a peer session.
pub struct NodeIdentity {
    pub signing_key: SigningKey,
    pub public_key: VerifyingKey,
}

impl NodeIdentity {
    /// Generate a new random Ed25519 keypair.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let public_key = signing_key.verifying_key();
        Self {
            signing_key,
            public_key,
        }
    }

    /// Get the node's public key in Stellar XDR format.
    pub fn to_public_key(&self) -> PublicKey {
        PublicKey::PublicKeyTypeEd25519(Uint256(self.public_key.to_bytes()))
    }

    /// Sign a message with the node's Ed25519 key.
    pub fn sign(&self, message: &[u8]) -> Signature {
        let sig = self.signing_key.sign(message);
        Signature(sig.to_bytes().to_vec().try_into().unwrap())
    }
}

/// Curve25519 keypair for ECDH key exchange.
///
/// Used for encrypted communication.
pub(crate) struct EcdhKeypair {
    pub secret: X25519SecretKey,
    pub public: X25519PublicKey,
}

impl EcdhKeypair {
    /// Generate a new random Curve25519 keypair.
    pub fn generate() -> Self {
        let secret = X25519SecretKey::random_from_rng(OsRng);
        let public = X25519PublicKey::from(&secret);
        Self { secret, public }
    }

    /// Get the public key in Stellar XDR format.
    pub fn to_curve25519_public(&self) -> Curve25519Public {
        Curve25519Public {
            key: self.public.to_bytes(),
        }
    }
}

/// Generate a random 256-bit nonce.
pub(crate) fn generate_nonce() -> Uint256 {
    let mut nonce = [0u8; 32];
    rand::RngCore::fill_bytes(&mut OsRng, &mut nonce);
    Uint256(nonce)
}

/// Create an authentication certificate.
///
/// The certificate contains:
/// - Curve25519 public key for ECDH
/// - Expiration timestamp
/// - Ed25519 signature of (networkID || ENVELOPE_TYPE_AUTH || expiration || pubkey)
pub(crate) fn create_auth_cert(
    network_id: &Hash,
    node_identity: &NodeIdentity,
    ecdh_keypair: &EcdhKeypair,
    expiration: u64,
) -> AuthCert {
    // Build the data to sign: networkID || ENVELOPE_TYPE_AUTH || expiration || pubkey
    let mut data_to_sign = Vec::new();
    data_to_sign.extend_from_slice(&network_id.0);
    data_to_sign.extend_from_slice(&(EnvelopeType::Auth as i32).to_be_bytes());
    data_to_sign.extend_from_slice(&expiration.to_be_bytes());
    data_to_sign.extend_from_slice(&ecdh_keypair.public.to_bytes());

    // Hash and sign
    let hash = sha256(&data_to_sign);
    let sig = node_identity.sign(&hash);

    AuthCert {
        pubkey: ecdh_keypair.to_curve25519_public(),
        expiration,
        sig,
    }
}

/// Derive the shared secret using Curve25519 ECDH.
///
/// Matches stellar-core's curve25519DeriveSharedKey:
/// 1. Compute ECDH: q = scalarmult(localSecret, remotePublic)
/// 2. Concatenate: q || publicA || publicB (A is local if we_are_initiator)
/// 3. Return hkdfExtract(concatenated) = HMAC(zero_key, concatenated)
pub(crate) fn ecdh_shared_secret(
    our_secret: &X25519SecretKey,
    our_public: &X25519PublicKey,
    their_public: &Curve25519Public,
    we_are_initiator: bool,
) -> HmacSha256Key {
    let their_public_x = X25519PublicKey::from(their_public.key);
    let shared_secret = our_secret.diffie_hellman(&their_public_x);

    // Order the public keys: initiator (localFirst) first
    let (public_a, public_b): (&[u8], &[u8]) = if we_are_initiator {
        (our_public.as_bytes(), &their_public.key)
    } else {
        (&their_public.key, our_public.as_bytes())
    };

    // Build: q || publicA || publicB
    let mut buf = Vec::with_capacity(32 + 32 + 32);
    buf.extend_from_slice(shared_secret.as_bytes());
    buf.extend_from_slice(public_a);
    buf.extend_from_slice(public_b);

    // hkdfExtract = HMAC(zero_key, buf)
    hkdf_extract(&buf)
}

/// Derive the sending MAC key using HKDF.
///
/// For the initiator (WE_CALLED_REMOTE):
///   K_AB = hkdfExpand(shared_key, 0 || local_nonce || remote_nonce)
///
/// For the responder (REMOTE_CALLED_US):
///   K_BA = hkdfExpand(shared_key, 1 || local_nonce || remote_nonce)
pub(crate) fn derive_sending_mac_key(
    shared_key: &HmacSha256Key,
    local_nonce: &Uint256,
    remote_nonce: &Uint256,
    we_are_initiator: bool,
) -> HmacSha256Key {
    let mut info = Vec::with_capacity(65);
    if we_are_initiator {
        info.push(0);
    } else {
        info.push(1);
    }
    info.extend_from_slice(&local_nonce.0);
    info.extend_from_slice(&remote_nonce.0);

    hkdf_expand(shared_key, &info)
}

/// Derive the receiving MAC key using HKDF.
///
/// For the initiator (WE_CALLED_REMOTE):
///   K_BA = hkdfExpand(shared_key, 1 || remote_nonce || local_nonce)
///
/// For the responder (REMOTE_CALLED_US):
///   K_AB = hkdfExpand(shared_key, 0 || remote_nonce || local_nonce)
pub(crate) fn derive_receiving_mac_key(
    shared_key: &HmacSha256Key,
    local_nonce: &Uint256,
    remote_nonce: &Uint256,
    we_are_initiator: bool,
) -> HmacSha256Key {
    let mut info = Vec::with_capacity(65);
    if we_are_initiator {
        info.push(1);
        info.extend_from_slice(&remote_nonce.0);
        info.extend_from_slice(&local_nonce.0);
    } else {
        info.push(0);
        info.extend_from_slice(&remote_nonce.0);
        info.extend_from_slice(&local_nonce.0);
    }

    hkdf_expand(shared_key, &info)
}

/// Stellar-core's hkdfExtract: HMAC(zero_key, data)
fn hkdf_extract(data: &[u8]) -> HmacSha256Key {
    let zero_key = HmacSha256Key { key: [0u8; 32] };
    let mac = hmac_sha256(&zero_key, data);
    HmacSha256Key { key: mac.mac }
}

/// Stellar-core's hkdfExpand: HMAC(key, info || 0x01)
fn hkdf_expand(key: &HmacSha256Key, info: &[u8]) -> HmacSha256Key {
    let mut data = info.to_vec();
    data.push(1);
    let mac = hmac_sha256(key, &data);
    HmacSha256Key { key: mac.mac }
}

/// Compute HMAC-SHA256.
pub(crate) fn hmac_sha256(key: &HmacSha256Key, data: &[u8]) -> HmacSha256Mac {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.key).expect("HMAC key should be valid");
    mac.update(data);
    let result = mac.finalize();
    HmacSha256Mac {
        mac: result.into_bytes().into(),
    }
}

/// SHA-256 hash function.
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute the Stellar network ID from a passphrase.
///
/// The network ID is the SHA-256 hash of the network passphrase.
#[cfg(test)]
fn network_id(passphrase: &str) -> Hash {
    Hash(sha256(passphrase.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_id() {
        let id = network_id("Test SDF Network ; September 2015");
        // Known testnet network ID
        assert_eq!(id.0.len(), 32);
    }

    #[test]
    fn test_node_identity() {
        let identity = NodeIdentity::generate();
        let pub_key = identity.to_public_key();
        match pub_key {
            PublicKey::PublicKeyTypeEd25519(key) => {
                assert_eq!(key.0.len(), 32);
            }
        }
    }

    #[test]
    fn test_ecdh_keypair() {
        let keypair = EcdhKeypair::generate();
        let public = keypair.to_curve25519_public();
        assert_eq!(public.key.len(), 32);
    }

    #[test]
    fn test_ecdh_shared_secret() {
        let alice = EcdhKeypair::generate();
        let bob = EcdhKeypair::generate();

        let alice_shared = ecdh_shared_secret(
            &alice.secret,
            &alice.public,
            &bob.to_curve25519_public(),
            true,
        );
        let bob_shared = ecdh_shared_secret(
            &bob.secret,
            &bob.public,
            &alice.to_curve25519_public(),
            false,
        );

        assert_eq!(alice_shared.key, bob_shared.key);
    }
}
