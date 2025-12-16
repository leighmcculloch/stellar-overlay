//! Integration test for txsub.
//!
//! This test connects to the Stellar Testnet and submits a test transaction.

use std::io::Write;
use std::process::{Command, Stdio};

/// Sample bump sequence transaction for testing.
/// This transaction may be rejected if the sequence number is invalid,
/// but we're testing that the overlay protocol handshake works.
const TEST_TX: &str = "AAAAAgAAAACwA6C/9FZ0ySnhc9z4OxUQhO80f4iykp3l3CPkU+YkhwAAAGQAH0PiAAAAAgAAAAAAAAAAAAAAAQAAAAAAAAALAAAAAAAAAAEAAAAAAAAAAA==";

#[test]
#[ignore] // Run with: cargo test --test integration -- --ignored
fn test_submit_transaction() {
    // Build the binary first
    let build_status = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("Failed to build");

    assert!(build_status.success(), "Build failed");

    // Run the txsub binary with the test transaction
    let mut child = Command::new("cargo")
        .args(["run", "--release", "--"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn txsub");

    // Write the transaction to stdin
    {
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        stdin
            .write_all(TEST_TX.as_bytes())
            .expect("Failed to write to stdin");
    }

    // Wait for the process to complete
    let status = child.wait().expect("Failed to wait for child");

    // The transaction submission should succeed (handshake + send)
    // Note: The actual transaction may be rejected by the network
    // if the sequence number is wrong, but that's a separate concern.
    // We're testing that the overlay protocol works.
    assert!(
        status.success(),
        "txsub failed with exit code: {:?}",
        status.code()
    );
}

#[test]
fn test_parse_sample_transaction() {
    use stellar_xdr::curr::{Limits, ReadXdr, TransactionEnvelope};

    // Verify we can parse the test transaction
    let tx_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, TEST_TX)
        .expect("Failed to decode base64");

    let tx = TransactionEnvelope::from_xdr(&tx_bytes, Limits::none())
        .expect("Failed to parse transaction envelope");

    // Verify it's a bump sequence operation
    match &tx {
        TransactionEnvelope::TxV0(_) => panic!("Expected TxV1, got TxV0"),
        TransactionEnvelope::Tx(tx_v1) => {
            assert_eq!(tx_v1.tx.operations.len(), 1);
            match &tx_v1.tx.operations[0].body {
                stellar_xdr::curr::OperationBody::BumpSequence(_) => {
                    // Success - it's a bump sequence operation
                }
                other => panic!("Expected BumpSequence operation, got {:?}", other),
            }
        }
        TransactionEnvelope::TxFeeBump(_) => panic!("Expected TxV1, got TxFeeBump"),
    }
}
