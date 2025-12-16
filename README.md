# stellar-txsub-cli

Submit transactions to the Stellar network via the peer-to-peer overlay protocol.

Connects directly to a stellar-core node and submits transactions without using Horizon or RPC.

> [!WARNING]
> This tool submits transactions to the overlay network but **does not confirm whether the transaction was included in a ledger or executed successfully**. Use the transaction hash with a Stellar block explorer to verify the final status.

## Example

```
$ stellar network use testnet
$ stellar keys generate me
$ stellar keys fund me
$ stellar tx new bump-sequence --source me --bump-to 1 --build-only \
  | stellar tx sign --sign-with-key me \
  | stellar-txsub --network testnet
ℹ️ Signing transaction: 8259924af89a287a36c3dc177b1fb387c41d5baf4ac234b2ece96f9f185777ab                    
ℹ️ Connecting to core-testnet1.stellar.org:11625
✅ Connected
ℹ️ Performing handshake
➡️ Hello: ledger_version=25, overlay_version=38, version_str=stellar-txsub 0.1.0
⬅️ Hello: ledger_version=25, overlay_version=38, version_str=stellar-core 25.0.0 (e9748b05a70d613437a52c8388dc0d8e68149394)
➡️ Auth: flags=200
⬅️ Auth: flags=200
✅ Authenticated
➡️ Transaction: hash=8259924af89a287a36c3dc177b1fb387c41d5baf4ac234b2ece96f9f185777ab
⬅️ Peers: count=50
⬅️ SendMoreExtended: num_messages=200, num_bytes=300000
⬅️ GetScpState
⬅️ GetScpQuorumset
ℹ️ Done (timeout)
⚠️ This tool does not confirm if the transaction was successful.
⚠️ Use the hash to check the status with a block explorer.
```

## Install

```
cargo install --locked \
  --git https://github.com/leighmcculloch/stellar-txsub-cli \
  --branch main
```

## Usage

```
stellar-txsub [OPTIONS]
```

Reads a base64-encoded transaction envelope from stdin and submits it to the network.

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--network` | `-n` | `testnet` | Network passphrase or short name |
| `--peer` | `-p` | (per network) | Peer address (host:port) |
| `--timeout` | `-t` | `5` | Timeout in seconds for responses |

### Network Short Names

| Network | Short Name |
|---------|------------|
| Testnet | `testnet` |
| Mainnet | `mainnet` |
| Local | `local` |


### Default Peers

| Network | Default Peer |
|---------|--------------|
| Testnet | `core-testnet1.stellar.org:11625` |
| Mainnet | `core-live-a.stellar.org:11625` |
| Local / Custom | `localhost:11625` |

## Examples

Submit a transaction to testnet:
```
echo "AAAAAgAAAA..." | stellar-txsub
```

Submit to mainnet:
```
echo "AAAAAgAAAA..." | stellar-txsub --network mainnet
```

Submit to local network ([stellar/quickstart](https://github.com/stellar/quickstart)):
```
echo "AAAAAgAAAA..." | stellar-txsub --network local
```

Submit with custom timeout:
```
echo "AAAAAgAAAA..." | stellar-txsub --timeout 10
```
