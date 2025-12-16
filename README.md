# stellar-txsub-cli

Submit transactions to the Stellar network via the peer-to-peer overlay protocol.

Connects directly to a Stellar Core node and submits transactions without using Horizon or RPC.

## Install

```
cargo install --locked --git https://github.com/leighmcculloch/stellar-txsub-cli --branch main
```

## Usage

```
stellar-txsub [OPTIONS]
```

Reads a base64-encoded transaction envelope from stdin and submits it to the network.

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--peer` | `-p` | (per network) | Peer address (host:port) |
| `--network` | `-n` | `testnet` | Network passphrase or shorthand |
| `--timeout` | `-t` | `5` | Timeout in seconds for responses |

### Network Shorthands

| Network | Shorthands | Default Peer |
|---------|------------|--------------|
| Testnet | `testnet`, `test` | `core-testnet1.stellar.org:11625` |
| Mainnet | `mainnet`, `main`, `pubnet`, `public` | `core-live-a.stellar.org:11625` |
| Local | `local`, `standalone` | `localhost:11625` |

Or provide a custom network passphrase directly (defaults to `localhost:11625`).

## Examples

Submit a transaction to testnet:
```
echo "AAAAAgAAAA..." | stellar-txsub
```

Submit to mainnet:
```
echo "AAAAAgAAAA..." | stellar-txsub --network mainnet
```

Submit to local standalone network:
```
echo "AAAAAgAAAA..." | stellar-txsub --network local
```

Submit with custom timeout:
```
echo "AAAAAgAAAA..." | stellar-txsub --timeout 10
```

## Output

The tool logs the handshake and transaction submission:

```
ℹ️ Connecting to core-testnet1.stellar.org:11625
✅ Connected
ℹ️ Performing handshake
➡️ HELLO: ledger_version=25, overlay_version=38, version_str=stellar-txsub/0.1.0
⬅️ HELLO: ledger_version=25, overlay_version=38, version_str=stellar-core 25.0.0
➡️ AUTH: flags=200
⬅️ AUTH: flags=200
✅ Authenticated
➡️ TRANSACTION: hash=5cf69ec6b9852161c2900907649cd48d9e52c83c92e8ec8b4181c80bf3c22289
⬅️ PEERS: count=50
⬅️ SEND_MORE_EXTENDED: num_messages=200, num_bytes=300000
ℹ️ Done (timeout)
⚠️ This tool does not confirm if the transaction was successful.
⚠️ Use the hash to check the status with a block explorer.
```

Exits with code 1 if an error message is received from the peer.

## Warning

This tool submits transactions to the overlay network but **does not confirm whether the transaction was included in a ledger or executed successfully**. The peer accepting the transaction only means it was forwarded to the network.

Use the transaction hash with a Stellar block explorer to verify the final status.
