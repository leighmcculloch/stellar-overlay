# stellar-peerinfo-cli

Get peer information from the Stellar network via the peer-to-peer overlay protocol.

Connects directly to stellar-core nodes, discovers peers recursively, and outputs peer information as NDJSON.

## Example

```
$ stellar-peerinfo --network testnet
Connecting to core-testnet1.stellar.org:11625 (depth 0)...
{"type":"info","peer_id":"a1b2...","peer_address":"core-testnet1.stellar.org:11625","version":"v21.0.0","overlay_version":35,"ledger_version":21}
Connecting to 34.123.45.67:11625 (depth 0)...
{"type":"info","peer_id":"c3d4...","peer_address":"34.123.45.67:11625","version":"v21.0.0","overlay_version":35,"ledger_version":21}
```

## Output

NDJSON (newline-delimited JSON) to stdout, with one JSON object per peer:

```json
{"type":"info","peer_id":"...","peer_address":"1.2.3.4:11625","version":"v21.0.0","overlay_version":35,"ledger_version":21}
{"type":"error","peer_address":"5.6.7.8:11625","error":"Connection timeout"}
```

## Install

```
cargo install --locked \
  --git https://github.com/leighmcculloch/stellar-txsub-cli \
  --package stellar-peerinfo-cli \
  --branch main
```

## Usage

```
stellar-peerinfo [OPTIONS]
```

Connects to a Stellar Core node, discovers peers, and collects information from each peer.

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--network` | `-n` | `testnet` | Network passphrase or short name |
| `--peer` | `-p` | (per network) | Initial peer address (host:port) |
| `--timeout` | `-t` | `10` | Timeout in seconds for responses |
| `--concurrency` | `-j` | `10` | Maximum concurrent peer connections |
| `--depth` | `-d` | `0` | Recursion depth (0 = no recursion) |

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

Get peer info from testnet:
```
stellar-peerinfo --network testnet
```

Recursively explore peers (2 levels deep):
```
stellar-peerinfo --network testnet --depth 2
```

Get peer info from mainnet with high concurrency:
```
stellar-peerinfo --network mainnet -j 50
```

Use a custom initial peer:
```
stellar-peerinfo --peer my-core-node.example.com:11625 --network mainnet
```

Use a longer timeout for slow peers:
```
stellar-peerinfo --timeout 30
```

Filter results with jq:
```
stellar-peerinfo 2>/dev/null | jq 'select(.type == "info")'
```
