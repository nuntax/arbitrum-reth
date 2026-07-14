# Robinhood mainnet

Robinhood Chain is an Orbit chain with chain ID `4663`, rooted on Ethereum mainnet. `arb-reth` boots it from Robinhood's `chaininfo.json` and genesis files, then derives batches from L1. It can follow the public sequencer feed while it catches up.

This is an operator recipe for the current mainnet configuration. Keep credentials out of shell history, repository files, and process listings.

## Bootstrap files

Download the mainnet `chaininfo.json` and `genesis.json` from Robinhood's [Run a full node guide](https://docs.robinhood.com/chain/run-a-full-node/). Robinhood publishes both files there for full-node operators.

The copies currently under `crates/arb-reth-node/tests/fixtures` exist to exercise the Orbit boot parser. They are test fixtures, not an operator distribution contract, and may be moved or removed from this repository at any time. Keep downloaded files in operator-controlled storage and pass their paths explicitly.

## Requirements

- A release build of `arb-reth`.
- An archive-capable Ethereum mainnet execution RPC.
- An Ethereum mainnet beacon API. Robinhood has blob batches, so the beacon endpoint is required for a complete sync.
- Disk space for the full chain and a persistent datadir. Do not place the datadir in a temporary directory.

Build the binary from the repository root:

```sh
cargo build --release -p arb-reth-node --bin arb-reth
```

Set the paths and RPC credentials in the environment. The execution and beacon endpoints below are placeholders and should be supplied by the operator.

```sh
export ARB_RETH="$PWD"
export DATADIR="$HOME/arb-data/robinhood-mainnet"
export L1_RPC="https://your-ethereum-archive-rpc.example"
export L1_BEACON="https://your-ethereum-beacon-api.example"
export FEED_URL="wss://feed.mainnet.chain.robinhood.com"
export CHAIN_INFO="$HOME/rh/config/robinhood-chain-info.json"
export GENESIS="$HOME/rh/config/robinhood-genesis.json"
```

The canonical RPC is `https://rpc.mainnet.chain.robinhood.com`. It is useful for parity checks, not for L1 derivation.

## Start the node

The command below creates the datadir when absent and resumes from its stored L1 checkpoint on later starts.

```sh
"$ARB_RETH/target/release/arb-reth" node \
  --datadir "$DATADIR" \
  --chain-info "$CHAIN_INFO" \
  --genesis "$GENESIS" \
  --l1-rpc "$L1_RPC" \
  --l1-beacon "$L1_BEACON" \
  --l1-getlogs-range 500 \
  --l1-prefetch 32 \
  --feed-url "$FEED_URL" \
  --persistence-threshold 128 \
  --memory-buffer-target 0 \
  --persistence-backpressure 512 \
  --http --http.port 8547
```

The feed and L1 derivation may run together. The feed improves time at the tip, while L1 remains the source of durable historical derivation. Feed messages ahead of the L1 cursor are reconciled by sequence number and applied when they become contiguous.

`--l1-getlogs-range 500` and `--l1-prefetch 32` are good starting values for an endpoint that permits wide log ranges and concurrent blob requests. Reduce the range when the provider limits `eth_getLogs`; reduce prefetch when the beacon service is rate-limited.

## Check progress

The local RPC reports the produced tip:

```sh
curl -fsS \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  http://127.0.0.1:8547
```

At the tip, compare the local and canonical block hashes and state roots only at a height both endpoints serve. The feed can place the local node slightly ahead of the canonical RPC, which is not a divergence.

## Recover from a confirmed divergence

Stop the node before modifying the datadir. If `N` is the first divergent L2 block, retain `N - 1` and re-derive the suffix after fixing the state-transition bug:

```sh
"$ARB_RETH/target/release/arb-reth" rewind \
  --datadir "$DATADIR" \
  --chain-info "$CHAIN_INFO" \
  --genesis "$GENESIS" \
  --diverged-at N
```

Run the same `node` command again afterwards. Start with `rewind --dry-run` when the target has not been independently verified.

See the [node command](../commands/node.md), [observability guide](../observability/README.md), and [rewind command](../commands/rewind.md) for option details.
