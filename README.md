# arbitrum-reth

`arbitrum-reth` is an Arbitrum execution and derivation node built on [reth](https://github.com/paradigmxyz/reth). It reads Arbitrum inputs from L1, executes ArbOS state transitions through `arbitrum-revm`, and writes the resulting L2 chain to a reth database.

It also follows a sequencer feed after catch-up, import Nitro state exports, verify genesis state roots, rewind a local database, and inspect stored blocks.

> Experimental software. Do not use it to custody funds or as production infrastructure.

## Snapshot sync benchmark

On the same VPS and persisted Arbitrum One snapshot, `arbitrum-reth` derived 50,000 L2 blocks at
**105.36 blocks/s**. Nitro v3.11.2 derived the identical range at **26.90 blocks/s**, making this
run **3.92× faster** for `arbitrum-reth`.
The VPS is a netcup VPS 8000 G12 (16 cores @ ~2.2GHz, 64GB RAM). Both nitro and `arbitrum-reth` ran on the same Local Block Storage Volume.

| Client | Range | Time | Throughput |
| --- | --- | ---: | ---: |
| `arbitrum-reth` | 481207204–481257203 | 474.56 s | 105.36 blocks/s |
| Nitro v3.11.2 | 481207204–481257203 | 1858.78 s | 26.90 blocks/s |

Each measurement starts when the first block in the range becomes available and ends at the last,
so process startup is excluded. The `arbitrum-reth` result used the sparse-trie payload-builder path,
`persistence-threshold=128`, `memory-buffer-target=0`, and `persistence-backpressure=512`.
The target block's `arbitrum-reth` hash and state root matched a public canonical RPC. This is one
configuration on one machine, not a general performance claim.

## Build

The workspace uses a pinned Rust toolchain and git revisions of `arbitrum-revm` and `arbitrum-alloy`.

```sh
cargo build --release -p arbitrum-reth-node --bin arbitrum-reth
./target/release/arbitrum-reth --help
```

Run the workspace checks with:

```sh
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Run a node

The node needs an L1 execution RPC for derivation. Post-Dencun ranges also need a beacon API endpoint for blob sidecars.

```sh
./target/release/arbitrum-reth node \
  --datadir /data/arbitrum-reth \
  --snapshot-head /data/head.stream \
  --l1-rpc https://your-archive-rpc.example \
  --l1-beacon https://your-beacon-api.example \
  --http
```

The command above is for a snapshot-seeded database. Orbit chains can instead boot from Nitro `chaininfo.json` and `genesis.json`.

```sh
./target/release/arbitrum-reth node \
  --datadir /data/orbit \
  --chain-info chaininfo.json \
  --genesis genesis.json \
  --l1-rpc https://your-archive-rpc.example \
  --http
```

See [node command](docs/commands/node.md) before selecting persistence or feed options.
For a complete Robinhood mainnet invocation, see the [Robinhood chain guide](docs/chains/robinhood-mainnet.md).

## Commands

| Command | Purpose |
| --- | --- |
| [`node`](docs/commands/node.md) | Run an L1-derived node, optionally with a sequencer feed. |
| [`snapshot import` and `snapshot read`](docs/commands/snapshot.md) | Convert and inspect a Nitro state export. |
| [`genesis verify`](docs/commands/genesis.md) | Verify genesis or a `reth-export` state stream. |
| [`rewind`](docs/commands/rewind.md) | Remove a local suffix after a confirmed divergence. |
| [`dump-blocks`](docs/commands/dump-blocks.md) | Print headers, transaction hashes, and receipt status from a datadir. |

The command index is in [docs](docs/README.md). The CLI is the source of truth for flags: `arbitrum-reth <command> --help`.

## Workspace layout

- `arbitrum-reth-derive`: batch, delayed-inbox, brotli, and blob decoding.
- `arbitrum-reth-l1`: L1 reads and batch resolution.
- `arbitrum-reth-sync`: resumable L1 catch-up.
- `arbitrum-reth-engine`: execute-to-derive block production.
- `arbitrum-reth-evm`: reth EVM integration for `arbitrum-revm`.
- `arbitrum-reth-node`: CLI, node wiring, and database lifecycle.
- `arbitrum-reth-rpc`: Arbitrum RPC conversion and reth RPC integration.
- `arbitrum-reth-genesis`: genesis import and verification helpers.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. The vendored Nitro `brotli` dependency is BUSL-1.1. See [NOTICE](NOTICE).
