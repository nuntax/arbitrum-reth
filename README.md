# arb-reth

`arb-reth` is an Arbitrum execution and derivation node built on [reth](https://github.com/paradigmxyz/reth). It reads Arbitrum inputs from L1, executes ArbOS state transitions through `arb-revm`, and writes the resulting L2 chain to a reth database.

It can also follow a sequencer feed after catch-up, import Nitro state exports, verify genesis state roots, rewind a local database, and inspect stored blocks.

> Experimental software. Do not use it to custody funds or as production infrastructure.

## Build

The workspace uses a pinned Rust toolchain and git revisions of `arb-revm` and `arbitrum-alloy`.

```sh
cargo build --release -p arb-reth-node --bin arb-reth
./target/release/arb-reth --help
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
./target/release/arb-reth node \
  --datadir /data/arb-reth \
  --snapshot-head /data/head.stream \
  --l1-rpc https://your-archive-rpc.example \
  --l1-beacon https://your-beacon-api.example \
  --http
```

The command above is for a snapshot-seeded database. Orbit chains can instead boot from Nitro `chaininfo.json` and `genesis.json`.

```sh
./target/release/arb-reth node \
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

The command index is in [docs](docs/README.md). The CLI is the source of truth for flags: `arb-reth <command> --help`.

## Workspace layout

- `arb-reth-derive`: batch, delayed-inbox, brotli, and blob decoding.
- `arb-reth-l1`: L1 reads and batch resolution.
- `arb-reth-sync`: resumable L1 catch-up.
- `arb-reth-engine`: execute-to-derive block production.
- `arb-reth-evm`: reth EVM integration for `arb-revm`.
- `arb-reth-node`: CLI, node wiring, and database lifecycle.
- `arb-reth-rpc`: Arbitrum RPC conversion and reth RPC integration.
- `arb-reth-genesis`: genesis import and verification helpers.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. The vendored Nitro `brotli` dependency is BUSL-1.1. See [NOTICE](NOTICE).
