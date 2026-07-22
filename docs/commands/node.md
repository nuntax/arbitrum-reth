# `arb-reth node`

Runs the node, opens the database, derives L2 messages from L1, and optionally serves JSON-RPC.
It uses Reth's native node command, so datadir, database, metrics, RPC, pruning, engine-tree,
static-file, and storage arguments use their upstream Reth names.

The command intentionally rejects native P2P, txpool, generic payload-builder, dev, ERA, debug-node,
and JIT options. Those components do not exist in the standalone L1/feed-derived architecture, so
accepting their settings would be misleading.

## Inputs

- `--datadir`: node database directory.
- `--l1-rpc`: archive-capable L1 execution endpoint. Required for L1 derivation.
- `--l1-beacon`: beacon API endpoint. Required when the selected range contains blob batches.
- One boot mode:
  - `--snapshot-head <blocks.stream>` for a datadir created by `snapshot import --blocks`.
  - `--chain-info <chaininfo.json> --genesis <genesis.json>` for an Orbit chain booted from genesis.
  - `--arb-chain-config <chain-config.json>` for a chain-config boot.

`--chain` is now Reth's chain-spec option and currently accepts only the `arb-one` placeholder.
It is not a substitute for an ArbOS bootstrap input.

For a snapshot-seeded database:

```sh
arb-reth node \
  --datadir /data/arb1 \
  --snapshot-head /data/head.stream \
  --l1-rpc https://your-archive-rpc.example \
  --l1-beacon https://your-beacon-api.example \
  --http --http.port 8545 \
  --ws --ws.port 8546
```

For an Orbit chain:

```sh
arb-reth node \
  --datadir /data/orbit \
  --chain-info chaininfo.json \
  --genesis genesis.json \
  --l1-rpc https://your-archive-rpc.example \
  --http
```

## L1 derivation

`--l1-rpc` starts the catch-up loop. The node records durable boundaries in `arb-l1-resume.json` under the datadir and resumes from that checkpoint by default.

Use `--l1-start-block` and `--l1-start-delayed` only when the supplied values describe the existing L2 tip. `--l1-end-block` caps derivation at an inclusive L1 height. `--l1-getlogs-range` should match the provider's `eth_getLogs` span limit. `--l1-prefetch` controls concurrent batch resolution.

## Sequencer feed

`--feed-url` connects to a live sequencer relay. A relay is a tip source, not a history source, so use L1 derivation or a snapshot to catch up first. L1 derivation and the feed can run together; messages already applied through one source are reconciled by sequence number.

`--no-l1-derive` makes the feed the only producer. It still needs `--l1-rpc` to bootstrap chain information, and it is appropriate only for a datadir that is already at the feed's retained range.

## Metrics

Pass `--metrics 127.0.0.1:9001` to serve reth's Prometheus endpoint. See the [observability guide](../observability/README.md) for feed latency, engine-tree, persistence, and Prometheus scrape details.

HTTP and WebSocket are separate native Reth servers. Enable each explicitly with `--http` and
`--ws`; configure methods with `--http.api` and `--ws.api`. The authenticated Engine API is always
disabled because ArbOS derives blocks locally rather than accepting beacon-client engine commands.
Reth's local IPC server follows its normal native default; pass `--ipcdisable` when it is not wanted.

## Execution cache

- `--engine.cross-block-cache-size <MiB>` controls Reth's cross-block account, storage, and
  bytecode cache. It defaults to 256 MiB for ArbOS's serial producer; Reth's generic 4 GiB
  `TreeConfig` default is unnecessarily sparse here.

## Payload execution

- `--engine.share-execution-cache-with-payload-builder <true|false>` shares Reth's cross-block account,
  storage, and bytecode cache with the serial Arbitrum payload builder. It defaults to `true`.
- `--engine.share-sparse-trie-with-payload-builder` lets Reth compute the state root concurrently with
  ArbOS execution. It is opt-in and requires useful state-root worker parallelism.

The node builds only one Arbitrum payload at a time. Do not reuse these settings in a node that can
run concurrent payload jobs without first reviewing Reth's cache and sparse-trie ownership rules.

## Persistence controls

- `--engine.persistence-threshold`: number of canonical blocks before a persistence batch.
- `--engine.memory-block-buffer-target`: recent blocks retained in memory before flushing.
- `--engine.persistence-backpressure-threshold`: maximum unpersisted gap before block production stalls.
- `--db.sync-mode safe-no-sync`: bulk-sync durability tradeoff. A crash can lose a recently produced suffix, which derivation can reproduce. `--no-fsync` remains a compatibility alias.

Start with the defaults unless a benchmark or recovery plan justifies changing them.

## History pruning

Without pruning flags, `arb-reth` is an archive node and retains all historical state and receipts.

- `--full` uses reth's full-node profile. It prunes sender recovery completely and retains the
  unwind-safe recent window for account history, storage history, and receipts.
- `--minimal` is more aggressive and also prunes transaction lookups, receipts, and static-file
  data according to reth's minimal-storage profile.
- `--prune.block-interval N` sets how often the persistence service may prune.
- `--prune.minimum-distance N` sets the minimum recent block window that pruning must retain.

The root `arb-start-sync.sh` wrapper exposes the two profiles as `--full` and `--minimal`, plus the
interval and minimum-distance options. For granular segment rules, invoke `arb-reth node --help`
directly and use the corresponding `--prune.*` flags.

Use pruning only after the chain has completed its initial import or catch-up. A pruned node cannot
serve arbitrary historical state, receipts, or transaction lookups that were intentionally removed.
