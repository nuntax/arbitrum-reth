# `arb-reth node`

Runs the node, opens the database, derives L2 messages from L1, and optionally serves HTTP JSON-RPC.

## Inputs

- `--datadir`: node database directory.
- `--l1-rpc`: archive-capable L1 execution endpoint. Required for L1 derivation.
- `--l1-beacon`: beacon API endpoint. Required when the selected range contains blob batches.
- One boot mode:
  - `--snapshot-head <blocks.stream>` for a datadir created by `snapshot import --blocks`.
  - `--chain-info <chaininfo.json> --genesis <genesis.json>` for an Orbit chain booted from genesis.
  - `--chain <chain-config.json>` for a chain-config boot.

For a snapshot-seeded database:

```sh
arb-reth node \
  --datadir /data/arb1 \
  --snapshot-head /data/head.stream \
  --l1-rpc https://your-archive-rpc.example \
  --l1-beacon https://your-beacon-api.example \
  --http --http.port 8545
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

Repeat `--feed-url` to race different relays. `--feed-connections N` opens `N` independent
WebSockets to every supplied relay. The first decoded copy of a sequence is sent to the engine;
duplicates are discarded by a bounded coordinator before they can delay execution. For example:

```sh
arb-reth node \
  --feed-url wss://relay-a.example/feed \
  --feed-url wss://relay-b.example/feed \
  --feed-connections 3 \
  ...
```

This creates six sockets. The first socket starts immediately and subsequent handshakes are
staggered by one second. Each socket reconnects independently with bounded exponential backoff.
HTTP 429 responses use a separate 30-second to five-minute backoff so an excessive connection
count does not hammer the relay. The reconnect cursor advances only across a contiguous observed
sequence prefix, so a faster connection cannot make reconnecting peers skip a gap.

Start with two or three connections per relay and use the source metrics to verify that additional
connections still win messages often enough to justify their bandwidth. The node accepts at most
64 total feed connections, but a relay may enforce a much lower per-IP limit. Excess sockets stay
on the rate-limit backoff and do not affect established connections. Endpoint paths, query strings,
and credentials are excluded from logs and metric labels.

`--no-l1-derive` makes the feed the only producer. It still needs `--l1-rpc` to bootstrap chain information, and it is appropriate only for a datadir that is already at the feed's retained range.

## Metrics

Pass `--metrics 127.0.0.1:9001` to serve reth's Prometheus endpoint. See the [observability guide](../observability/README.md) for feed latency, engine-tree, persistence, and Prometheus scrape details.

## MEV transaction-log IPC

`--mev-tx-log-ipc /run/arb-reth/mev-logs.sock` opens a local Unix socket. Each connected client
receives one compact binary frame after every included ArbOS transaction finishes EVM execution.
Events include the provisional block number, transaction index and hash, transaction kind, status,
gas used, and logs. They are deliberately pre-canonical: there is no block hash yet, and a later
state-root or engine-insertion failure can discard the enclosing block. See the [wire
specification](../mev-tx-log-ipc.md) before implementing a consumer.

Enabling the feed also installs `arb_simulateAtFrontier`. Version-2 feed frames carry the exact
post-transaction `frontierId` accepted by this method, allowing a client to simulate against the
same in-progress state that produced the observed logs.

The stream is best effort. A slow client is disconnected rather than delaying execution; reconnect
to resume from the current transaction. Log payloads are allocated only while a client is
connected. Frontier state deltas are retained whenever the feature is enabled.

## Execution cache

- `--engine.cross-block-cache-size <MiB>` controls Reth's cross-block account, storage, and
  bytecode cache. It defaults to 256 MiB for ArbOS's serial producer; Reth's generic 4 GiB
  `TreeConfig` default is unnecessarily sparse here.

## Payload execution

- `--share-execution-cache-with-payload-builder <true|false>` shares Reth's cross-block account,
  storage, and bytecode cache with the serial Arbitrum payload builder. It defaults to `true`.
- `--share-sparse-trie-with-payload-builder` lets Reth compute the state root concurrently with
  ArbOS execution. It is opt-in and requires useful state-root worker parallelism.

The node builds only one Arbitrum payload at a time. Do not reuse these settings in a node that can
run concurrent payload jobs without first reviewing Reth's cache and sparse-trie ownership rules.

## Persistence controls

- `--persistence-threshold`: number of canonical blocks before a persistence batch.
- `--memory-buffer-target`: recent blocks retained in memory before flushing.
- `--persistence-backpressure`: maximum unpersisted gap before block production stalls.
- `--no-fsync`: bulk-sync durability tradeoff. A crash can lose a recently produced suffix, which derivation can reproduce.

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
