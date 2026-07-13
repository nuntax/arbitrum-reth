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

`--no-l1-derive` makes the feed the only producer. It still needs `--l1-rpc` to bootstrap chain information, and it is appropriate only for a datadir that is already at the feed's retained range.

## Metrics

Pass `--metrics 127.0.0.1:9001` to serve reth's Prometheus endpoint. It includes reth's engine-tree and persistence metrics, so no separate pipeline service is needed.

With `--feed-url`, `reth_arb_reth_feed_frame_to_canonical_seconds` measures from receiving a WebSocket data frame through JSON decoding, queueing, block execution, and reth in-memory canonicalization. It ends when the shared provider state backing RPC has the new canonical head. It does not include an RPC client's network round trip or response serialization.

The dashboard should focus on the execution loop:

- `reth_blockchain_tree_in_mem_state_num_blocks` and `reth_consensus_engine_beacon_backpressure_active` show the unpersisted window and whether it is stalling execution.
- `reth_consensus_engine_beacon_persistence_duration` and `reth_consensus_engine_persistence_save_blocks_*` show persistence batch latency and size.
- `reth_executor_worker_pool_job_{duration,queue_wait}_seconds{pool="state-ovly"}` exposes the parallel state-root worker pool.
- `reth_sync_block_validation_state_trie_overlay_overlay_cache_*` and `reth_trie_parallel_*` show overlay reuse and parallel state-root work.

The standard reth pipeline and `newPayload` metrics are also exported, but they stay unused in this execute-then-persist architecture.

`reth_arb_reth_feed_tracking_dropped_total` counts samples intentionally skipped when telemetry would contend with the feed reader or block producer. It should stay at zero during normal operation.

## Persistence controls

- `--persistence-threshold`: number of canonical blocks before a persistence batch.
- `--memory-buffer-target`: recent blocks retained in memory before flushing.
- `--persistence-backpressure`: maximum unpersisted gap before block production stalls.
- `--no-ring-overlay`: debug-only switch to the legacy parent-state path. The ring overlay is enabled by default.
- `--no-fsync`: bulk-sync durability tradeoff. A crash can lose a recently produced suffix, which derivation can reproduce.

Start with the defaults unless a benchmark or recovery plan justifies changing them.
