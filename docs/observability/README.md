# Observability

`arb-reth` serves reth's Prometheus endpoint with `--metrics <ADDR>`. Bind it to loopback unless the metrics network is deliberately exposed:

```sh
arb-reth node --metrics 127.0.0.1:9001 ...
```

The endpoint includes reth engine-tree, persistence, state-root, and RPC metrics. With `--feed-url`, arb-reth also records the live sequencer path. Telemetry deliberately drops a sample instead of blocking the feed reader or block producer.

## Feed to canonical state

`reth_arb_reth_feed_frame_to_canonical_seconds` measures from receiving a WebSocket data frame through decoding, queueing, block production, forkchoice, and in-memory canonicalization. It ends when the shared provider state used by RPC has the new canonical head. It does not include an RPC client's network round trip or response serialization.

Use the phase metrics to locate the delay:

- `reth_arb_reth_feed_frame_decode_seconds`, `channel_wait_seconds`, and `sequencing_wait_seconds` cover ingress and ordering.
- `reth_arb_reth_feed_block_*_seconds` separates parent-state setup, message preparation, ArbOS execution, and state-root finalisation.
- `reth_arb_reth_feed_engine_{insert,forkchoice}_seconds` and `canonicalization_wait_seconds` cover the reth handoff.
- `reth_arb_reth_feed_tracking_dropped_total` should remain zero during normal operation.

Samples collected while catching up from a feed backlog include that backlog in the end-to-end measurement. Use a node at the live tip to judge MEV-facing latency.

## Execute and persist loop

These reth metrics describe the path arb-reth actually uses:

- `reth_blockchain_tree_in_mem_state_num_blocks` is the unpersisted in-memory window.
- `reth_consensus_engine_beacon_backpressure_active` and `backpressure_stall_duration` show persistence-induced stalls.
- `reth_consensus_engine_beacon_persistence_duration` and `reth_consensus_engine_persistence_save_blocks_*` show persistence batch latency and size.
- `reth_consensus_engine_beacon_inserted_already_executed_blocks` and `forkchoice_updated_*` show engine-tree throughput and outcomes.
- `reth_executor_worker_pool_job_{duration,queue_wait}_seconds{pool="state-ovly"}` shows parallel state-root worker saturation.
- `reth_sync_block_validation_state_trie_overlay_overlay_cache_*` and `reth_trie_parallel_*` show overlay reuse and parallel root work.

The standard reth pipeline and `newPayload` metrics are exported too, but they do not drive arb-reth's execute-then-persist loop.

## Prometheus scrape

For a Prometheus server on the host:

```yaml
scrape_configs:
  - job_name: arb-reth
    static_configs:
      - targets: ["127.0.0.1:9001"]
```

When Prometheus runs in a local container, use the host address supplied by that container runtime, such as `host.docker.internal:9001`.
