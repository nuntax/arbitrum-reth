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
- `reth_arb_reth_feed_payload_attributes_seconds`, `payload_job_seconds`,
  `engine_handoff_seconds`, and `engine_apply_overhead_seconds` are exclusive outer phases. Their
  means add up to `engine_apply_total_seconds`.
- `reth_arb_reth_feed_payload_job_{launch,resolve}_seconds` splits the payload job's wall time.
  `payload_job_overhead_seconds` subtracts the builder's measured production from that outer job.
- `reth_arb_reth_feed_block_*_seconds` separates parent-state setup, message preparation, ArbOS execution, and state-root finalisation.
- `reth_arb_reth_feed_engine_{insert,forkchoice}_seconds` and `canonicalization_wait_seconds` are
  nested diagnostics inside `engine_handoff_seconds`. Do not add them to the outer phases.
- `reth_arb_reth_feed_tracking_dropped_total` should remain zero during normal operation.

## ArbOS transaction execution

The executor exports these bounded-label series for each consensus transaction family (`legacy`,
`eip1559`, `deposit`, `unsigned`, `contract`, `retry`, `submit_retryable`, and `internal`):

- `reth_arb_reth_arbos_transaction_execution_seconds` measures the full ArbOS handler transition,
  including the applicable pre-execution hooks and EVM or protocol transaction body.
- `reth_arb_reth_arbos_transaction_commit_seconds` measures receipt construction and the in-memory
  state commit after that transition.
- `reth_arb_reth_arbos_transaction_{,l1_}gas_used` records L2 gas and L1 poster-gas distributions.
- `reth_arb_reth_arbos_pre_execution_system_call_seconds` measures the EIP-2935 parent-hash
  prelude, and `post_execution_header_info_seconds` measures the ArbOS header-field read.
- `reth_arb_revm_arbos_handler_phase_seconds{phase,tx_type,mode}` splits the transition itself.
  `pre_execution` covers ArbOS gas charging and filtering, `execution` covers the protocol or EVM
  frame, and `end_tx_hook` covers fee distribution, refunds, and backlog updates. `mode="execute"`
  is block production; `mode="inspect"` is debug tracing and should be excluded from latency views.

These labels deliberately exclude addresses, transaction hashes, block numbers, and ArbOS version
to keep Prometheus cardinality bounded. Use a one-off profiler for per-contract or opcode detail.

Detailed transaction and handler histograms can be sampled with
`ARB_EXECUTION_METRICS_SAMPLE_RATE`. The default is `1`, which records every transaction. A value
of `16` records one transaction of each type per 16-transaction window and is the recommended
throughput setting. `0` disables only these detailed histograms. Block production, MGas/s,
persistence, state-root, and feed-to-canonical metrics are never sampled by this setting.

Samples collected while catching up from a feed backlog include that backlog in the end-to-end measurement. Use a node at the live tip to judge MEV-facing latency.

## Execute and persist loop

These reth metrics describe the path arb-reth actually uses:

- `reth_blockchain_tree_in_mem_state_num_blocks` is the unpersisted in-memory window.
- `reth_consensus_engine_beacon_backpressure_active` and `backpressure_stall_duration` show persistence-induced stalls.
- `reth_consensus_engine_beacon_persistence_duration` and `reth_consensus_engine_persistence_save_blocks_*` show persistence batch latency and size.
- `reth_consensus_engine_beacon_inserted_already_executed_blocks` and `forkchoice_updated_*` show engine-tree throughput and outcomes.

The standard reth pipeline and `newPayload` metrics are exported too, but they do not drive arb-reth's execute-then-persist loop.

The `reth_arb_reth_engine_block_*` family records the same payload, production, execution,
finalisation, and handoff phases for every produced block, including historical L1 catch-up. Use
that family instead of `feed_*` when benchmarking sync throughput.

During catch-up, the driver may queue block N+1's attributes FCU behind block N's final FCU before
awaiting N. The engine still processes them in order. Each block's `engine_handoff_seconds`, total,
and completion timestamp end at that block's actual final FCU response, not when the next driver
iteration happens to collect it.

For native payload construction, inspect
`reth_arb_reth_engine_block_finish_state_root_task_wait_seconds` together with Reth's sparse-trie
and persistence metrics to distinguish root work from commit stalls. When execution-cache sharing
is enabled, `reth_arb_reth_execution_cache_access_total{kind,result}` and
`reth_arb_reth_execution_cache_accesses_per_block{kind,result}` show block-local builder cache
hits and misses without putting Prometheus recording inside the measured production interval.

## Prometheus scrape

For a Prometheus server on the host:

```yaml
scrape_configs:
  - job_name: arb-reth
    static_configs:
      - targets: ["127.0.0.1:9001"]
```

When Prometheus runs in a local container, use the host address supplied by that container runtime, such as `host.docker.internal:9001`.
