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

Samples collected while catching up from a feed backlog include that backlog in the end-to-end measurement. Use a node at the live tip to judge MEV-facing latency.

## Execute and persist loop

These reth metrics describe the path arb-reth actually uses:

- `reth_blockchain_tree_in_mem_state_num_blocks` is the unpersisted in-memory window.
- `reth_consensus_engine_beacon_backpressure_active` and `backpressure_stall_duration` show persistence-induced stalls.
- `reth_consensus_engine_beacon_persistence_duration` and `reth_consensus_engine_persistence_save_blocks_*` show persistence batch latency and size.
- `reth_consensus_engine_beacon_inserted_already_executed_blocks` and `forkchoice_updated_*` show engine-tree throughput and outcomes.

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
