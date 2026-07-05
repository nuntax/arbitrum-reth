# arb-reth

An Arbitrum One execution and derivation node built on [reth](https://github.com/paradigmxyz/reth), with a custom Arbitrum revm for the state transition. It derives the L2 chain from L1 (sequencer batches, the delayed inbox, and post-Dencun blobs), executes it with ArbOS semantics, and rebuilds Arbitrum One state from Nitro genesis, checking every block against canonical Nitro.

> **This is an experimental research prototype, not production software.** It is a single-developer, work-in-progress reimplementation of an Arbitrum node. All it does today is replay Arbitrum One history from L1 and check each block against canonical Nitro; it has never run against the live sequencer feed, makes no correctness, stability, or API guarantees, is unaudited, and should not be relied on for anything real or used to secure funds. It has bugs, and parity divergences are still being found and fixed block by block. Expect it to break.

## Background

The idea came out of latency-sensitive MEV work on Arbitrum. Nitro was too slow at updating state after receiving sequencer feed messages, so I built a heuristic executor on top of revm to run Arbitrum transactions faster. That executor was approximate: it traded correctness for speed and did not have full parity with Arbitrum. It was useful, but it was nowhere near a node.

This node did not grow out of that executor. What the executor did was show two things: how much faster Arbitrum execution can be, and how much real work exact parity actually takes. arb-reth is a separate, from-scratch attempt at a harder version, which is to execute Arbitrum fast and to match the canonical chain block for block.

## What it is

arb-reth is a standalone, Nitro-style node. Arbitrum is execute-to-derive. Precisely, a sequencer message is the input and the block, including its state root, is the output of executing that message. So instead of reth's download-then-execute pipeline, the node reads L1, derives the message stream, mints each block, and persists it. The state transition runs through `arb_revm`, a project which consumes revm and adds ArbOS precompiles, gas accounting, and retryable/redeem handling.

Standalone is the point of it. Other Arbitrum execution clients are as the naming suggets, execution clients. They still need a Nitro node next to them, in consensus mode, to derive the chain from L1 and drive them over the consensus api. arb-reth does that derivation itself, reading L1 directly, so it does not depend on Nitro to run. Nitro is used only as the parity reference to check the output against, not as a component in the pipeline.

## Architecture

- `arb-reth-derive`: decoders for sequencer batches, the delayed inbox, brotli-compressed payloads, and blob field elements. Turns raw L1 data into the feed message stream.
- `arb-reth-l1`: reads the `SequencerInbox` and `Bridge` contracts over an L1 RPC, resolves batch payloads from calldata, separate events, or blob sidecars, and reconstructs delayed messages.
- `arb-reth-sync`: the catch-up runtime. Walks L1 block ranges, derives feed messages, and feeds them to the engine driver. Records a resume checkpoint once blocks are durable.
- `arb-reth-evm`: bridges `arb_revm` into reth's `ConfigureEvm`, so the executor runs inside reth's block-building path.
- `arb-reth-engine`: the block producer. Mints execute-to-derive blocks through reth's engine tree, reads parent state through an in-memory overlay, and can compute the state root in parallel.
- `arb-reth-node`: the node skeleton, launcher, and binaries. Wires Arbitrum primitives into reth's `NodeTypes`, stands up the database and provider, and drives the sync.
- `arb-reth-rpc`: an `eth_*` RPC layer with Arbitrum receipt fields (for example `gasUsedForL1`).
- `arb-reth-genesis`: imports the Nitro genesis state and verifies it.

Binaries live under `arb-reth-node/src/bin`: `arb-reth` (the node), `arb-rewind` (roll the database back to a block), `arb-snapshot-import` / `arb-snapshot-read`, and `dump-blocks`.

## Performance

- Execution is minted, not re-executed. Produced blocks go into reth's engine tree via `InsertExecutedBlock`, so the tree owns canonicalization and persistence without a second execution pass.
- Parent state is threaded forward through an in-memory ring overlay anchored on a pinned read transaction, rather than read back through the provider. Reading it back races the tree's asynchronous persistence and can tear; the overlay avoids that and makes deeper in-memory buffering safe.
- State root computation is optionally parallel (`ARB_PARALLEL_STATEROOT`), over an overlay of the unpersisted chain.


## Status

Experimental, and early. The current goal is a proof-of-concept full sync of Arbitrum One from Nitro genesis (L2 block 22207817), replaying every block and checking the state root against canonical Nitro. It is not meant to run as an archive node; history is pruned as the parity check passes. It is not a finished node and is not close to one.

At the time of writing, over two million blocks have replayed clean from genesis. Divergences from Nitro are found and fixed as the sync advances, using a parity tripwire that bisects to the exact block and a per-transaction trace comparison.

Execution from the live sequencer feed has not run yet. The node is wired to consume feed messages, and the L1 derivation runtime pushes derived messages through that same path, but the sync is still far from the tip, so the real-time feed-following mode is unexercised so far. Everything to date is historical replay derived from L1.

Progress on the sync is mostly gated by hardware and budget. A full replay wants fast NVMe and a lot of RAM to keep the working set hot, which means renting a beefy VPS, and with limited funds available that is the practical constraint on how fast this moves. 
Right now I am working on mitigating that by discarding any historical state and running the sync on my laptop. This, however, is explicitly not how a normal node would operate.

## Building and running

This is a Rust workspace pinned to a specific reth revision, revm 41, and alloy 2.0. It also depends on the sibling `arb_revm` and `arb-alloy` repositories.
```
cargo build --release
```

Running the node needs an L1 execution RPC (with historical `eth_getLogs`) and, for the post-Dencun blob era, a beacon (consensus-layer) REST endpoint for blob sidecars. See `arb-reth --help` for flags.

## License

See the workspace license.
