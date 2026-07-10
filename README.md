# arb-reth

An Arbitrum One execution and derivation node built on [reth](https://github.com/paradigmxyz/reth), with a custom Arbitrum revm for the state transition. It derives the L2 chain from L1 (sequencer batches, the delayed inbox, and post-Dencun blobs), executes it with ArbOS semantics, and rebuilds Arbitrum One state from Nitro genesis, checking every block against canonical Nitro.

> **This is an experimental research prototype, not production software.** It is a single-developer, work-in-progress reimplementation of an Arbitrum node. It replays Arbitrum One history from L1 and follows the live sequencer feed, checking each block against canonical Nitro. It makes no correctness, stability, or API guarantees, is unaudited, and should not be relied on for anything real or used to secure funds. It has bugs, and parity divergences are still being found and fixed block by block. Expect it to break.

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
- `arb-reth-rpc`: Arbitrum RPC converters (receipt fields like `gasUsedForL1`), wired into reth's canonical `RpcAddOns`. The node serves the full module fleet (`eth`/`net`/`web3`/`txpool`/`trace`/`debug`, including `eth_getLogs`) over HTTP and WebSocket through reth's own RPC stack.
- `arb-reth-genesis`: imports the Nitro genesis state and verifies it.

The `arb-reth` binary exposes subcommands: `node` (run the node), `snapshot import` / `snapshot read`, `genesis verify`, `rewind` (roll the database back to a block), and `dump-blocks`.

## Performance

- Execution is minted, not re-executed. Produced blocks go into reth's engine tree via `InsertExecutedBlock`, so the tree owns canonicalization and persistence without a second execution pass.
- Parent state is threaded forward through an in-memory ring overlay anchored on a pinned read transaction, rather than read back through the provider. Reading it back races the tree's asynchronous persistence and can tear; the overlay avoids that and makes deeper in-memory buffering safe.
- State root computation runs in parallel by default, over an overlay of the unpersisted chain.


## Status

Experimental, and early. The current goal is a proof-of-concept full sync of Arbitrum One from Nitro genesis (L2 block 22207817), replaying every block and checking the state root against canonical Nitro. It is not meant to run as an archive node; history is pruned as the parity check passes. It is not a finished node and is not close to one.

The full Nitro genesis state root reproduces canonical exactly, over all ~1.27M genesis accounts. From there, roughly 20M blocks have replayed with block-for-block parity; divergences from Nitro are found as the sync advances, and are now extremely rare.

The repo includes a Nitro snapshot converter that turns a Nitro state snapshot into a reth database. Nitro keys state by hash (no preimages), so arb-reth runs on reth's hashed-state (storage v2) tables. reth forward-hashes the address on read, so point-query state RPC (`eth_getBalance`, `eth_getStorageAt`, `eth_getCode`, `eth_call`, traces, logs) works directly against a converted snapshot. The one gap is whole-state enumeration by address (for example `debug_dumpState`), which needs `keccak(address)` preimages that Nitro snapshots do not ship, the same limitation as a default Nitro node.

The node follows the live sequencer feed. A feed follower and the L1-derivation hand-off drive the same block-production path; validated against a local Nitro test node, the derived and feed-followed blocks match canonical bit-for-bit while riding the live tip in lockstep. On Arbitrum One the from-genesis sync is still far from the tip, so mainnet tip-following waits on either finishing the replay or bootstrapping from a snapshot.

Progress on the sync is mostly gated by hardware and budget. A full replay wants fast NVMe and a lot of RAM to keep the working set hot, and with limited funds that is the practical constraint on how fast this moves. Work happens across a mix of a cheap VPS and my laptop, discarding historical state to keep the working set small. This is explicitly not how a normal node would operate.

## Building and running

This is a Rust workspace pinned to a specific reth revision, revm 41, and alloy 2.0. It also depends on the sibling `arbitrum-revm` and `arbitrum-alloy` repositories.
```
cargo build --release
```

Running the node needs an L1 execution RPC (with historical `eth_getLogs`) and, for the post-Dencun blob era, a beacon (consensus-layer) REST endpoint for blob sidecars. See `arb-reth --help` for flags.

## License

Licensed under either of MIT ([LICENSE-MIT](LICENSE-MIT)) or Apache-2.0
([LICENSE-APACHE](LICENSE-APACHE)) at your option. One transitive dependency,
`brotli` (from Offchain Labs' Nitro, via `arb_revm`), is BUSL-1.1; see
[NOTICE](NOTICE) for what that means for running a built binary.
