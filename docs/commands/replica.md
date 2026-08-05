# `arb-reth-replica`

**Experimental.** Parity monitor for running `arb-reth` as a live replica of an existing
Arbitrum chain. It supervises nothing about the node process itself; it watches two
JSON-RPC endpoints — the local `arb-reth` node and a canonical upstream (typically the
chain's hosted Nitro endpoint) — and continuously answers one question: *is the replayed
chain the same chain?*

## Replica replay path

`arb-reth node` already provides the replay mechanics; this tool adds the verification
boundary on top:

1. **Bootstrap** — seed the datadir from a Nitro state export (`genesis import`) or a
   block snapshot (`snapshot import`), per [`node.md`](node.md).
2. **Catch-up** — `--l1-rpc` drives derivation of historical batches from L1.
3. **Tip following** — `--feed-url` tails the sequencer feed for low-latency blocks.
4. **Verification (this tool)** — poll both heads, compare block hash + state root at a
   confirmation depth, and localize the first divergent height if they ever disagree.

## Usage

Run the node, then point the monitor at it and the canonical endpoint:

```sh
arb-reth node \
  --datadir /data/orbit \
  --chain-info chaininfo.json --genesis genesis.json \
  --l1-rpc https://your-archive-rpc.example \
  --feed-url wss://your-chain-feed.example \
  --http --http.port 8545 &

arb-reth-replica \
  --local-rpc http://127.0.0.1:8545 \
  --canonical-rpc https://your-canonical-nitro.example \
  --poll-interval 5 \
  --status-file /data/replica-status.json
```

## Options

- `--local-rpc`: HTTP JSON-RPC of the local `arb-reth` replica.
- `--canonical-rpc`: HTTP JSON-RPC of the canonical upstream.
- `--poll-interval`: seconds between polls (default 5).
- `--confirm-depth`: compare this many blocks behind `min(local, canonical)` head
  (default 2). Avoids flagging the transient window where the feed puts the replica ahead
  of the canonical RPC, or vice versa.
- `--sync-lag-threshold`: lag above which the replica is reported as `syncing` and
  comparison is skipped (default 8).
- `--max-walkback`: on mismatch, walk back at most this many blocks to find the first
  divergent height (default 256).
- `--status-file`: write the latest status as JSON after every poll (for probes/scrapes).
- `--exit-on-divergence`: exit non-zero on confirmed divergence so a supervisor can act.

## States

- `syncing` — replica is behind by more than the threshold (initial import or L1
  catch-up). Progress only; no comparison.
- `in_sync` — replica is at the tip and the checked block matched hash and state root.
- `diverged` — a checked block mismatched. The report carries both identities and
  `first_diverged`, the earliest disagreeing height found within the walkback window;
  `first_diverged - 1` is the target for `arb-reth rewind`.
- `unavailable` — one side has no block at the comparison height (pruned history, empty
  datadir). Not a divergence verdict.

Transport errors (endpoint down, timeouts) are logged and retried; they never produce a
`diverged` verdict.

## Limitations

- Block hash parity implies header parity (state root, receipts root, logs bloom are all
  committed in the hash), but this does not by itself validate receipt contents, traces,
  or RPC-level behavior. It also isn't a consensus-equivalence proof for unreplayed
  ArbOS/Stylus paths — it proves the replica reproduced the canonical chain, block for
  block, as far as it has replayed.
- Divergence handling is detection only. The operator response today is: stop the node,
  `arb-reth rewind` to `first_diverged - 1`, restart, and investigate the offending
  block with `dump-blocks`. Automated rewind/reconciliation is future work.
- The canonical endpoint is trusted as ground truth. Pointing both flags at nodes of the
  same (wrong) implementation tells you nothing.
