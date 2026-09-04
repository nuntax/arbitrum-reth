# `arb-reth rewind`

`rewind` removes blocks above a chosen L2 height and truncates the L1 resume log to a compatible boundary. Stop the node before running it.

For a confirmed first divergent block `N`, keep `N - 1`:

```sh
arb-reth rewind \
  --datadir /data/arb1 \
  --snapshot-head /data/head.stream \
  --diverged-at N
```

Use `--to <block>` when the desired surviving tip is already known. Run `--dry-run` first to inspect the target without writing.

The node keeps recent L1 derivation boundaries densely and compacted historical boundaries for
deep recovery. `rewind` refuses to modify the database when it cannot find a safe boundary at or
below the target. `--allow-genesis-rescan` overrides that guard, but the following node start must
re-derive every message from Nitro genesis before producing a new block.

Resume logs created by older releases only contain their final 128 boundaries. Updating cannot
restore history those releases already discarded. After upgrading, newly observed boundaries are
retained in the compacted history; until then, a rewind older than the legacy log is refused.

The boot information must match the datadir:

- Snapshot-seeded datadir: pass `--snapshot-head`.
- Orbit datadir: pass `--chain-info <chaininfo.json> --genesis <genesis.json>`.

Do not rewind because a reference RPC temporarily lacks a tip block. Confirm the mismatch with stable, non-null state roots first.
