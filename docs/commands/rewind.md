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

The boot information must match the datadir:

- Snapshot-seeded datadir: pass `--snapshot-head`.
- Orbit datadir: pass `--chain-info <chaininfo.json> --genesis <genesis.json>`.

Do not rewind because a reference RPC temporarily lacks a tip block. Confirm the mismatch with stable, non-null state roots first.
