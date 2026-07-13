# `arb-reth snapshot`

Snapshot tools convert a Nitro state export to a reth datadir and inspect its hashed state.

## Import

`snapshot import` writes a new datadir and verifies the resulting state root.

```sh
arb-reth snapshot import \
  --state /data/state.stream \
  --blocks /data/blocks.stream \
  --out /data/arb1 \
  --expect 0x<state-root>
```

- `--state` is the Nitro state stream to import.
- `--expect` is required and must be the expected root.
- `--blocks` is optional, but required when the imported datadir must open at a specific snapshot head. It supplies the head header and stage checkpoints.
- `--out` is created when absent. Do not import into a datadir that contains a node you intend to keep.

Use the same blocks stream with `node --snapshot-head`.

## Read

`snapshot read` opens the converted datadir read-only and queries hashed state using a normal address input.

```sh
arb-reth snapshot read --db /data/arb1 --addr 0x1234...
arb-reth snapshot read --db /data/arb1 --addr 0x1234... --slot 0x0000...
arb-reth snapshot read --db /data/arb1 --addr 0x1234... --list-storage
```

The command prints account data and, when requested, a storage value or the non-zero storage slots for the address.
