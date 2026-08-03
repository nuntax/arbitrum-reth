# `arb-reth snapshot`

Snapshot tools convert the Arbitrum One Nitro-genesis snapshot into a Storage V2 reth datadir and inspect its hashed state.

## Import workflow

The conversion needs both official source snapshots:

- The [Arbitrum One Classic Export](https://snapshot-explorer.arbitrum.io/?chain=Arbitrum+One&dir=Classic+Export) provides plaintext storage-slot keys.
- The [Nitro genesis Pebble snapshot](https://snapshot.arbitrum.io/arb1/nitro-genesis-pebble-path.tar) provides the canonical hashed state and head header.

Start with a new output directory. The commands below intentionally use the same `--out` path.

### 1. Build slot preimages

Extract the Classic Export, then point `--classic-state` at the directory containing its `index.json` file:

```sh
arb-reth snapshot build-preimages \
  --classic-state /data/classic-export/state/<block> \
  --out /data/arb1
```

This creates a validated preimage store and completion manifest under `/data/arb1/db/preimage`.

### 2. Export state and the snapshot head

Build the `reth-export` helper from `crates/arb-reth-genesis/go-exporter`, using a Nitro checkout so it links against Nitro's geth fork. Run it against the extracted Nitro genesis database:

```sh
reth-export --mode state /data/nitro/l2chaindata > /data/genesis-state.stream
reth-export --mode blocks /data/nitro/l2chaindata > /data/head-block.stream
```

The default blocks range is the database head, which is the record required by the importer and later by `node --snapshot-head`.

### 3. Import and verify

```sh
arb-reth snapshot import \
  --state /data/genesis-state.stream \
  --blocks /data/head-block.stream \
  --out /data/arb1 \
  --expect 0x7f2bfc4481d02bfcfc606ebb949384ef78d03a0f30a2dc9cccd652eb80926ae1
```

- `--state` is the Nitro state stream to import.
- `--blocks` is required. It supplies the canonical snapshot-head header and stage checkpoints.
- `--expect` is required. The importer verifies the computed root and snapshot identity.
- `--out` must be fresh except for the preimage sidecar created in step 1.

If an import fails after creating database files, discard the entire output directory and restart from step 1. The importer refuses to continue in a partially written target.

A successful import writes `snapshot-import.json` only after the state root, canonical head, launch check, and static-file layout all pass. The node refuses to boot a new-format snapshot datadir without this completion manifest.

Use `/data/head-block.stream` with `node --snapshot-head` when starting the converted datadir.

## Storage V2 preimages

The Nitro state stream contains hashed storage keys, but Storage V2 changesets use plain slot keys. Those plain keys are required to record reversible storage wipes and to unwind safely below the imported head.

Keep `db/preimage`, including its manifest, with the datadir permanently. Include it when moving or backing up the database. It is not temporary conversion data. The node adds newly observed slot preimages while syncing, while the Classic Export supplies the historical set present at the snapshot head.

## Read

`snapshot read` opens the converted datadir read-only and queries hashed state using a normal address input.

```sh
arb-reth snapshot read --db /data/arb1 --addr 0x1234...
arb-reth snapshot read --db /data/arb1 --addr 0x1234... --slot 0x0000...
arb-reth snapshot read --db /data/arb1 --addr 0x1234... --list-storage
```

The command prints account data and, when requested, a storage value or the non-zero storage slots for the address.
