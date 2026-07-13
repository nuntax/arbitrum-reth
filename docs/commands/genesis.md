# `arb-reth genesis`

Genesis verification computes a state root without starting a node.

## Verify a Nitro classic-state export

```sh
arb-reth genesis verify /data/genesis-export/state/0x152dd48
```

The default path is `genesis-export/state/0x152dd48`. Use `--arbos-only` to verify only ArbOS storage. `--dump <address,...>` prints selected trie leaves for differential debugging.

## Verify a `reth-export` state stream

```sh
reth-export --mode state /data/source | \
  arb-reth genesis verify-export 0x<expected-state-root>
```

Omit the expected root to print the computed root without asserting it.
