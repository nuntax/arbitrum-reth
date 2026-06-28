# reth-export (Go)

Streams a Nitro geth pebble DB (path-scheme state + ancient freezer) → line-oriented
`A`/`C`/`S` (state) and `H`/`B`/`R` (blocks) records consumed by `arb-snapshot-import`.

`reth-export.go` is a **copy for version control** — the `nitro/` checkout in this
workspace is not a git repo. Build/run it from the Nitro module so it links the geth fork:

```
cp reth-export.go <nitro>/cmd/reth-export/main.go   # if not already there
cd <nitro> && go build -o reth-export-bin ./cmd/reth-export/
./reth-export-bin --mode state   <l2chaindata-dir>   # A/C/S stream
./reth-export-bin --mode blocks  <l2chaindata-dir>   # H/B/R stream (--from/--to)
```
