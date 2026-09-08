# Frontier v3 verification receipt

Code-only integration based on `6100b7c7826e185e9444e16deec1e9014ff79c7d`,
branch `codex/frontier-v3-execution`. No deployment, node restart, configuration change, or order
activation. Review caught stale completed handles surviving failures before guard construction;
accepted fix invalidates at native build and production entry, with a real early-failure regression.

## Linux verification

An isolated source and **copied** Cargo target cache were used on the existing `rh-reth-1` Linux
host, under `/ssd1/build/frontier-v3-test.uhGVKWkn`. Build concurrency was two low-priority jobs.
Live node PID remained 654; the RSS guard and independent posting latch were not changed.
No external RPC was called. Fixture tests use their own temporary databases/loopback sockets.

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`,
`cargo 1.98.1 (797e8a9bc 2026-08-05)`.
The existing node build's dependency lock was copied into the test source, SHA-256
`93fd647fe97e2218b186d77643c692d5dd220ba2f591682fa5e59431420b7f58`.
The repository does not track a lock file; the copied lock was not committed.

From the isolated source directory:

```sh
CARGO_TARGET_DIR=/ssd1/build/frontier-v3-test.uhGVKWkn/target nice -n 10 \
  /home/alphanonce/.cargo/bin/cargo test --locked --offline --release -j 2 \
  -p arb-reth-engine -p arb-reth-node --lib frontier

CARGO_TARGET_DIR=/ssd1/build/frontier-v3-test.uhGVKWkn/target nice -n 10 \
  /home/alphanonce/.cargo/bin/cargo test --locked --offline --release -j 2 \
  -p arb-reth-engine -p arb-reth-node --lib
```

- Frontier filter: engine **6 passed**, node **5 passed**.
- Complete libraries: engine **25 passed**, node **73 passed / 1 ignored** (existing manual
  deep-buffer persistence stress test). No failures. Final cached command build 0.55s;
  test runtimes 0.75s and 1.60s, respectively. These are test durations, not trading latency.
- The first actual-EVM test attempt failed in its helper because RPC conversion rejected a low
  fee before reaching the EVM. The helper now constructs the test TxEnv after production flag
  preparation to independently exercise the handler. Production conversion/rejection was retained.
- The early-failure test initially used `Default` for a pinned provider mock exposing only `new`;
  corrected before the passing run. No production gate was weakened to pass either test.
- A dependency emits a future-incompatibility warning for `proc-macro-error2 2.0.1`.

Test executable SHA-256:

```text
c3160967f4737b2be19991d41617c0799323d669b20da4c871027a5975dcd2fe  arb_reth_engine-36b24c306e3fee2f
aab504e6cbdd28310e885cd52f3021140c56c6d3435355e5c5bda5ac9086fe6b  arb_reth_node-dc7a79f93ff13c81
```

These are library test executables, not a deployable node CLI build. The copied cache's old
`target/release/arb-reth` was renamed `arb-reth.baseline-cache-only` to prevent accidental use.
The real `/usr/local/bin/arb-reth` binary was untouched.

All seven changed source/manifest files were hashed locally and remotely after the passing run;
the pairs were identical:

```text
1a5b58f035c77d668bb4a4a6aaeb6d910bc162b5c844c7a34c0e91145af9adba  crates/arb-reth-engine/Cargo.toml
94299e69204e9d106b3ea149cd566e2fb60ec9d5708178629103385c0b8a98af  crates/arb-reth-engine/src/engine.rs
dd5131abb7ec302e26c50f9c4e4c825147476afce5e1115b6b403792711001c8  crates/arb-reth-engine/src/lib.rs
145ea196efd98734703e60db43fa7fcec99e0421beb1a142da50b62ada14f282  crates/arb-reth-engine/src/native_payload.rs
bd35abcb1b0b247899a5d6f1c0f8b634f9364203f473087bf7ced3fa6afbce5f  crates/arb-reth-engine/src/tx_log_stream.rs
4f0f9ff2321fe9c9d543dc92a75c54648b407e04277ea437859a3e8139c2027f  crates/arb-reth-node/src/mev_frontier_rpc.rs
a209647a480141b3ac685502c548a6eda4f3d2106e956895a223913d405dad49  crates/arb-reth-node/src/mev_tx_logs.rs
```

## Scope of assurance

Touched-file rustfmt checks and `git diff --check` passed. Workspace-wide rustfmt check has
pre-existing differences in untouched files; those were not rewritten. CI separately runs Rust
1.97.1 `cargo check --workspace --all-targets`, Clippy with warnings denied, and workspace tests;
CI status belongs to the PR and is not implied by this local receipt.

Tests cover exact 148-byte golden hash/field binding, contiguous order, duplicate rejection,
same-parent rederivation, failed-attempt drop, failure before execution starts, cumulative state
retention, expiry, v3 binary offsets/socket delivery, explicit capability/result flags, strict gas
intent, privileged-type rejection, canonical-parent recheck, and actual Arb EVM strict/relaxed
nonce/base-fee/sender-code/block-gas validation.

No live frontier simulation against the deployed executor, continuous chain-root parity,
end-to-end signed posting, or profitability qualification is established by these tests.
Follow the rollout/rollback boundary in [the protocol](mev-tx-log-ipc.md) before deployment.
