# Local testnode parity harness

A way to check arb-reth against a controlled Arbitrum chain that runs entirely on localhost.

The mainnet forward sync validates against Arbitrum One, which costs RPC budget and disk and
only exercises the ArbOS version the chain is currently on. This harness instead points arb-reth
at a local [nitro-testnode](https://github.com/OffchainLabs/nitro-testnode): a real Nitro node, its
L1 geth, and the deployed rollup contracts, all in docker. arb-reth derives the testnode's chain
from its L1 and checks every block's state root against the testnode's own Nitro node.

Because the testnode is local and cheap to reset, it gives what the mainnet sync cannot:

- correctness coverage that does not depend on reaching a particular block on Arbitrum One,
- the ability to drive exotic transaction types on demand (deposits, retryables, redeems),
- a chain that can be redeployed at a chosen ArbOS version, so the harness can walk versions.

## What it exercises

arb-reth boots genesis from the testnode's chain config, then runs its normal L1-derivation path
against the testnode L1 (`--l1-rpc`), using the testnode's SequencerInbox and Bridge addresses.
This is the same derivation and execution code the mainnet sync uses, so a match here means the
derivation, the ArbOS execution, and the state-root machinery all agree with Nitro on this chain.

Genesis is bootstrapped from the chain's Initialize message on L1, the way Nitro bootstraps a
fresh chain: that message (delayed-inbox message 0) carries the chain id, the serialized chain
config, and the initial L1 base fee, so none of them are passed by hand. Passing `--l1-rpc`
without `--chain` triggers that path. The genesis anchor is L2 block 0 (a fresh chain), not the
Arbitrum One Nitro genesis at 22207817. Only the `--l1-sequencer-inbox` and `--l1-bridge`
addresses and the block-0 anchors are supplied, so the same binary targets a different chain
without a rebuild.

## Running

Bring up a testnode (from the nitro-testnode dir), then:

```
# build the node once
cargo build --release -p arb-reth-node --bin arb-reth

# capture -> run -> catch up -> compare, then stop arb-reth
./testnode/testnode-parity.sh all
```

Or step through it:

```
./testnode/testnode-parity.sh capture   # pull chain config + contract addresses out of the testnode
./testnode/testnode-parity.sh run        # boot arb-reth against the testnode L1
./testnode/testnode-parity.sh compare    # diff block roots, bisecting to the first divergence
./testnode/testnode-parity.sh stop
```

Artifacts land in `/tmp/arb-testnode-parity` (override with `WORKDIR`). Endpoints and paths are
env-overridable: `L1_RPC`, `L2_RPC`, `ARB_HTTP_PORT`, `TESTNODE_DIR`, `ARB_RETH_BIN`.

## Walking ArbOS versions

The point of a local chain is that you can redeploy it at a chosen ArbOS version and replay each
one. The testnode selects its initial ArbOS version at deploy time; bring it up at the lowest
version of interest, exercise the transaction types you care about, run the harness, then redeploy
at the next version and repeat. Each pass is independent and self-contained.

## Notes

- Genesis is derived from L1, so a genesis (block 0) mismatch means the Initialize-message parse
  did not reproduce the testnode's genesis, which is a real bug to chase, not a missing flag. An
  explicit `--initial-l1-base-fee` still exists as an override if ever needed.
- The testnode uses well-known throwaway keys, so the harness needs no secrets and does not read
  the mainnet `arb-env.sh` credentials.
- The compare step mirrors `arb-parity-monitor.sh`'s monotone-forward bisection: a divergence
  poisons every descendant block, so checking the newest block is enough and the first bad block
  is found by bisection.
