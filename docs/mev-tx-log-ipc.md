# MEV transaction-log IPC

`arb-reth` can stream the final EVM logs for each included ArbOS transaction before it finishes
receipt hashing, state-root calculation, and engine insertion. It is intended for a colocated
latency-sensitive consumer, not as a replacement for RPC or an externally exposed API.

Enable it with:

```sh
arb-reth node ... --mev-tx-log-ipc /run/arb-reth/mev-logs.sock
```

The socket is a Unix `SOCK_STREAM`. It is owner-access controlled by the directory and socket-file
permissions. Do not expose it through a TCP proxy.

## Delivery and validity

Each event is emitted only after the transaction has completed and been accepted by the block
builder. It includes reverted transactions, with `success = 0`; their log list will normally be
empty.

The event is **pre-canonical**. `blockNumber` identifies the block currently being built, but there
is no block hash because the final hash depends on every transaction and the state root. If block
building, root calculation, or engine insertion later fails, discard the events for that provisional
block. Correlate this stream with the normal canonical block/RPC view before taking an action that
requires finality.

The stream is best effort:

- It begins at the next transaction after a client connects. There is no replay.
- Events are FIFO for a connected client.
- A slow client is disconnected once it lags the bounded producer buffer. Reconnect and recover
  from another source if loss matters.
- Consumers must distinguish `(parentHash, blockNumber, attemptId)` execution attempts. Identical
  transactions and indices in a re-executed block are not evidence of identical prefix state.
  On reconnect or a changed attempt, recover a canonical anchor and wait for index zero; do not
  splice a suffix onto a previous attempt.

Binary log payloads are allocated only while a client is connected. When this feature is enabled,
the node also retains a bounded chain of post-transaction state deltas for exact frontier
simulation. Binary encoding and socket writes run outside the execution task.

## Framing

All integers are unsigned, big-endian. Every frame starts with a four-byte `frameLength` that
excludes the length field itself. The frame body is exactly `frameLength` bytes.

```text
u32 frameLength
bytes[frameLength] body
```

Version 3 uses this body. Its fixed prefix is 160 bytes. The first 96 bytes retain the version-2
layout (with version set to 3), followed by the parent hash and random execution-attempt identity.
Version 2 is no longer emitted and its frontier identities are not safe prefix witnesses.

| Offset | Size | Field | Meaning |
| --- | ---: | --- | --- |
| 0 | 1 | `version` | Always `3`. |
| 1 | 1 | `kind` | `0` start-block, `1` user transaction, `2` scheduled retry. |
| 2 | 1 | `success` | `1` for EVM success, `0` for revert or halt. |
| 3 | 1 | `flags` | Reserved. Must be zero in version 3. |
| 4 | 8 | `blockNumber` | Provisional L2 block number. |
| 12 | 8 | `transactionIndex` | Index in the final block transaction order, including start-block. |
| 20 | 8 | `gasUsed` | Final transaction gas used, including refunds. |
| 28 | 32 | `transactionHash` | 32-byte transaction hash. |
| 60 | 32 | `frontierId` | Exact post-transaction state accepted by `arb_simulateAtFrontier`. |
| 92 | 4 | `logCount` | Number of log records following the fixed prefix. |
| 96 | 32 | `parentHash` | Canonical parent used to build this provisional block. |
| 128 | 32 | `attemptId` | Nonzero OS-random identity generated for each block-construction attempt. |

Each of the `logCount` records is encoded consecutively:

```text
bytes[20] address
u8        topicCount
u32       dataLength
bytes[32 * topicCount] topics
bytes[dataLength] data
```

`topicCount` is the log's EVM topic count and must be at most four. `dataLength` may be zero.
The consumer must reject a frame whose fields run past `frameLength` or which leaves trailing bytes
after the final log.

## Consumer requirements

Read exactly four bytes, decode `frameLength`, then read exactly that many additional bytes. Never
assume a socket read aligns with a frame. Set an application maximum before allocating the body;
16 MiB is a reasonable initial ceiling for a local consumer, while the protocol's theoretical
maximum is `u32::MAX` bytes.

Unknown `version`, nonzero version-3 `flags`, an unknown `kind`, malformed lengths, or extra bytes
must be treated as a protocol error. Close and reconnect rather than trying to resynchronize in the
middle of a stream.

The protocol intentionally avoids JSON, hex encoding, and a schema runtime on the hot path. Its
only compatibility commitment is this versioned frame format. Future incompatible changes will use
a new `version` value.

### Prefix identity and discovery

Each frontier commits the entire ordered transaction prefix, not merely the last transaction:

```text
frontierId = keccak256(
    ASCII("RHF3")       // 4 bytes, no terminator
    || parentHash      // 32 bytes
    || blockNumber     // unsigned big-endian u64
    || attemptId       // 32 bytes
    || previousId      // 32 bytes; all zero for transactionIndex == 0
    || transactionIndex // unsigned big-endian u64
    || transactionHash // 32 bytes
)                      // exactly 148 bytes before hashing
```

Indices must start at zero and increase by one, including ArbOS start-block, reverted transactions,
and scheduled retries. The node refuses duplicate identities or out-of-order advancement.
Consumers must recompute every prefix, reject gaps/zero attempt IDs/mismatches, and independently
check the parent against their canonical anchor. The witness proves which node-local execution
was used, not consensus finality or the correctness of an untrusted node.

`arb_frontierCapabilities` takes no arguments and returns:

```json
{
  "frontierVersion": 3,
  "frameVersion": 3,
  "frameFixedBodyBytes": 160,
  "frontierIdScheme": "keccak256-rhf3-prefix-v1",
  "frontierIdFormula": "keccak256(RHF3||parentHash||blockNumberBE64||attemptId||previousFrontierId||transactionIndexBE64||transactionHash)",
  "strictValidation": true,
  "canonicalParentValidation": true
}
```

All numeric capability/version fields are JSON numbers, unlike the simulation's block/index/gas
quantities, which are hex quantity strings. Older servers without this capability contract must
not be used by a version-3 execution client.

## Exact frontier simulation

`arb_simulateAtFrontier` executes an uncommitted transaction against the exact state immediately
after the streamed transaction. It uses the same provisional block environment and ArbOS
block-scoped context, including the Stylus recent-program cache. This avoids the race where
`latest` is still the parent block or has already advanced past the observed transaction.

The method is installed when `--mev-tx-log-ipc` is enabled and is available through the node's
configured JSON-RPC transports. Pass the `frontierId` from the version-3 frame:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "arb_simulateAtFrontier",
  "params": [{
    "frontierId": "0x...",
    "transaction": {
      "from": "0x...",
      "to": "0x...",
      "data": "0x...",
      "value": "0x0",
      "gas": "0x2faf080"
    },
    "validation": false
  }]
}
```

`validation` defaults to `false`. In that mode nonce, sender-code, base-fee, and block-gas-limit
checks are disabled, matching call-style simulation. The transaction still executes the normal
Arbitrum EVM and ArbOS hooks. `gasUsed` reports compute execution gas. `gasUsedForL1` is normally
zero because an RPC transaction has no sequencer poster bytes. The simulation respects the node's
`--rpc.gascap` limit.

With `validation: true`, nonce, base-fee, EIP-3607 sender-code, block-gas-limit and balance checks
are explicitly enabled in the actual Arb EVM configuration. Only ordinary user transaction types
(legacy, EIP-2930, EIP-1559, EIP-7702) are admitted; privileged ArbOS transaction types bypass some
of those checks and are rejected. Explicit nonzero `gas` is required; exceeding `--rpc.gascap`
returns `-32602` without silently changing the transaction. A zero configured RPC cap means no
additional RPC cap, not permission to bypass the chain's own limits. Strict mode preserves the
chain's transaction gas-cap and ArbOS-specific fee rules. Relaxed mode retains call-style gas
clamping for backward compatibility. No signature is verified and no transaction is submitted.

Both modes verify `block_hash(blockNumber - 1) == parentHash` and that the retained frontier still
belongs to the active execution attempt, before and after simulation. The parent need not remain
the latest tip: this check establishes canonical ancestry at that height, not freshness/finality.
A new payload build supersedes every older attempt, even at the same height with the same parent,
before state-provider acquisition or message parsing can fail. Failed payload construction revokes
its handles. A completed payload's frontier still does not
promise later engine insertion; clients must continue enforcing their own age and canonicality gates.

The result contains `frontierId`, `frontierVersion: 3`, `parentHash`, `attemptId`,
`validationChecks`, provisional `blockNumber`, `transactionIndex`,
`transactionHash`, `status`, `returnData`, `gasUsed`, `gasUsedForL1`, `logs`, and an optional
`createdAddress` or halt `error`.

For strict simulation `validationChecks` is
`{"nonce":true,"baseFee":true,"senderCode":true,"blockGasLimit":true,"canonicalParent":true}`.
For relaxed simulation the first four fields are false, while `canonicalParent` remains true.
These describe enabled checks, not a profitability or eventual-inclusion guarantee. Compute-only
simulation still omits the poster-byte L1 fee; execution clients must bound that fee separately.

Frontiers are memory-only and the most recent 1,024 are retained. Error `-32001` means the exact
frontier expired, was never observed by this process, its attempt failed/was superseded, or its
parent is unavailable/noncanonical. Error `-32602` denotes invalid intent, `-32000` an EVM
validation/execution error, and `-32603` a failed simulation worker. EVM revert/halt returns a
normal result with the corresponding status, not a success authorization.
The server never falls back to `latest` or another state. Clients should treat that error as a
miss and avoid using a result from a different state.

## Rollout and rollback boundary

This is a breaking local wire upgrade, not a switch to enable trading. A version-2 Go MEV consumer
must be upgraded or disabled before a v3 node is restarted. The ordinary JSON-RPC methods and
canonical block subscriptions are unchanged. Do not infer posting state from a unit named `shadow`:
the observed `rh-reth-1` Go `robinhood-arb-shadow.service` was armed during the implementation audit.

Before a separately authorized deployment, record the node binary SHA-256, unit/config revision,
chain/genesis, current head, all MEV consumer versions, and the actual posting owner. Save the exact
old binary and config for rollback; do not assume an untracked build directory contains a backup.
Build in an isolated directory with bounded CPU/memory. `arb-reth-rssguard.service` was active on
the observed host and may stop the node under memory pressure; do not disable the guard to make a
build succeed. The writer's independent post latch must also remain effective.

Deploy with all writers disabled, then verify v3 capabilities, frame-prefix continuity, strict
simulation, canonical parent/chain/root parity, and the intended single-writer ownership. Live
receipt/profitability qualification and latency gates remain separate. A failure requires stopping
the new consumer first, restoring the saved node binary/config and compatible consumer, and
rechecking chain health. Restoring a v2 binary must leave any v3-only Rust writer blocked; it must
not silently downgrade to unsafe v2 frontier IDs. Re-enabling Go is a separate, verified writer
handoff, never an automatic rollback side effect.

No node restart, binary replacement, configuration change, or order activation is part of the
code-only frontier-v3 implementation and tests.
