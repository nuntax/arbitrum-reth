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
- Consumers should deduplicate by `(blockNumber, transactionIndex, transactionHash)` across a
  reconnect or a producer restart.

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

Version 2 uses this body. Its fixed prefix is 96 bytes.

| Offset | Size | Field | Meaning |
| --- | ---: | --- | --- |
| 0 | 1 | `version` | Always `2`. |
| 1 | 1 | `kind` | `0` start-block, `1` user transaction, `2` scheduled retry. |
| 2 | 1 | `success` | `1` for EVM success, `0` for revert or halt. |
| 3 | 1 | `flags` | Reserved. Must be zero in version 2. |
| 4 | 8 | `blockNumber` | Provisional L2 block number. |
| 12 | 8 | `transactionIndex` | Index in the final block transaction order, including start-block. |
| 20 | 8 | `gasUsed` | Final transaction gas used, including refunds. |
| 28 | 32 | `transactionHash` | 32-byte transaction hash. |
| 60 | 32 | `frontierId` | Exact post-transaction state accepted by `arb_simulateAtFrontier`. |
| 92 | 4 | `logCount` | Number of log records following the fixed prefix. |

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

Unknown `version`, nonzero version-2 `flags`, an unknown `kind`, malformed lengths, or extra bytes
must be treated as a protocol error. Close and reconnect rather than trying to resynchronize in the
middle of a stream.

The protocol intentionally avoids JSON, hex encoding, and a schema runtime on the hot path. Its
only compatibility commitment is this versioned frame format. Future incompatible changes will use
a new `version` value.

## Exact frontier simulation

`arb_simulateAtFrontier` executes an uncommitted transaction against the exact state immediately
after the streamed transaction. It uses the same provisional block environment and ArbOS
block-scoped context, including the Stylus recent-program cache. This avoids the race where
`latest` is still the parent block or has already advanced past the observed transaction.

The method is installed when `--mev-tx-log-ipc` is enabled and is available through the node's
configured JSON-RPC transports. Pass the `frontierId` from the version-2 frame:

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

The result contains `frontierId`, provisional `blockNumber`, `transactionIndex`,
`transactionHash`, `status`, `returnData`, `gasUsed`, `gasUsedForL1`, `logs`, and an optional
`createdAddress` or halt `error`.

Frontiers are memory-only and the most recent 1,024 are retained. Error `-32001` means the exact
frontier expired, was never observed by this process, or its parent state is no longer available.
The server never falls back to `latest` or another state. Clients should treat that error as a
miss and avoid using a result from a different state.
