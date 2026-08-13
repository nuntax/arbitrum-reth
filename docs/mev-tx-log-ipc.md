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

No payload is allocated or encoded when no client is connected. A connected client causes only a
bounded broadcast enqueue on the execution thread; binary encoding and socket writes run in its
own task.

## Framing

All integers are unsigned, big-endian. Every frame starts with a four-byte `frameLength` that
excludes the length field itself. The frame body is exactly `frameLength` bytes.

```text
u32 frameLength
bytes[frameLength] body
```

Version 1 uses this body. Its fixed prefix is 64 bytes.

| Offset | Size | Field | Meaning |
| --- | ---: | --- | --- |
| 0 | 1 | `version` | Always `1`. |
| 1 | 1 | `kind` | `0` start-block, `1` user transaction, `2` scheduled retry. |
| 2 | 1 | `success` | `1` for EVM success, `0` for revert or halt. |
| 3 | 1 | `flags` | Reserved. Must be zero in version 1. |
| 4 | 8 | `blockNumber` | Provisional L2 block number. |
| 12 | 8 | `transactionIndex` | Index in the final block transaction order, including start-block. |
| 20 | 8 | `gasUsed` | Final transaction gas used, including refunds. |
| 28 | 32 | `transactionHash` | 32-byte transaction hash. |
| 60 | 4 | `logCount` | Number of log records following the fixed prefix. |

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

Unknown `version`, nonzero version-1 `flags`, an unknown `kind`, malformed lengths, or extra bytes
must be treated as a protocol error. Close and reconnect rather than trying to resynchronize in the
middle of a stream.

The protocol intentionally avoids JSON, hex encoding, and a schema runtime on the hot path. Its
only compatibility commitment is this versioned frame format. Future incompatible changes will use
a new `version` value.
