//! L1 `SequencerInbox` ABI surface: the `SequencerBatchDelivered` event (for the
//! log topic filter) and the batch-posting functions (for recovering the batch
//! payload from transaction calldata).
//!
//! The event's non-indexed data is decoded by
//! [`arb_reth_derive::batch::parse_sequencer_batch_delivered`]; here `sol!` is used
//! only to derive the canonical topic-0 hash and the call selectors.

use alloy_primitives::{address, Address};
use alloy_sol_types::sol;

/// Arbitrum One `SequencerInbox` proxy (verified against the `to` of batch-poster
/// txns, e.g. batch seq 497980 at L1 block 19000015).
pub const SEQUENCER_INBOX_MAINNET: Address =
    address!("0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6");

/// Arbitrum One `Bridge` proxy. Sole emitter of `MessageDelivered` for this chain;
/// the authoritative source of delayed-message metadata and index numbering.
/// (Other Arbitrum deployments share the event signature, so pin to this address.)
pub const BRIDGE_MAINNET: Address = address!("0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a");

sol! {
    /// timeBounds tuple, encoded inline (4 static `uint64` words) in the event data.
    struct TimeBounds {
        uint64 minTimestamp;
        uint64 maxTimestamp;
        uint64 minBlockNumber;
        uint64 maxBlockNumber;
    }

    /// Emitted by `SequencerInbox` for every posted batch. Used for its topic-0
    /// (`SIGNATURE_HASH`); the data layout is decoded by arb-reth-derive.
    event SequencerBatchDelivered(
        uint256 indexed batchSequenceNumber,
        bytes32 indexed beforeAcc,
        bytes32 indexed afterAcc,
        bytes32 delayedAcc,
        uint256 afterDelayedMessagesRead,
        TimeBounds timeBounds,
        uint8 dataLocation
    );

    /// Emitted by `Bridge` when a delayed message enters the delayed inbox. Carries
    /// the message metadata; the body arrives via `InboxMessageDelivered`.
    event MessageDelivered(
        uint256 indexed messageIndex,
        bytes32 indexed beforeInboxAcc,
        address inbox,
        uint8 kind,
        address sender,
        bytes32 messageDataHash,
        uint256 baseFeeL1,
        uint64 timestamp
    );

    /// Emitted by an inbox with the delayed message body inline.
    event InboxMessageDelivered(uint256 indexed messageNum, bytes data);

    /// Emitted by an inbox when the body lives in the tx calldata instead.
    event InboxMessageDeliveredFromOrigin(uint256 indexed messageNum);
}

/// `sendL2MessageFromOrigin(bytes messageData)`: the inbox call backing
/// `InboxMessageDeliveredFromOrigin`; the body is the `messageData` argument.
pub mod from_origin {
    use alloy_sol_types::sol;
    sol! {
        function sendL2MessageFromOrigin(bytes messageData) external returns (uint256);
    }
}

/// Current 6-arg origin poster (selector `0x8f111f3c`). The batch payload is the
/// `data` argument.
pub mod origin {
    use alloy_sol_types::sol;
    sol! {
        function addSequencerL2BatchFromOrigin(
            uint256 sequenceNumber,
            bytes data,
            uint256 afterDelayedMessagesRead,
            address gasRefunder,
            uint256 prevMessageCount,
            uint256 newMessageCount
        ) external;
    }
}

/// Legacy 4-arg origin poster (pre prev/new message-count upgrade). Separate `sol!`
/// expansion so the overloaded Solidity name does not collide with [`origin`].
pub mod origin_legacy {
    use alloy_sol_types::sol;
    sol! {
        function addSequencerL2BatchFromOrigin(
            uint256 sequenceNumber,
            bytes data,
            uint256 afterDelayedMessagesRead,
            address gasRefunder
        ) external;
    }
}
