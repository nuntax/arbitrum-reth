//! Best-effort per-transaction execution observations for local consumers.
//!
//! Events leave the ArbOS payload builder immediately after a transaction has executed. They are
//! deliberately not canonical receipts: the enclosing block can still fail while its state root is
//! calculated or while it is handed to the engine tree.

use alloy_primitives::{B256, Log};
use tokio::sync::broadcast;

/// Number of execution events retained for a slow local consumer before it is disconnected.
pub const TX_LOG_STREAM_CAPACITY: usize = 1_024;

/// The source of a transaction in the deterministic ArbOS block order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArbTxExecutionKind {
    /// ArbOS's synthetic start-block transaction.
    StartBlock,
    /// A transaction carried by the sequencer or delayed message.
    User,
    /// A retry scheduled by a preceding transaction in the same block.
    ScheduledRetry,
}

/// Logs and final execution status for one successfully included transaction.
///
/// `block_number` and `transaction_index` identify the block currently being built. There is no
/// block hash because that is only available after all transactions, receipt hashing, and state-root
/// calculation have completed.
#[derive(Clone, Debug)]
pub struct ArbTxLogEvent {
    /// Number of the block currently being built.
    pub block_number: u64,
    /// Index in the final transaction order, including the start-block transaction.
    pub transaction_index: u64,
    /// Hash of the included transaction.
    pub transaction_hash: B256,
    /// Deterministic ArbOS transaction source.
    pub kind: ArbTxExecutionKind,
    /// Receipt-status equivalent for this transaction.
    pub success: bool,
    /// Final transaction gas used, including refunds.
    pub gas_used: u64,
    /// EVM logs emitted by the transaction. Reverted and halted transactions normally have none.
    pub logs: Vec<Log>,
}

/// Non-blocking publisher for per-transaction execution observations.
///
/// A producer does no event cloning or serialization unless a local consumer is connected. Slow
/// consumers are isolated by Tokio's bounded broadcast channel and cannot delay ArbOS execution.
#[derive(Clone, Debug)]
pub struct ArbTxLogBroadcaster {
    sender: broadcast::Sender<ArbTxLogEvent>,
}

impl ArbTxLogBroadcaster {
    /// Creates a broadcaster with the fixed bounded event buffer.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(TX_LOG_STREAM_CAPACITY);
        Self { sender }
    }

    /// Returns whether a local consumer is currently connected.
    #[inline]
    pub fn has_subscribers(&self) -> bool {
        self.sender.receiver_count() != 0
    }

    /// Adds one local consumer to the event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<ArbTxLogEvent> {
        self.sender.subscribe()
    }

    /// Publishes an event without ever waiting for a consumer.
    #[inline]
    pub fn publish(&self, event: ArbTxLogEvent) {
        let _ = self.sender.send(event);
    }
}

impl Default for ArbTxLogBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_only_when_a_consumer_is_connected() {
        let broadcaster = ArbTxLogBroadcaster::new();
        assert!(!broadcaster.has_subscribers());

        let receiver = broadcaster.subscribe();
        assert!(broadcaster.has_subscribers());
        drop(receiver);

        assert!(!broadcaster.has_subscribers());
    }
}
