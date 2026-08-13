//! Best-effort per-transaction execution observations for local consumers.
//!
//! Events leave the ArbOS payload builder immediately after a transaction has executed. They are
//! deliberately not canonical receipts: the enclosing block can still fail while its state root is
//! calculated or while it is handed to the engine tree.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use alloy_evm::EvmEnv;
use alloy_primitives::{B256, Log, keccak256};
use arb_reth_evm::ArbBlockEnv;
use arb_revm::{ArbChainContext, ArbSpecId};
use revm::state::EvmState;
use revm_database::CacheState;
use tokio::sync::broadcast;

/// Number of execution events retained for a slow local consumer before it is disconnected.
pub const TX_LOG_STREAM_CAPACITY: usize = 1_024;

/// Number of exact post-transaction execution frontiers retained for RPC simulation.
pub const EXECUTION_FRONTIER_CAPACITY: usize = TX_LOG_STREAM_CAPACITY;

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
    /// Exact post-transaction state frontier accepted by `arb_simulateAtFrontier`.
    pub frontier_id: B256,
    /// Deterministic ArbOS transaction source.
    pub kind: ArbTxExecutionKind,
    /// Receipt-status equivalent for this transaction.
    pub success: bool,
    /// Final transaction gas used, including refunds.
    pub gas_used: u64,
    /// EVM logs emitted by the transaction. Reverted and halted transactions normally have none.
    pub logs: Vec<Log>,
}

#[derive(Debug)]
struct FrontierBlockBase {
    parent_hash: B256,
    evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
    pre_execution_state: CacheState,
}

#[derive(Debug)]
struct FrontierStateDelta {
    previous: Option<Arc<Self>>,
    update: Arc<EvmState>,
}

/// Immutable execution state immediately after one transaction in a block being built.
#[derive(Clone, Debug)]
pub struct ArbExecutionFrontier {
    /// Stable identifier emitted with the corresponding transaction-log event.
    pub frontier_id: B256,
    /// Provisional L2 block number.
    pub block_number: u64,
    /// Transaction index in ArbOS execution order, including the start-block transaction.
    pub transaction_index: u64,
    /// Hash of the transaction that produced this frontier.
    pub transaction_hash: B256,
    chain_context: ArbChainContext,
    base: Arc<FrontierBlockBase>,
    tail: Arc<FrontierStateDelta>,
}

impl ArbExecutionFrontier {
    /// Hash of the canonical parent state on which the in-progress block is executing.
    pub fn parent_hash(&self) -> B256 {
        self.base.parent_hash
    }

    /// Exact EVM environment used by the in-progress block.
    pub fn evm_env(&self) -> &EvmEnv<ArbSpecId, ArbBlockEnv> {
        &self.base.evm_env
    }

    /// Cache after pre-execution changes, before the first block transaction.
    pub fn pre_execution_state(&self) -> &CacheState {
        &self.base.pre_execution_state
    }

    /// Block-scoped ArbOS context after the observed transaction.
    pub fn chain_context(&self) -> &ArbChainContext {
        &self.chain_context
    }

    /// Returns cumulative transaction state deltas in execution order.
    pub fn state_updates(&self) -> Vec<Arc<EvmState>> {
        let mut updates = Vec::new();
        let mut node = Some(Arc::clone(&self.tail));
        while let Some(current) = node {
            updates.push(Arc::clone(&current.update));
            node = current.previous.clone();
        }
        updates.reverse();
        updates
    }
}

#[derive(Debug, Default)]
struct ExecutionFrontierInner {
    order: VecDeque<B256>,
    frontiers: HashMap<B256, ArbExecutionFrontier>,
}

/// Bounded, thread-safe store of exact post-transaction execution frontiers.
#[derive(Clone, Debug)]
pub struct ArbExecutionFrontierStore {
    inner: Arc<RwLock<ExecutionFrontierInner>>,
    capacity: usize,
}

impl ArbExecutionFrontierStore {
    /// Creates a frontier store retaining at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ExecutionFrontierInner::default())),
            capacity: capacity.max(1),
        }
    }

    /// Looks up an exact frontier. Missing entries are expired or were never observed.
    pub fn get(&self, frontier_id: B256) -> Option<ArbExecutionFrontier> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frontiers
            .get(&frontier_id)
            .cloned()
    }

    fn insert(&self, frontier: ArbExecutionFrontier) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let frontier_id = frontier.frontier_id;
        if inner.frontiers.insert(frontier_id, frontier).is_none() {
            inner.order.push_back(frontier_id);
        }
        while inner.order.len() > self.capacity {
            if let Some(expired) = inner.order.pop_front() {
                inner.frontiers.remove(&expired);
            }
        }
    }
}

impl Default for ArbExecutionFrontierStore {
    fn default() -> Self {
        Self::new(EXECUTION_FRONTIER_CAPACITY)
    }
}

/// Per-block producer handle. State deltas form a persistent linked chain, so retained frontiers
/// share all preceding transaction state instead of cloning the cumulative block state each time.
#[derive(Debug)]
pub struct ArbExecutionFrontierBlock {
    store: ArbExecutionFrontierStore,
    base: Arc<FrontierBlockBase>,
    tail: Option<Arc<FrontierStateDelta>>,
}

impl ArbExecutionFrontierBlock {
    fn new(
        store: ArbExecutionFrontierStore,
        parent_hash: B256,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
        pre_execution_state: CacheState,
    ) -> Self {
        Self {
            store,
            base: Arc::new(FrontierBlockBase {
                parent_hash,
                evm_env,
                pre_execution_state,
            }),
            tail: None,
        }
    }

    /// Advances and retains the exact state after one committed transaction.
    pub fn advance(
        &mut self,
        block_number: u64,
        transaction_index: u64,
        transaction_hash: B256,
        update: EvmState,
        chain_context: ArbChainContext,
    ) -> B256 {
        let mut identity = [0u8; 80];
        identity[..32].copy_from_slice(self.base.parent_hash.as_slice());
        identity[32..40].copy_from_slice(&block_number.to_be_bytes());
        identity[40..48].copy_from_slice(&transaction_index.to_be_bytes());
        identity[48..].copy_from_slice(transaction_hash.as_slice());
        let frontier_id = keccak256(identity);
        let tail = Arc::new(FrontierStateDelta {
            previous: self.tail.clone(),
            update: Arc::new(update),
        });
        self.tail = Some(Arc::clone(&tail));
        self.store.insert(ArbExecutionFrontier {
            frontier_id,
            block_number,
            transaction_index,
            transaction_hash,
            chain_context,
            base: Arc::clone(&self.base),
            tail,
        });
        frontier_id
    }
}

/// Non-blocking publisher for per-transaction execution observations.
///
/// A producer does no event cloning or serialization unless a local consumer is connected. Slow
/// consumers are isolated by Tokio's bounded broadcast channel and cannot delay ArbOS execution.
#[derive(Clone, Debug)]
pub struct ArbTxLogBroadcaster {
    sender: broadcast::Sender<ArbTxLogEvent>,
    frontiers: ArbExecutionFrontierStore,
}

impl ArbTxLogBroadcaster {
    /// Creates a broadcaster with the fixed bounded event buffer.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(TX_LOG_STREAM_CAPACITY);
        Self {
            sender,
            frontiers: ArbExecutionFrontierStore::default(),
        }
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

    /// Returns the execution-frontier store used by the RPC extension.
    pub fn frontier_store(&self) -> ArbExecutionFrontierStore {
        self.frontiers.clone()
    }

    /// Begins tracking one in-progress block after its pre-execution changes have committed.
    pub fn begin_frontier_block(
        &self,
        parent_hash: B256,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
        pre_execution_state: CacheState,
    ) -> ArbExecutionFrontierBlock {
        ArbExecutionFrontierBlock::new(
            self.frontiers.clone(),
            parent_hash,
            evm_env,
            pre_execution_state,
        )
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

    #[test]
    fn frontier_deltas_are_retained_in_execution_order() {
        use alloy_evm::EvmEnv;
        use alloy_primitives::Address;
        use revm::state::{Account, AccountInfo};

        let broadcaster = ArbTxLogBroadcaster::new();
        let mut block = broadcaster.begin_frontier_block(
            B256::repeat_byte(0x11),
            EvmEnv::default(),
            CacheState::default(),
        );
        let address = Address::repeat_byte(0x22);
        let mut first_account = Account::default();
        first_account.info = AccountInfo {
            nonce: 1,
            ..Default::default()
        };
        let mut first = EvmState::default();
        first.insert(address, first_account);
        let first_id = block.advance(
            42,
            0,
            B256::repeat_byte(0x33),
            first,
            ArbChainContext::default(),
        );
        let mut second_account = Account::default();
        second_account.info = AccountInfo {
            nonce: 2,
            ..Default::default()
        };
        let mut second = EvmState::default();
        second.insert(address, second_account);
        let second_id = block.advance(
            42,
            1,
            B256::repeat_byte(0x44),
            second,
            ArbChainContext::default(),
        );

        assert_eq!(
            broadcaster
                .frontier_store()
                .get(first_id)
                .unwrap()
                .state_updates()
                .len(),
            1
        );
        let second = broadcaster.frontier_store().get(second_id).unwrap();
        let updates = second.state_updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0][&address].info.nonce, 1);
        assert_eq!(updates[1][&address].info.nonce, 2);
    }

    #[test]
    fn frontier_store_expires_oldest_entry_without_fallback() {
        let store = ArbExecutionFrontierStore::new(1);
        let broadcaster = ArbTxLogBroadcaster {
            sender: broadcast::channel(1).0,
            frontiers: store.clone(),
        };
        let mut block = broadcaster.begin_frontier_block(
            B256::repeat_byte(0x11),
            EvmEnv::default(),
            CacheState::default(),
        );
        let first_id = block.advance(
            42,
            0,
            B256::repeat_byte(0x33),
            EvmState::default(),
            ArbChainContext::default(),
        );
        let second_id = block.advance(
            42,
            1,
            B256::repeat_byte(0x44),
            EvmState::default(),
            ArbChainContext::default(),
        );

        assert!(store.get(first_id).is_none());
        assert!(store.get(second_id).is_some());
    }
}
