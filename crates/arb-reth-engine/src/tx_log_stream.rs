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

/// Wire and identity version; a version-2 handle must never be used as a prefix proof.
pub const EXECUTION_FRONTIER_VERSION: u8 = 3;
pub const EXECUTION_FRONTIER_ID_SCHEME: &str = "keccak256-rhf3-prefix-v1";
pub const EXECUTION_FRONTIER_ID_FORMULA: &str = "keccak256(RHF3||parentHash||blockNumberBE64||attemptId||previousFrontierId||transactionIndexBE64||transactionHash)";

/// Hash a complete execution-prefix witness. Index zero uses a zero previous ID.
pub fn execution_frontier_id(
    parent_hash: B256,
    block_number: u64,
    attempt_id: B256,
    previous_id: B256,
    transaction_index: u64,
    transaction_hash: B256,
) -> B256 {
    let mut identity = [0u8; 148];
    identity[..4].copy_from_slice(b"RHF3");
    identity[4..36].copy_from_slice(parent_hash.as_slice());
    identity[36..44].copy_from_slice(&block_number.to_be_bytes());
    identity[44..76].copy_from_slice(attempt_id.as_slice());
    identity[76..108].copy_from_slice(previous_id.as_slice());
    identity[108..116].copy_from_slice(&transaction_index.to_be_bytes());
    identity[116..].copy_from_slice(transaction_hash.as_slice());
    keccak256(identity)
}

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
    /// Canonical parent and random block-attempt identity used by the prefix hash.
    pub parent_hash: B256,
    pub attempt_id: B256,
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
    attempt_id: B256,
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

    /// Random identity of this particular execution of the provisional block.
    pub fn attempt_id(&self) -> B256 {
        self.base.attempt_id
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
    active_attempt: Option<B256>,
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
        let inner = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .frontiers
            .get(&frontier_id)
            .filter(|frontier| Some(frontier.attempt_id()) == inner.active_attempt)
            .cloned()
    }

    /// Recheck after simulation: retaining an Arc cannot authorize an old attempt.
    pub fn is_current(&self, frontier: &ArbExecutionFrontier) -> bool {
        self.get(frontier.frontier_id).is_some_and(|current| {
            current.attempt_id() == frontier.attempt_id()
                && current.parent_hash() == frontier.parent_hash()
        })
    }

    fn activate(&self, attempt_id: B256) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_attempt = Some(attempt_id);
    }

    fn invalidate_attempt(&self, attempt_id: B256) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.active_attempt == Some(attempt_id) {
            inner.active_attempt = None;
        }
    }

    fn insert(&self, frontier: ArbExecutionFrontier) -> eyre::Result<()> {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let frontier_id = frontier.frontier_id;
        if inner.active_attempt != Some(frontier.attempt_id()) {
            eyre::bail!("execution attempt superseded");
        }
        if inner.frontiers.contains_key(&frontier_id) {
            eyre::bail!("duplicate execution frontier identity");
        }
        inner.frontiers.insert(frontier_id, frontier);
        inner.order.push_back(frontier_id);
        while inner.order.len() > self.capacity {
            if let Some(expired) = inner.order.pop_front() {
                inner.frontiers.remove(&expired);
            }
        }
        Ok(())
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
    previous_id: B256,
    next_index: u64,
    completed: bool,
}

impl ArbExecutionFrontierBlock {
    fn new(
        store: ArbExecutionFrontierStore,
        parent_hash: B256,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
        pre_execution_state: CacheState,
    ) -> eyre::Result<Self> {
        // OS entropy failure must stop this attempt, not produce a predictable
        // cross-process incarnation. A zero draw is retried with a bounded limit.
        let attempt_id = (0..4)
            .find_map(|_| B256::try_random().ok().filter(|id| *id != B256::ZERO))
            .ok_or_else(|| eyre::eyre!("execution attempt entropy unavailable"))?;
        store.activate(attempt_id);
        Ok(Self {
            store,
            base: Arc::new(FrontierBlockBase {
                parent_hash,
                attempt_id,
                evm_env,
                pre_execution_state,
            }),
            tail: None,
            previous_id: B256::ZERO,
            next_index: 0,
            completed: false,
        })
    }

    /// Mark successful payload construction. Retention still does not imply canonical inclusion.
    pub fn complete(&mut self) {
        self.completed = true;
    }

    pub fn attempt_id(&self) -> B256 {
        self.base.attempt_id
    }

    /// Advances and retains the exact state after one committed transaction.
    pub fn advance(
        &mut self,
        block_number: u64,
        transaction_index: u64,
        transaction_hash: B256,
        update: EvmState,
        chain_context: ArbChainContext,
    ) -> eyre::Result<B256> {
        if transaction_index != self.next_index
            || self.base.evm_env.block_env.inner.number
                != alloy_primitives::U256::from(block_number)
        {
            eyre::bail!("execution frontier order or block mismatch");
        }
        let next_index = transaction_index
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("execution frontier index overflow"))?;
        let frontier_id = execution_frontier_id(
            self.base.parent_hash,
            block_number,
            self.base.attempt_id,
            self.previous_id,
            transaction_index,
            transaction_hash,
        );
        let tail = Arc::new(FrontierStateDelta {
            previous: self.tail.clone(),
            update: Arc::new(update),
        });
        self.store.insert(ArbExecutionFrontier {
            frontier_id,
            block_number,
            transaction_index,
            transaction_hash,
            chain_context,
            base: Arc::clone(&self.base),
            tail: Arc::clone(&tail),
        })?;
        self.tail = Some(tail);
        self.previous_id = frontier_id;
        self.next_index = next_index;
        Ok(frontier_id)
    }
}

impl Drop for ArbExecutionFrontierBlock {
    fn drop(&mut self) {
        if !self.completed {
            self.store.invalidate_attempt(self.base.attempt_id);
        }
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

    /// Revoke earlier attempts before any fallible preparation of a new payload.
    pub fn invalidate_frontiers(&self) {
        self.frontiers
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_attempt = None;
    }

    /// Begins tracking one in-progress block after its pre-execution changes have committed.
    pub fn begin_frontier_block(
        &self,
        parent_hash: B256,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
        pre_execution_state: CacheState,
    ) -> eyre::Result<ArbExecutionFrontierBlock> {
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

    fn test_env() -> EvmEnv<ArbSpecId, ArbBlockEnv> {
        let mut env: EvmEnv<ArbSpecId, ArbBlockEnv> = EvmEnv::default();
        env.block_env.inner.number = alloy_primitives::U256::from(42);
        env
    }

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
        use alloy_primitives::Address;
        use revm::state::{Account, AccountInfo};

        let broadcaster = ArbTxLogBroadcaster::new();
        let mut block = broadcaster
            .begin_frontier_block(B256::repeat_byte(0x11), test_env(), CacheState::default())
            .unwrap();
        let address = Address::repeat_byte(0x22);
        let mut first_account = Account::default();
        first_account.info = AccountInfo {
            nonce: 1,
            ..Default::default()
        };
        let mut first = EvmState::default();
        first.insert(address, first_account);
        let first_id = block
            .advance(
                42,
                0,
                B256::repeat_byte(0x33),
                first,
                ArbChainContext::default(),
            )
            .unwrap();
        let mut second_account = Account::default();
        second_account.info = AccountInfo {
            nonce: 2,
            ..Default::default()
        };
        let mut second = EvmState::default();
        second.insert(address, second_account);
        let second_id = block
            .advance(
                42,
                1,
                B256::repeat_byte(0x44),
                second,
                ArbChainContext::default(),
            )
            .unwrap();

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
        let mut block = broadcaster
            .begin_frontier_block(B256::repeat_byte(0x11), test_env(), CacheState::default())
            .unwrap();
        let first_id = block
            .advance(
                42,
                0,
                B256::repeat_byte(0x33),
                EvmState::default(),
                ArbChainContext::default(),
            )
            .unwrap();
        let second_id = block
            .advance(
                42,
                1,
                B256::repeat_byte(0x44),
                EvmState::default(),
                ArbChainContext::default(),
            )
            .unwrap();

        assert!(store.get(first_id).is_none());
        assert!(store.get(second_id).is_some());
    }

    #[test]
    fn prefix_identity_commits_every_fixed_width_field() {
        let parent = B256::repeat_byte(0x11);
        let attempt = B256::repeat_byte(0x22);
        let previous = B256::repeat_byte(0x33);
        let tx = B256::repeat_byte(0x44);
        // Independent concatenation oracle for the public 148-byte protocol, including domain.
        let bytes = [
            b"RHF3".as_slice(),
            parent.as_slice(),
            &42u64.to_be_bytes(),
            attempt.as_slice(),
            previous.as_slice(),
            &7u64.to_be_bytes(),
            tx.as_slice(),
        ]
        .concat();
        assert_eq!(bytes.len(), 148);
        let id = execution_frontier_id(parent, 42, attempt, previous, 7, tx);
        assert_eq!(
            id,
            alloy_primitives::b256!(
                "8badcee9394a8d15f450bc007511b5377466466dc4b61bdee56023aea19b5caf"
            )
        );
        assert_eq!(id, keccak256(bytes));
        for other in [
            execution_frontier_id(B256::ZERO, 42, attempt, previous, 7, tx),
            execution_frontier_id(parent, 43, attempt, previous, 7, tx),
            execution_frontier_id(parent, 42, B256::ZERO, previous, 7, tx),
            execution_frontier_id(parent, 42, attempt, B256::ZERO, 7, tx),
            execution_frontier_id(parent, 42, attempt, previous, 8, tx),
            execution_frontier_id(parent, 42, attempt, previous, 7, B256::ZERO),
        ] {
            assert_ne!(id, other);
        }
    }

    #[test]
    fn rederivation_expires_old_attempt_and_cannot_alias_identical_transactions() {
        let broadcaster = ArbTxLogBroadcaster::new();
        let store = broadcaster.frontier_store();
        let parent = B256::repeat_byte(0x11);
        let tx = B256::repeat_byte(0x22);
        let mut first = broadcaster
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        let first_id = first
            .advance(42, 0, tx, EvmState::default(), ArbChainContext::default())
            .unwrap();
        let held = store.get(first_id).unwrap();
        assert_ne!(held.attempt_id(), B256::ZERO);
        let mut second = broadcaster
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        assert_ne!(first.attempt_id(), second.attempt_id());
        assert!(store.get(first_id).is_none());
        assert!(!store.is_current(&held));
        assert!(
            first
                .advance(42, 1, tx, EvmState::default(), ArbChainContext::default())
                .is_err()
        );
        let second_id = second
            .advance(42, 0, tx, EvmState::default(), ArbChainContext::default())
            .unwrap();
        assert_ne!(first_id, second_id);
        assert!(store.is_current(&store.get(second_id).unwrap()));
    }

    #[test]
    fn advances_require_contiguous_indices_and_preserve_existing_identity() {
        let broadcaster = ArbTxLogBroadcaster::new();
        let parent = B256::repeat_byte(0x11);
        let mut block = broadcaster
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        let tx = B256::repeat_byte(0x22);
        assert!(
            block
                .advance(42, 1, tx, EvmState::default(), ArbChainContext::default())
                .is_err()
        );
        assert!(
            block
                .advance(43, 0, tx, EvmState::default(), ArbChainContext::default())
                .is_err()
        );
        let first = block
            .advance(42, 0, tx, EvmState::default(), ArbChainContext::default())
            .unwrap();
        assert_eq!(
            first,
            execution_frontier_id(parent, 42, block.attempt_id(), B256::ZERO, 0, tx)
        );
        let second = block
            .advance(42, 1, tx, EvmState::default(), ArbChainContext::default())
            .unwrap();
        assert_eq!(
            second,
            execution_frontier_id(parent, 42, block.attempt_id(), first, 1, tx)
        );
        let store = broadcaster.frontier_store();
        let mut duplicate = store.get(first).unwrap();
        duplicate.transaction_hash = B256::ZERO;
        assert!(store.insert(duplicate).is_err());
        assert_eq!(store.get(first).unwrap().transaction_hash, tx);
        assert!(
            block
                .advance(42, 1, tx, EvmState::default(), ArbChainContext::default())
                .is_err()
        );
    }

    #[test]
    fn failed_attempt_drop_revokes_handles_but_cannot_revoke_a_newer_attempt() {
        let stream = ArbTxLogBroadcaster::new();
        let store = stream.frontier_store();
        let parent = B256::repeat_byte(0x11);
        let mut block = stream
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        let id = block
            .advance(
                42,
                0,
                B256::ZERO,
                EvmState::default(),
                ArbChainContext::default(),
            )
            .unwrap();
        let held = store.get(id).unwrap();
        drop(block);
        assert!(!store.is_current(&held));

        let old = stream
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        let mut new = stream
            .begin_frontier_block(parent, test_env(), CacheState::default())
            .unwrap();
        let id = new
            .advance(
                42,
                0,
                B256::ZERO,
                EvmState::default(),
                ArbChainContext::default(),
            )
            .unwrap();
        drop(old);
        assert!(store.get(id).is_some());
        new.complete();
        drop(new);
        assert!(store.get(id).is_some());
    }
}
