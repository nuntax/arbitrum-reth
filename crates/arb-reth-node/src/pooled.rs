//! D.3 — `ArbPooledTransaction`: Arbitrum pooled tx wrapping `EthPooledTransaction<ArbTxEnvelope>`.
//!
//! The [`NoopTransactionPool`] rejects all txs, but `NodeBuilder` requires a
//! [`PoolTransaction`] type that satisfies `EthPoolTransaction<Consensus = ArbTxEnvelope>`.
//!
//! Arbitrum does not have separate consensus/pooled tx variants (no blob sidecars),
//! so `Consensus == Pooled == ArbTxEnvelope` with `TryFromConsensusError = Infallible`.

use core::convert::Infallible;

use alloy_consensus::Transaction;
use alloy_eips::eip2718::Typed2718;
use alloy_eips::eip4844::{BlobTransactionValidationError, env_settings::KzgSettings};
use alloy_eips::eip7594::BlobTransactionSidecarVariant;
use alloy_primitives::{Address, B256, TxHash, TxKind, U256};
use alloy_rlp::Encodable;
use arb_alloy_consensus::ArbTxEnvelope;
use reth_evm::RecoveredTx;
use reth_primitives_traits::{InMemorySize, Recovered};
use reth_transaction_pool::{
    EthBlobTransactionSidecar, EthPoolTransaction, EthPooledTransaction, PoolTransaction,
};

/// Wrapper around [`EthPooledTransaction<ArbTxEnvelope>`] that satisfies
/// `PoolTransaction` and `EthPoolTransaction` for the Arbitrum noop pool.
///
/// Delegates to the inner [`EthPooledTransaction`] for all Ethereum tx methods.
/// Arbitrum-specific variants (Deposit, SubmitRetryable, etc.) go through the
/// same path — the pool is a noop, so these never execute.
#[derive(Debug, Clone)]
pub struct ArbPooledTransaction {
    inner: EthPooledTransaction<ArbTxEnvelope>,
}

impl ArbPooledTransaction {
    /// Creates a new `ArbPooledTransaction` from a recovered `ArbTxEnvelope`.
    pub fn new(tx: Recovered<ArbTxEnvelope>, encoded_length: usize) -> Self {
        Self { inner: EthPooledTransaction::new(tx, encoded_length) }
    }
}

impl From<ArbPooledTransaction> for Recovered<ArbTxEnvelope> {
    fn from(tx: ArbPooledTransaction) -> Self {
        tx.inner.transaction
    }
}

// ---------------------------------------------------------------------------
// InMemorySize
// ---------------------------------------------------------------------------

impl InMemorySize for ArbPooledTransaction {
    fn size(&self) -> usize {
        self.inner.size()
    }
}

// ---------------------------------------------------------------------------
// Typed2718
// ---------------------------------------------------------------------------

impl Typed2718 for ArbPooledTransaction {
    fn ty(&self) -> u8 {
        self.inner.ty()
    }
}

// ---------------------------------------------------------------------------
// alloy_consensus::Transaction
// ---------------------------------------------------------------------------

impl Transaction for ArbPooledTransaction {
    fn chain_id(&self) -> Option<alloy_primitives::ChainId> {
        self.inner.chain_id()
    }

    fn nonce(&self) -> u64 {
        self.inner.nonce()
    }

    fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }

    fn gas_price(&self) -> Option<u128> {
        self.inner.gas_price()
    }

    fn max_fee_per_gas(&self) -> u128 {
        self.inner.max_fee_per_gas()
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.inner.max_priority_fee_per_gas()
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.inner.max_fee_per_blob_gas()
    }

    fn priority_fee_or_price(&self) -> u128 {
        self.inner.priority_fee_or_price()
    }

    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        self.inner.effective_gas_price(base_fee)
    }

    fn is_dynamic_fee(&self) -> bool {
        self.inner.is_dynamic_fee()
    }

    fn kind(&self) -> TxKind {
        self.inner.kind()
    }

    fn is_create(&self) -> bool {
        self.inner.is_create()
    }

    fn value(&self) -> U256 {
        self.inner.value()
    }

    fn input(&self) -> &alloy_primitives::Bytes {
        self.inner.input()
    }

    fn access_list(&self) -> Option<&alloy_eips::eip2930::AccessList> {
        self.inner.access_list()
    }

    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        self.inner.blob_versioned_hashes()
    }

    fn authorization_list(&self) -> Option<&[alloy_eips::eip7702::SignedAuthorization]> {
        self.inner.authorization_list()
    }
}

// ---------------------------------------------------------------------------
// PoolTransaction
// ---------------------------------------------------------------------------

impl PoolTransaction for ArbPooledTransaction {
    type TryFromConsensusError = Infallible;
    type Consensus = ArbTxEnvelope;
    type Pooled = ArbTxEnvelope;

    fn clone_into_consensus(&self) -> Recovered<Self::Consensus> {
        self.inner.transaction.clone()
    }

    fn consensus_ref(&self) -> Recovered<&Self::Consensus> {
        Recovered::new_unchecked(self.inner.transaction.tx(), self.inner.transaction.signer())
    }

    fn into_consensus(self) -> Recovered<Self::Consensus> {
        self.inner.transaction
    }

    fn from_pooled(tx: Recovered<Self::Pooled>) -> Self {
        let encoded_length = tx.tx().length();
        Self { inner: EthPooledTransaction::new(tx, encoded_length) }
    }

    fn hash(&self) -> &TxHash {
        alloy_consensus::transaction::TxHashRef::tx_hash(self.inner.transaction.tx())
    }

    fn sender(&self) -> Address {
        self.inner.transaction.signer()
    }

    fn sender_ref(&self) -> &Address {
        self.inner.transaction.signer_ref()
    }

    fn cost(&self) -> &U256 {
        &self.inner.cost
    }

    fn encoded_length(&self) -> usize {
        self.inner.encoded_length
    }
}

// ---------------------------------------------------------------------------
// EthPoolTransaction
// ---------------------------------------------------------------------------

impl EthPoolTransaction for ArbPooledTransaction {
    fn take_blob(&mut self) -> EthBlobTransactionSidecar {
        EthBlobTransactionSidecar::None
    }

    fn try_into_pooled_eip4844(
        self,
        _sidecar: alloc::sync::Arc<BlobTransactionSidecarVariant>,
    ) -> Option<Recovered<Self::Pooled>> {
        None
    }

    fn try_from_eip4844(
        _tx: Recovered<Self::Consensus>,
        _sidecar: BlobTransactionSidecarVariant,
    ) -> Option<Self> {
        None
    }

    fn validate_blob(
        &self,
        _blob: &BlobTransactionSidecarVariant,
        _settings: &KzgSettings,
    ) -> Result<(), BlobTransactionValidationError> {
        Err(BlobTransactionValidationError::InvalidProof)
    }
}
