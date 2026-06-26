//! [`ArbTx`] — newtype wrapper around `arb_revm`'s [`ArbTransaction<TxEnv>`] that implements the
//! foreign alloy-evm transaction traits ([`IntoTxEnv`], [`FromRecoveredTx`], [`FromTxWithEncoded`]).
//!
//! Mirrors `alloy-op-evm`'s `OpTx`. The orphan rule forces a local newtype: `ArbTransaction<TxEnv>`
//! lives in `arb_revm` and the alloy-evm traits live in `alloy_evm`, so neither crate can carry the
//! impls. This wrapper is the seam that lets reth/alloy hand us a recovered Arbitrum consensus tx
//! and get back the exact revm tx env `arb_revm`'s handler executes.

use alloy_consensus::transaction::Transaction as AlloyTransaction;
use alloy_eips::eip2718::{Encodable2718, Typed2718};
use alloy_evm::{FromRecoveredTx, FromTxWithEncoded, IntoTxEnv, TransactionEnvMut};
use alloy_primitives::{Address, Bytes, B256, U256};
use arb_alloy_consensus::transactions::ArbTxEnvelope;
use arb_revm::ArbTransaction;
use arb_revm::transaction::RetryTxMeta;
use core::ops::{Deref, DerefMut};
use revm::context::TxEnv;
use revm::context::transaction::Transaction as RevmTransaction;
use revm::context_interface::{either::Either, transaction::AccessList};
use revm::primitives::TxKind;

/// Newtype wrapper around [`ArbTransaction<TxEnv>`] that allows implementing the foreign
/// alloy-evm transaction traits. This is the `Tx` type carried by [`crate::ArbEvm`] /
/// [`crate::ArbEvmFactory`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArbTx(pub ArbTransaction<TxEnv>);

impl From<ArbTx> for ArbTransaction<TxEnv> {
    fn from(tx: ArbTx) -> Self {
        tx.0
    }
}

impl From<ArbTransaction<TxEnv>> for ArbTx {
    fn from(tx: ArbTransaction<TxEnv>) -> Self {
        Self(tx)
    }
}

impl Deref for ArbTx {
    type Target = ArbTransaction<TxEnv>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArbTx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoTxEnv<Self> for ArbTx {
    fn into_tx_env(self) -> Self {
        self
    }
}

// `revm::context::Transaction` for the newtype: delegate to the inner `ArbTransaction<TxEnv>`, which
// already implements it (arb_revm `transaction.rs`). Required because `TransactionEnvMut` has
// `Transaction` as a supertrait, and reth's `ConfigureEvm` bounds the `EvmFactory::Tx` with
// `TransactionEnvMut`. (Deref does not carry trait impls, so this must be spelled out.)
impl RevmTransaction for ArbTx {
    type AccessListItem<'a> = <ArbTransaction<TxEnv> as RevmTransaction>::AccessListItem<'a>;
    type Authorization<'a> = <ArbTransaction<TxEnv> as RevmTransaction>::Authorization<'a>;

    fn tx_type(&self) -> u8 {
        self.0.tx_type()
    }
    fn caller(&self) -> Address {
        self.0.caller()
    }
    fn gas_limit(&self) -> u64 {
        self.0.gas_limit()
    }
    fn value(&self) -> U256 {
        self.0.value()
    }
    fn input(&self) -> &Bytes {
        self.0.input()
    }
    fn nonce(&self) -> u64 {
        self.0.nonce()
    }
    fn kind(&self) -> TxKind {
        self.0.kind()
    }
    fn chain_id(&self) -> Option<u64> {
        self.0.chain_id()
    }
    fn access_list(&self) -> Option<impl Iterator<Item = Self::AccessListItem<'_>>> {
        self.0.access_list()
    }
    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.0.max_priority_fee_per_gas()
    }
    fn max_fee_per_gas(&self) -> u128 {
        self.0.max_fee_per_gas()
    }
    fn gas_price(&self) -> u128 {
        self.0.gas_price()
    }
    fn blob_versioned_hashes(&self) -> &[B256] {
        self.0.blob_versioned_hashes()
    }
    fn max_fee_per_blob_gas(&self) -> u128 {
        self.0.max_fee_per_blob_gas()
    }
    fn effective_gas_price(&self, base_fee: u128) -> u128 {
        self.0.effective_gas_price(base_fee)
    }
    fn authorization_list_len(&self) -> usize {
        self.0.authorization_list_len()
    }
    fn authorization_list(&self) -> impl Iterator<Item = Self::Authorization<'_>> {
        self.0.authorization_list()
    }
}

// `TransactionEnvMut` mutators delegate to the inner revm `TxEnv` (`self.0.base`). This satisfies the
// `ConfigureEvm::BlockExecutorFactory::EvmFactory::Tx: TransactionEnvMut` bound for `ArbTx`.
impl TransactionEnvMut for ArbTx {
    fn set_gas_limit(&mut self, gas_limit: u64) {
        self.0.base.set_gas_limit(gas_limit);
    }
    fn set_nonce(&mut self, nonce: u64) {
        self.0.base.set_nonce(nonce);
    }
    fn set_access_list(&mut self, access_list: AccessList) {
        self.0.base.set_access_list(access_list);
    }
}

/// Builds the revm tx env for an [`ArbTxEnvelope`] given an already-recovered `caller`.
///
/// This is the recovered-sender analogue of `arb_revm`'s `TryFrom<&ArbTxEnvelope>` adapter: it
/// reuses the same field lowering but takes the caller from reth (which has already recovered or
/// otherwise determined the sender), instead of re-running secp256k1 recovery. That also makes it
/// total — Arbitrum's unsigned/system tx variants (Deposit, Unsigned, Internal, …) have no
/// recoverable signature but do carry a `from`, which reth supplies here.
fn arb_tx_from_envelope(tx: &ArbTxEnvelope, caller: Address, encoded: Bytes) -> ArbTx {
    let access_list = tx
        .access_list()
        .map(|items| AccessList(items.0.clone()))
        .unwrap_or_default();
    let blob_hashes = tx
        .blob_versioned_hashes()
        .map_or_else(Vec::new, |hashes| hashes.to_vec());
    let authorization_list = tx
        .authorization_list()
        .map(|auths| auths.iter().cloned().map(Either::Left).collect())
        .unwrap_or_default();

    let base = TxEnv {
        tx_type: tx.ty(),
        caller,
        gas_limit: tx.gas_limit(),
        gas_price: tx.gas_price().unwrap_or(tx.max_fee_per_gas()),
        kind: tx.kind(),
        value: tx.value(),
        data: tx.input().clone(),
        nonce: tx.nonce(),
        chain_id: tx.chain_id(),
        access_list,
        gas_priority_fee: tx.max_priority_fee_per_gas(),
        blob_hashes,
        max_fee_per_blob_gas: tx.max_fee_per_blob_gas().unwrap_or(0),
        authorization_list,
    };

    let retry_meta = match tx {
        ArbTxEnvelope::Retry(retry) => Some(RetryTxMeta {
            ticket_id: retry.ticket_id,
            refund_to: retry.refund_to,
            max_refund: retry.max_refund,
            submission_fee_refund: retry.submission_fee_refund,
        }),
        _ => None,
    };

    ArbTx(ArbTransaction {
        base,
        retry_meta,
        encoded_2718: Some(encoded),
    })
}

impl FromRecoveredTx<ArbTxEnvelope> for ArbTx {
    fn from_recovered_tx(tx: &ArbTxEnvelope, sender: Address) -> Self {
        let encoded = Bytes::from(tx.encoded_2718());
        arb_tx_from_envelope(tx, sender, encoded)
    }
}

impl FromTxWithEncoded<ArbTxEnvelope> for ArbTx {
    fn from_encoded_tx(tx: &ArbTxEnvelope, sender: Address, encoded: Bytes) -> Self {
        arb_tx_from_envelope(tx, sender, encoded)
    }
}
