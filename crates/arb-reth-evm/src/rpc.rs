//! RPC compatibility impls for `arb-reth-evm` types.
//!
//! Gated behind the `rpc` feature. Provides:
//! - `TryIntoTxEnv<ArbTx, ArbSpecId, BlockEnv> for ArbTransactionRequest` — satisfies the
//!   `TxEnvConverter<ArbTransactionRequest, ArbEvmConfig>` blanket so `()` works as the
//!   `TxEnv` parameter in `RpcConverter<Arbitrum, ArbEvmConfig, ArbReceiptConverter<_>>`.
//! - `BuildPendingEnv<Header> for ArbNextBlockEnvAttributes` — satisfies the
//!   `PendingEnvBuilder<ArbEvmConfig>` blanket for `()` so `EthApiBuilder::build()` compiles.

use alloy_evm::{
    rpc::{EthTxEnvError, TryIntoTxEnv},
};
use arb_alloy_rpc_types::ArbTransactionRequest;
use arb_revm::ArbTransaction;

use crate::{ArbNextBlockEnvAttributes, ArbTx};

use arb_revm::ArbSpecId;
use revm::context::BlockEnv;

// ---------------------------------------------------------------------------
// TryIntoTxEnv impl
// ---------------------------------------------------------------------------

impl TryIntoTxEnv<ArbTx, ArbSpecId, BlockEnv> for ArbTransactionRequest {
    type Err = EthTxEnvError;

    fn try_into_tx_env(
        self,
        evm_env: &alloy_evm::EvmEnv<ArbSpecId, BlockEnv>,
    ) -> Result<ArbTx, EthTxEnvError> {
        // Build a standard revm TxEnv from the inner TransactionRequest.
        let tx_env: revm::context::TxEnv = self.inner.try_into_tx_env(evm_env)?;
        // Wrap in ArbTransaction (no retry_meta for RPC sim; encoded_2718 not needed).
        Ok(ArbTx(ArbTransaction::new(tx_env)))
    }
}

// ---------------------------------------------------------------------------
// BuildPendingEnv impl
// ---------------------------------------------------------------------------

use reth_rpc_eth_api::helpers::pending_block::BuildPendingEnv;
use alloy_consensus::BlockHeader as AlloyBlockHeader;
use reth_primitives_traits::SealedHeader;
use alloy_rpc_types_eth::BlockOverrides;
use alloy_primitives::B256;

impl<H: AlloyBlockHeader> BuildPendingEnv<H> for ArbNextBlockEnvAttributes {
    fn build_pending_env(
        parent: &SealedHeader<H>,
        _block_overrides: Option<&BlockOverrides>,
    ) -> Self {
        // For pending blocks in eth_call / eth_estimateGas, we fill in sensible defaults.
        // l1_block_number and arbos_format_version are not available from the parent header —
        // callers that need precise values should use a custom pending-env builder.
        Self {
            timestamp: parent.timestamp().saturating_add(1),
            suggested_fee_recipient: parent.beneficiary(),
            prev_randao: B256::ZERO,
            gas_limit: parent.gas_limit(),
            l1_block_number: 0,
            l1_base_fee_wei: alloy_primitives::U256::ZERO,
            arbos_format_version: 0,
            delayed_messages_read: parent.nonce().map(|n| u64::from_be_bytes(n.0)).unwrap_or(0),
            extra_data: alloy_primitives::Bytes::default(),
            withdrawals: None,
        }
    }
}
