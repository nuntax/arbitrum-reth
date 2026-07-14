//! RPC compatibility impls for `arb-reth-evm` types (gated behind the `rpc` feature).
//!
//! - `TryIntoTxEnv<ArbTx, ArbSpecId, BlockEnv> for ArbTransactionRequest`: satisfies the
//!   `TxEnvConverter<ArbTransactionRequest, ArbEvmConfig>` blanket.
//! - `BuildPendingEnv<Header> for ArbNextBlockEnvAttributes`: satisfies the
//!   `PendingEnvBuilder<ArbEvmConfig>` blanket so `EthApiBuilder::build()` compiles.

use alloy_evm::rpc::{EthTxEnvError, TryIntoTxEnv};
use arb_revm::ArbTransaction;
use arbitrum_alloy_rpc_types::ArbTransactionRequest;

use crate::{ArbNextBlockEnvAttributes, ArbTx};

use arb_revm::ArbSpecId;
use revm::context::BlockEnv;

impl TryIntoTxEnv<ArbTx, ArbSpecId, BlockEnv> for ArbTransactionRequest {
    type Err = EthTxEnvError;

    fn try_into_tx_env(
        self,
        evm_env: &alloy_evm::EvmEnv<ArbSpecId, BlockEnv>,
    ) -> Result<ArbTx, EthTxEnvError> {
        let tx_env: revm::context::TxEnv = self.inner.try_into_tx_env(evm_env)?;
        // No retry_meta for RPC sim; encoded_2718 not needed.
        Ok(ArbTx(ArbTransaction::new(tx_env)))
    }
}

use alloy_consensus::BlockHeader as AlloyBlockHeader;
use alloy_primitives::B256;
use alloy_rpc_types_eth::BlockOverrides;
use reth_primitives_traits::SealedHeader;
use reth_rpc_eth_api::helpers::pending_block::BuildPendingEnv;

impl<H: AlloyBlockHeader> BuildPendingEnv<H> for ArbNextBlockEnvAttributes {
    fn build_pending_env(
        parent: &SealedHeader<H>,
        _block_overrides: Option<&BlockOverrides>,
    ) -> Self {
        // `l1_block_number` and `arbos_format_version` are not available from the parent header.
        // Callers that need precise values should use a custom pending-env builder.
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
            finish_timing_out: Default::default(),
        }
    }
}
