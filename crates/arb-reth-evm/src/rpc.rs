//! RPC compatibility impls for `arb-reth-evm` types (gated behind the `rpc` feature).
//!
//! - `TryIntoTxEnv<ArbTx, ArbSpecId, ArbBlockEnv> for ArbTransactionRequest`: satisfies the
//!   `TxEnvConverter<ArbTransactionRequest, ArbEvmConfig>` blanket.
//! - `BuildPendingEnv<Header> for ArbNextBlockEnvAttributes`: satisfies the
//!   `PendingEnvBuilder<ArbEvmConfig>` blanket so `EthApiBuilder::build()` compiles.

use alloy_evm::rpc::{EthTxEnvError, TryIntoTxEnv};
use arb_revm::ArbTransaction;
use arbitrum_alloy_rpc_types::ArbTransactionRequest;

use crate::{ArbBlockEnv, ArbNextBlockEnvAttributes, ArbTx};

use arb_revm::ArbSpecId;

impl TryIntoTxEnv<ArbTx, ArbSpecId, ArbBlockEnv> for ArbTransactionRequest {
    type Err = EthTxEnvError;

    fn try_into_tx_env(
        self,
        evm_env: &alloy_evm::EvmEnv<ArbSpecId, ArbBlockEnv>,
    ) -> Result<ArbTx, EthTxEnvError> {
        let tx_env: revm::context::TxEnv = self.inner.try_into_tx_env(evm_env)?;
        // No retry_meta for RPC sim; encoded_2718 not needed.
        Ok(ArbTx(ArbTransaction::new(tx_env)))
    }
}

use alloy_consensus::{BlockHeader as AlloyBlockHeader, Header};
use alloy_primitives::B256;
use alloy_rpc_types_eth::BlockOverrides;
use arbitrum_alloy_consensus::header::ArbHeaderInfo;
use reth_primitives_traits::SealedHeader;
use reth_rpc_eth_api::helpers::pending_block::BuildPendingEnv;

impl BuildPendingEnv<Header> for ArbNextBlockEnvAttributes {
    fn build_pending_env(
        parent: &SealedHeader<Header>,
        _block_overrides: Option<&BlockOverrides>,
    ) -> Self {
        let arb_info = ArbHeaderInfo::decode_header(parent.header()).ok();
        Self {
            timestamp: parent.timestamp().saturating_add(1),
            suggested_fee_recipient: parent.beneficiary(),
            prev_randao: B256::ZERO,
            gas_limit: parent.gas_limit(),
            l1_block_number: arb_info.as_ref().map_or(0, |info| info.l1_block_number),
            // This value is not encoded in the L2 header. It is irrelevant to compute-only RPC
            // simulation because simulated calls do not carry poster-cost input bytes.
            l1_base_fee_wei: alloy_primitives::U256::ZERO,
            arbos_format_version: arb_info.map_or(0, |info| info.arbos_format_version),
            delayed_messages_read: parent.nonce().map(|n| u64::from_be_bytes(n.0)).unwrap_or(0),
            extra_data: alloy_primitives::Bytes::default(),
            withdrawals: None,
            finish_timing_out: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B64, U256};

    #[test]
    fn pending_env_inherits_arbitrum_context_from_parent() {
        let mut header = Header {
            timestamp: 1_700_000_000,
            beneficiary: alloy_primitives::address!("a4b000000000000000000073657175656e636572"),
            gas_limit: 1 << 50,
            nonce: B64::new(23_u64.to_be_bytes()),
            ..Header::default()
        };
        ArbHeaderInfo {
            send_root: B256::repeat_byte(0x11),
            send_count: 42,
            l1_block_number: 19_876_543,
            arbos_format_version: 61,
            collect_tips: false,
        }
        .update_header(&mut header);
        let parent = SealedHeader::seal_slow(header);

        let env = ArbNextBlockEnvAttributes::build_pending_env(&parent, None);

        assert_eq!(env.timestamp, 1_700_000_001);
        assert_eq!(env.suggested_fee_recipient, parent.beneficiary());
        assert_eq!(env.gas_limit, 1 << 50);
        assert_eq!(env.l1_block_number, 19_876_543);
        assert_eq!(env.arbos_format_version, 61);
        assert_eq!(env.delayed_messages_read, 23);
        assert_eq!(env.l1_base_fee_wei, U256::ZERO);
    }
}
