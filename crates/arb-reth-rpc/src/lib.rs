//! Arbitrum `eth_*` RPC layer.
//!
//! Provides [`ArbReceiptConverter`] / [`ArbRpcConverter`]: convert `ArbReceiptEnvelope<Log>`
//! primitives to [`ArbTransactionReceipt`] RPC responses, surfacing `gas_used_for_l1`. Wired into
//! reth's canonical `RpcAddOns` eth-api builder (see `arb-reth-node::addons`), not a bespoke server.

use std::fmt::Debug;

use alloy_consensus::{Receipt, ReceiptWithBloom};
use alloy_primitives::Log;
use alloy_rpc_types_eth::Log as RpcLog;
use arbitrum_alloy_consensus::{ArbReceipt, ArbReceiptEnvelope, ArbTxEnvelope};
use arbitrum_alloy_network::Arbitrum;
use arbitrum_alloy_rpc_types::ArbTransactionReceipt;
use arb_reth_evm::ArbEvmConfig;
use reth_node_api::NodePrimitives;
use reth_primitives_traits::SealedBlock;
use reth_rpc_convert::{RpcConverter, transaction::{ConvertReceiptInput, ReceiptConverter}};
use reth_rpc_eth_types::{EthApiError, receipt::build_receipt};

/// Converts `ArbReceiptEnvelope<Log>` primitives into [`ArbTransactionReceipt`] RPC responses.
///
/// Analogous to op-reth's `OpReceiptConverter`. `gas_used_for_l1` is stored on the receipt
/// by the block executor, so no L1-fee hardfork math is needed here.
#[derive(Debug, Clone)]
pub struct ArbReceiptConverter<Provider> {
    // reth's RPC builder constructs the converter with a provider, but Arbitrum stores
    // gas_used_for_l1 on the receipt at execution time, so it is never read here.
    #[allow(dead_code)]
    provider: Provider,
}

impl<Provider> ArbReceiptConverter<Provider> {
    /// Creates a new [`ArbReceiptConverter`].
    pub const fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

impl<Provider, N> ReceiptConverter<N> for ArbReceiptConverter<Provider>
where
    N: NodePrimitives<
        Receipt = ArbReceiptEnvelope<Log>,
        SignedTx = ArbTxEnvelope,
    >,
    Provider: Debug + Clone + 'static,
{
    type RpcReceipt = ArbTransactionReceipt;
    type Error = EthApiError;

    fn convert_receipts(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        inputs.into_iter().map(build_arb_receipt).collect()
    }

    fn convert_receipts_with_block(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
        _block: &SealedBlock<N::Block>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        self.convert_receipts(inputs)
    }
}

/// Maps a consensus `ArbReceiptEnvelope<Log>` to an RPC `ArbReceiptEnvelope<RpcLog>`,
/// returning `(gas_used_for_l1, rpc_envelope)`.
fn map_arb_receipt_envelope(
    envelope: ArbReceiptEnvelope<Log>,
    next_log_index: usize,
    meta: reth_primitives_traits::TransactionMeta,
) -> (u64, ArbReceiptEnvelope<RpcLog>) {
    /// Maps `ReceiptWithBloom<ArbReceipt<Log>>` to `ReceiptWithBloom<ArbReceipt<RpcLog>>`
    /// while capturing `gas_used_for_l1`.
    fn map_rwb(
        rwb: ReceiptWithBloom<ArbReceipt<Log>>,
        next_log_index: usize,
        meta: reth_primitives_traits::TransactionMeta,
    ) -> (u64, ReceiptWithBloom<ArbReceipt<RpcLog>>) {
        let logs_bloom = rwb.logs_bloom;
        let ArbReceipt { inner: receipt, gas_used_for_l1 } = rwb.receipt;
        let Receipt { status, cumulative_gas_used, logs } = receipt;

        let mut idx = next_log_index;
        let rpc_logs: Vec<RpcLog> = logs
            .into_iter()
            .map(|log| {
                let log_index = idx;
                idx += 1;
                RpcLog {
                    inner: log,
                    block_hash: Some(meta.block_hash),
                    block_number: Some(meta.block_number),
                    block_timestamp: Some(meta.timestamp),
                    transaction_hash: Some(meta.tx_hash),
                    transaction_index: Some(meta.index),
                    log_index: Some(log_index as u64),
                    removed: false,
                }
            })
            .collect();

        let arb_rpc = ArbReceipt {
            inner: Receipt { status, cumulative_gas_used, logs: rpc_logs },
            gas_used_for_l1,
        };
        (gas_used_for_l1, ReceiptWithBloom { receipt: arb_rpc, logs_bloom })
    }

    match envelope {
        ArbReceiptEnvelope::Legacy(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Legacy(r))
        }
        ArbReceiptEnvelope::Eip2930(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip2930(r))
        }
        ArbReceiptEnvelope::Eip1559(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip1559(r))
        }
        ArbReceiptEnvelope::Eip4844(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip4844(r))
        }
        ArbReceiptEnvelope::Eip7702(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip7702(r))
        }
        ArbReceiptEnvelope::Deposit(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Deposit(r))
        }
        ArbReceiptEnvelope::Unsigned(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Unsigned(r))
        }
        ArbReceiptEnvelope::Contract(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Contract(r))
        }
        ArbReceiptEnvelope::Retry(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Retry(r))
        }
        ArbReceiptEnvelope::SubmitRetryable(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::SubmitRetryable(r))
        }
        ArbReceiptEnvelope::Internal(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Internal(r))
        }
    }
}

/// Builds a single [`ArbTransactionReceipt`] from a [`ConvertReceiptInput`].
fn build_arb_receipt<N>(
    input: ConvertReceiptInput<'_, N>,
) -> Result<ArbTransactionReceipt, EthApiError>
where
    N: NodePrimitives<Receipt = ArbReceiptEnvelope<Log>>,
{
    // Use a Cell to capture gas_used_for_l1 from within the mapping closure.
    let gas_cell = std::cell::Cell::new(0u64);

    let core = build_receipt::<N, _>(input, None, |envelope, next_log_index, meta| {
        let (g, rpc_envelope) = map_arb_receipt_envelope(envelope, next_log_index, meta);
        gas_cell.set(g);
        rpc_envelope
    });

    Ok(ArbTransactionReceipt {
        inner: core,
        gas_used_for_l1: gas_cell.get(),
        // l1_block_number is not available at receipt-conversion time without reading
        // block extra_data; it gets populated from block metadata elsewhere.
        l1_block_number: None,
        timeboosted: None,
    })
}

/// Convenience type alias for the Arb [`RpcConverter`].
pub type ArbRpcConverter<Provider> = RpcConverter<
    Arbitrum,
    ArbEvmConfig,
    ArbReceiptConverter<Provider>,
>;
