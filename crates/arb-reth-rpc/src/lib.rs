//! Arbitrum `eth_*` RPC layer.
//!
//! Provides [`ArbReceiptConverter`] / [`ArbRpcConverter`]: convert `ArbReceiptEnvelope<Log>`
//! primitives to [`ArbTransactionReceipt`] RPC responses, surfacing `gas_used_for_l1`. Wired into
//! reth's canonical `RpcAddOns` eth-api builder (see `arb-reth-node::addons`), not a bespoke server.

use std::fmt::Debug;

use alloy_consensus::{Header, Receipt, ReceiptWithBloom};
use alloy_primitives::{B256, Log};
use alloy_rpc_types_eth::Log as RpcLog;
use arbitrum_alloy_consensus::header::ArbHeaderInfo;
use arbitrum_alloy_consensus::{ArbReceipt, ArbReceiptEnvelope, ArbTxEnvelope};
use arbitrum_alloy_network::Arbitrum;
use arbitrum_alloy_rpc_types::ArbTransactionReceipt;
use arb_reth_evm::ArbEvmConfig;
use reth_node_api::NodePrimitives;
use reth_primitives_traits::SealedBlock;
use reth_rpc_convert::{RpcConverter, transaction::{ConvertReceiptInput, ReceiptConverter}};
use reth_rpc_eth_types::{EthApiError, receipt::build_receipt};
use reth_storage_api::HeaderProvider;

/// Converts `ArbReceiptEnvelope<Log>` primitives into [`ArbTransactionReceipt`] RPC responses.
///
/// Analogous to op-reth's `OpReceiptConverter`. `gas_used_for_l1` is stored on the receipt
/// by the block executor, so no L1-fee hardfork math is needed here.
#[derive(Debug, Clone)]
pub struct ArbReceiptConverter<Provider> {
    // Read for `l1BlockNumber`: gas_used_for_l1 is on the receipt, but the L1 block number is only
    // in the header, so the block has to be looked up.
    provider: Provider,
}

impl<Provider> ArbReceiptConverter<Provider> {
    /// Creates a new [`ArbReceiptConverter`].
    pub const fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

impl<Provider: HeaderProvider<Header = Header>> ArbReceiptConverter<Provider> {
    /// Decode the L1 block number and receipt-pricing flag from the persisted header.
    ///
    /// It is not on the receipt: Nitro encodes it in the header (`extra_data` + `mix_hash`), so
    /// read the header back. `None` for a block we cannot find or one that is not an Arbitrum
    /// block, matching how the rest of the converter degrades.
    fn header_info(&self, block_hash: B256) -> Result<Option<ArbHeaderInfo>, EthApiError> {
        let Some(header) = self.provider.header(block_hash)? else {
            return Ok(None);
        };
        Ok(ArbHeaderInfo::decode_header(&header)
            .ok()
            .filter(ArbHeaderInfo::is_arbitrum))
    }
}

impl<Provider, N> ReceiptConverter<N> for ArbReceiptConverter<Provider>
where
    N: NodePrimitives<
        Receipt = ArbReceiptEnvelope<Log>,
        SignedTx = ArbTxEnvelope,
    >,
    Provider: HeaderProvider<Header = Header> + Debug + Clone + 'static,
{
    type RpcReceipt = ArbTransactionReceipt;
    type Error = EthApiError;

    fn convert_receipts(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        // Every receipt in one call comes from the same block in practice (eth_getBlockReceipts),
        // so memoize the header lookup on the block hash rather than repeating it per receipt.
        let mut memo: Option<(B256, Option<(u64, bool)>)> = None;
        inputs
            .into_iter()
            .map(|input| {
                let block_hash = input.meta.block_hash;
                let info = match memo {
                    Some((hash, info)) if hash == block_hash => info,
                    _ => {
                        let info = self
                            .header_info(block_hash)?
                            .map(|info| (info.l1_block_number, info.collect_tips));
                        memo = Some((block_hash, info));
                        info
                    }
                };
                build_arb_receipt(input, info.map(|i| i.0), info.is_some_and(|i| i.1))
            })
            .collect()
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
    l1_block_number: Option<u64>,
    collect_tips: bool,
) -> Result<ArbTransactionReceipt, EthApiError>
where
    N: NodePrimitives<Receipt = ArbReceiptEnvelope<Log>>,
{
    // Use a Cell to capture gas_used_for_l1 from within the mapping closure.
    let gas_cell = std::cell::Cell::new(0u64);

    // Nitro receipt.go DeriveFields uses the serialized block CollectTips bit.
    // build_receipt already uses the envelope's effective price (including zero
    // for internal/deposit txs); override it with base fee only when tips are off.
    let base_fee = input.meta.base_fee;

    let mut core = build_receipt::<N, _>(input, None, |envelope, next_log_index, meta| {
        let (g, rpc_envelope) = map_arb_receipt_envelope(envelope, next_log_index, meta);
        gas_cell.set(g);
        rpc_envelope
    });
    core.effective_gas_price = receipt_gas_price(base_fee, core.effective_gas_price, collect_tips);

    Ok(ArbTransactionReceipt {
        inner: core,
        gas_used_for_l1: gas_cell.get(),
        l1_block_number,
        timeboosted: None,
    })
}

/// Convenience type alias for the Arb [`RpcConverter`].
pub type ArbRpcConverter<Provider> = RpcConverter<
    Arbitrum,
    ArbEvmConfig,
    ArbReceiptConverter<Provider>,
>;

// Keep the no-tip branch identical to the existing RPC behavior.
fn receipt_gas_price(base_fee: Option<u64>, envelope_price: u128, collect_tips: bool) -> u128 {
    if collect_tips {
        envelope_price
    } else {
        base_fee.map(u128::from).unwrap_or(envelope_price)
    }
}

#[cfg(test)]
mod tests {
    use super::receipt_gas_price;

    #[test]
    fn receipt_prices_follow_nitro_collect_tips_bit() {
        assert_eq!(
            receipt_gas_price(Some(20_000_000), 21_100_220, true),
            21_100_220
        );
        assert_eq!(
            receipt_gas_price(Some(20_000_000), 21_100_220, false),
            20_000_000
        );
        // Internal/deposit envelopes have a zero effective price; only collecting
        // blocks preserve it. Non-collecting blocks continue reporting base fee.
        assert_eq!(receipt_gas_price(Some(20_000_000), 0, true), 0);
        assert_eq!(receipt_gas_price(Some(20_000_000), 0, false), 20_000_000);
    }
}
