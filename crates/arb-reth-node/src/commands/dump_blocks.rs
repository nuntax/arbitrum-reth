//! `arb-reth dump-blocks`: quick MDBX reader: dump block headers + tx hashes + receipt status.
use std::path::PathBuf;
use std::sync::Arc;

use alloy_consensus::TxReceipt;
use alloy_eips::BlockHashOrNumber;
use crate::ArbNode;
use clap::Parser;
use reth_node_types::NodeTypesWithDBAdapter;
use reth_primitives_traits::SealedHeader;
use reth_provider::providers::{ProviderFactoryBuilder, ReadOnlyConfig};
use reth_provider::{HeaderProvider, ReceiptProvider, TransactionsProvider};

/// Dump block headers, tx hashes, and receipt status from a node datadir.
#[derive(Debug, Parser)]
#[command(name = "dump-blocks", about = "Dump block headers + tx hashes + receipt status")]
pub struct DumpBlocksArgs {
    /// Node datadir to read.
    #[arg(value_name = "datadir")]
    datadir: PathBuf,

    /// Block numbers to dump.
    #[arg(value_name = "block_number", required = true)]
    block_numbers: Vec<u64>,
}

pub fn run(args: DumpBlocksArgs) -> eyre::Result<()> {
    let datadir = args.datadir.as_path();
    let chain_spec: Arc<reth_chainspec::ChainSpec> = reth_chainspec::MAINNET.clone();
    let runtime = reth_tasks::Runtime::test();

    let factory = ProviderFactoryBuilder::<
        NodeTypesWithDBAdapter<ArbNode, reth_db::DatabaseEnv>,
    >::default()
    .open_read_only(chain_spec, ReadOnlyConfig::from_datadir(datadir), runtime)?;

    let provider = factory.provider()?;

    for bn in &args.block_numbers {
        let bn: u64 = *bn;
        let header = match provider.header_by_number(bn)? {
            Some(h) => h,
            None => { println!("{bn}: NOT FOUND"); continue; }
        };
        let sealed = SealedHeader::seal_slow(header);
        let hash = sealed.hash();

        let txs = provider.transactions_by_block(BlockHashOrNumber::Hash(hash))?;
        let receipts = provider.receipts_by_block(BlockHashOrNumber::Hash(hash))?;

        println!("=== BLOCK {bn} ===");
        println!("  hash:       {hash:?}");
        println!("  state_root: {:?}", sealed.header().state_root);
        println!("  gas_used:   {}", sealed.header().gas_used);

        if let Some(ref txs) = txs {
            let rec_count = receipts.as_ref().map(|r| r.len()).unwrap_or(0);
            println!("  tx_count:   {} (receipts: {})", txs.len(), rec_count);
            for (i, tx) in txs.iter().enumerate() {
                let status = receipts.as_ref()
                    .and_then(|r| r.get(i))
                    .map(|r| if r.status() { "OK" } else { "REVERTED" })
                    .unwrap_or("?");
                let gas = receipts.as_ref()
                    .and_then(|r| r.get(i))
                    .map(|r| r.cumulative_gas_used())
                    .unwrap_or(0);
                println!("  tx[{i}]: hash={:?} status={} gas={}", tx.tx_hash(), status, gas);
            }
        }
    }
    Ok(())
}
