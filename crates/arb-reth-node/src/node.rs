//! D.4 — standalone Arbitrum node launcher.
//!
//! A lightweight driver: given a reth `ProviderFactory` and a feed channel,
//! creates an [`ArbChainDriver`] and advances the chain one message at a time.
//!
//! This is the à-la-carte reth SDK composition: no `NodeBuilder`, no engine API.
//! See `lib.rs` §"Rationale" for why.

use alloy_consensus::Header;
use arb_alloy_consensus::reth::ArbPrimitives;
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use reth_primitives_traits::SealedHeader;
use reth_provider::providers::ProviderNodeTypes;
use reth_provider::ProviderFactory;

use crate::driver::ArbChainDriver;

/// Run the Arbitrum node: consume messages from the channel and advance the
/// chain. Blocks are persisted immediately (persistence threshold = 1).
///
/// The function blocks until the channel is closed and all messages are processed.
pub async fn run<N: ProviderNodeTypes<Primitives = ArbPrimitives>>(
    factory: ProviderFactory<N>,
    genesis_tip: SealedHeader<Header>,
    mut messages: tokio::sync::mpsc::Receiver<(BroadcastFeedMessage, u8)>,
) -> eyre::Result<()> {
    let mut driver = ArbChainDriver::new(factory, crate::ARB_ONE_CHAIN_ID, genesis_tip, 1);

    while let Some((msg, version)) = messages.recv().await {
        driver.advance(&msg, version)?;
    }

    // Defensive flush (should be empty with threshold=1).
    driver.flush()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, U256};
    use reth_provider::{BlockNumReader, HeaderProvider, test_utils::create_test_provider_factory_with_node_types};
    use reth_storage_api::AccountReader;

    use crate::ArbNode;
    use crate::driver::seed_genesis;

    /// D.4 smoke test: a standalone node boots, processes 2 fixture deposits,
    /// and blocks 1 & 2 are durably persisted.
    ///
    /// **Acceptance criteria:** open a fresh factory, send 2 messages through the
    /// channel, await `run()`, then verify blocks 1 & 2 exist on disk with the
    /// correct cumulative deposit balance.
    #[tokio::test]
    async fn standalone_node_boots_and_produces_blocks() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(
            reth_chainspec::MAINNET.clone(),
        );
        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel::<(BroadcastFeedMessage, u8)>(4);
        tx.send((feed_msg.clone(), 0)).await.unwrap();
        tx.send((feed_msg.clone(), 0)).await.unwrap();
        drop(tx);

        run(factory.clone(), genesis_tip, rx)
            .await
            .expect("run must succeed");

        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), 2);

        let h1 = provider.header_by_number(1).unwrap().unwrap();
        let h2 = provider.header_by_number(2).unwrap().unwrap();
        assert_eq!(h1.number, 1);
        assert_eq!(h2.number, 2);
        assert_ne!(h1.state_root, h2.state_root, "state roots must differ");

        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let single_deposit = U256::from(111000000000000000u128);
        let expected_cumulative = single_deposit * U256::from(2);

        drop(provider);
        let state = factory.latest().unwrap();
        let acct = state
            .basic_account(&deposit_to)
            .unwrap()
            .expect("deposit recipient must exist");
        assert_eq!(
            acct.balance, expected_cumulative,
            "cumulative balance after 2 deposits"
        );
    }
}
