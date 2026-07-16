//! Native payload-builder integration tests over testnode replay fixtures.
#![allow(missing_docs)]

mod tests {
    use arb_reth_engine::{ArbEngineDriver, ArbEngineTuning};
    use arb_reth_evm::ArbEvmConfig;

    use std::sync::Arc;
    use std::vec::Vec;

    use alloy_consensus::Header;
    use alloy_primitives::{U256, address};
    use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;

    use reth_primitives_traits::SealedHeader;
    use reth_provider::HeaderProvider;
    use reth_provider::providers::BlockchainProvider;
    use reth_provider::test_utils::create_test_provider_factory_with_node_types;
    use reth_tasks::Runtime;

    use arb_reth_node::ArbNode;

    /// Concrete test factory type (temp MDBX over `ArbNode` types).
    type TestDb = Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>;
    type TestNodeTypes = reth_node_api::NodeTypesWithDBAdapter<ArbNode, TestDb>;
    type TestFactory = reth_provider::ProviderFactory<TestNodeTypes>;

    /// The testnode ArbOS chain spec (ArbOS v40, chain 412346) shared by both fixtures.
    fn testnode_spec() -> Arc<reth_chainspec::ChainSpec> {
        use arb_reth_node::arb_chain_spec;
        use arb_revm::arbos_init::ArbosInitConfig;
        const CHAIN_CONFIG: &[u8] =
            include_bytes!("../tests/fixtures/testnode_l2_chain_config.json");
        let init = ArbosInitConfig {
            initial_arbos_version: 40,
            initial_chain_owner: address!("5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927"),
            chain_id: U256::from(412346u64),
            genesis_block_number: 0,
            initial_l1_base_fee: U256::from(167u64),
            serialized_chain_config: CHAIN_CONFIG.to_vec(),
            debug_precompiles: true,
        };
        Arc::new(arb_chain_spec(&init).expect("build ArbOS chain spec"))
    }

    /// Gate (plain state): native payload construction over a v1/plain-state base.
    #[tokio::test(flavor = "multi_thread")]
    async fn engine_tree_tier1_replay() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(testnode_spec());
        reth_db_common::init::init_genesis(&factory).expect("init ArbOS genesis block 0");
        drive_replay_native(factory, 412346).await;
    }

    /// Gate (hashed-only / storage v2): the mainnet-shaped base where hashed-state tables are
    /// canonical (no `PlainAccountState`), exactly like the imported Arb One snapshot. The
    /// plain-state gate above does not exercise the hashed read path; this proves the native
    /// payload builder and engine-owned sparse trie task read the hashed tables correctly.
    #[tokio::test(flavor = "multi_thread")]
    async fn engine_tree_tier1_replay_v2_hashed() {
        use reth_db_api::models::StorageSettings;
        use reth_provider::{MetadataWriter, StorageSettingsCache};

        let factory = create_test_provider_factory_with_node_types::<ArbNode>(testnode_spec());
        // Emit a storage-v2 DB (hashed-state canonical, `PackedKeyAdapter`), mirroring the importer.
        factory.set_storage_settings_cache(StorageSettings::v2());
        {
            let provider_rw = factory.provider_rw().expect("provider_rw");
            provider_rw
                .write_storage_settings(StorageSettings::v2())
                .expect("write storage settings");
            provider_rw.commit().expect("commit storage settings");
        }
        reth_db_common::init::init_genesis_with_settings_and_validate(
            &factory,
            StorageSettings::v2(),
            true,
        )
        .expect("init ArbOS genesis (v2 hashed-canonical)");
        drive_replay_native(factory, 412346).await;
    }

    async fn drive_replay_native(factory: TestFactory, chain_id: u64) {
        const TARGET: u64 = 17;
        const FEED: &str = include_str!("../tests/fixtures/testnode_feed_seq0_17.ndjson");
        const BLOCKS: &str = include_str!("../tests/fixtures/testnode_blocks_0_17.json");

        let expected: Vec<serde_json::Value> = serde_json::from_str(BLOCKS).unwrap();
        let genesis_tip: SealedHeader<Header> = {
            let provider = factory.provider().expect("provider");
            let header = provider
                .sealed_header(0)
                .expect("read genesis")
                .expect("genesis header");
            drop(provider);
            header
        };
        let provider = BlockchainProvider::new(factory.clone()).expect("BlockchainProvider::new");
        let canonical = provider.canonical_in_memory_state();
        let mut driver = ArbEngineDriver::<TestNodeTypes>::spawn(
            factory,
            provider,
            ArbEvmConfig::new(chain_id),
            chain_id,
            genesis_tip,
            0,
            canonical,
            Runtime::test(),
            ArbEngineTuning::reth_defaults(),
            None,
        )
        .expect("spawn native payload driver");

        let messages = FEED
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str::<BroadcastFeedMessage>(line).expect("feed message"))
            .filter(|message| message.sequence_number > 0 && message.sequence_number <= TARGET)
            .collect::<Vec<_>>();
        let message_count = messages.len();
        let mut canonicalized = Vec::with_capacity(message_count);
        for (index, message) in messages.into_iter().enumerate() {
            let number = message.sequence_number;
            let defer_tail = index + 1 < message_count;
            let hash = driver
                .advance_with_applied_overlap(&message, defer_tail, |sequence_number, _| {
                    canonicalized.push(sequence_number);
                })
                .await
                .expect("native overlap advance");
            let header = driver.tip().header();
            let expected_block = &expected[number as usize];
            assert_eq!(
                format!("{hash:#x}"),
                expected_block["hash"].as_str().expect("expected hash"),
                "block {number} hash"
            );
            assert_eq!(
                format!("{:#x}", header.state_root),
                expected_block["stateRoot"]
                    .as_str()
                    .expect("expected state root"),
                "block {number} state root"
            );
        }
        assert_eq!(canonicalized, (1..=TARGET).collect::<Vec<_>>());

        driver.shutdown().await;
    }
}
