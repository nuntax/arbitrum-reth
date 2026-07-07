//! Engine-tree Tier-1 integration test, relocated from the former `arb-reth-node::engine_spike`
//! module when the driver was split into the `arb-reth-engine` crate. It drives reth's
//! `EngineApiTreeHandler` for `ArbNode` over testnode-replay fixtures, so it lives with the node
//! crate (which owns `ArbNode` + genesis + the fixtures).
#![allow(missing_docs)]

mod tests {
    use arb_reth_evm::ArbEvmConfig;
    use arb_reth_engine::ArbPayloadValidator;

    use std::sync::Arc;
    use std::vec;
    use std::vec::Vec;

    use alloy_consensus::Header;
    use alloy_primitives::{address, B256, U256};
    use arbitrum_alloy_consensus::reth::ArbPrimitives;
    use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;

    use reth_engine_primitives::{
        BeaconEngineMessage, NoopInvalidBlockHook, TreeConfig,
    };
    use reth_engine_tree::engine::{EngineApiKind, EngineApiRequest, EngineApiEvent, FromEngine};
    use reth_engine_tree::tree::{BasicEngineValidator, EngineApiTreeHandler};
    use reth_payload_builder::PayloadBuilderHandle;
    use reth_primitives_traits::SealedHeader;
    use reth_provider::providers::BlockchainProvider;
    use reth_provider::test_utils::create_test_provider_factory_with_node_types;
    use reth_provider::{HeaderProvider, StateProviderFactory};
    use reth_prune::Pruner;
    use reth_tasks::Runtime;
    use reth_trie_db::ChangesetCache;

    // Single source of truth: production block-producer + head-observer live in `crate::engine`.
    use arb_reth_engine::{produce, wait_for_head};

    use arb_reth_node::ArbNode;

    /// Concrete test factory type (temp MDBX over `ArbNode` types).
    type TestDb = Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>;
    type TestNodeTypes = reth_node_api::NodeTypesWithDBAdapter<ArbNode, TestDb>;
    type TestFactory = reth_provider::ProviderFactory<TestNodeTypes>;

    /// The testnode ArbOS chain spec (ArbOS v40, chain 412346) shared by both gates.
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

    /// Gate (plain state): the tree over a v1/plain-state `init_genesis` base.
    #[tokio::test(flavor = "multi_thread")]
    async fn engine_tree_tier1_replay() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(testnode_spec());
        reth_db_common::init::init_genesis(&factory).expect("init ArbOS genesis block 0");
        drive_replay(factory, 412346).await;
    }

    /// Gate (hashed-only / storage v2): the mainnet-shaped base where hashed-state tables are
    /// canonical (no `PlainAccountState`), exactly like the imported Arb One snapshot. The
    /// plain-state gate above does not exercise the hashed read path; this proves the engine
    /// tree's overlay + deferred-trie task + `state_by_block_hash` all read the hashed tables
    /// correctly under the tree. This is the de-risk for engine-tree adoption on mainnet.
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
        drive_replay(factory, 412346).await;
    }

    /// Shared drive body: stand up reth's `EngineApiTreeHandler` over `factory` and produce the
    /// testnode-replay blocks via `InsertExecutedBlock` + `ForkchoiceUpdated` (no re-execution),
    /// asserting each canonical block's `state_root` and hash equal the testnode's captured values.
    async fn drive_replay(factory: TestFactory, chain_id: u64) {
        /// How many blocks to drive through the tree.
        const TARGET: u64 = 17;
        const FEED: &str = include_str!("../tests/fixtures/testnode_feed_seq0_17.ndjson");
        const BLOCKS: &str = include_str!("../tests/fixtures/testnode_blocks_0_17.json");

        let expected: Vec<serde_json::Value> = serde_json::from_str(BLOCKS).unwrap();

        // Genesis (block 0) must match; guards the parent chain for block 1.
        let genesis_tip: SealedHeader<Header> = {
            let p = factory.provider().unwrap();
            let h = p.sealed_header(0).unwrap().expect("genesis header");
            drop(p);
            h
        };
        assert_eq!(
            format!("{:#x}", genesis_tip.hash()),
            expected[0]["hash"].as_str().unwrap(),
            "genesis (block 0) hash must match the testnode"
        );

        // ---- stand up the BlockchainProvider (serves overlay + canonical_in_memory_state) ----
        let provider = BlockchainProvider::new(factory.clone()).expect("BlockchainProvider::new");
        let canonical_in_memory = provider.canonical_in_memory_state();

        // ---- persistence service (real MDBX writer, noop pruner) ----
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(reth_exex_types::FinishedExExHeight::NoExExs);
        let pruner = Pruner::new_with_factory(
            factory.clone(),
            vec![],
            5,
            0,
            None,
            finished_exex_height_rx,
        );
        let (sync_metrics_tx, _sync_metrics_rx) =
            tokio::sync::mpsc::unbounded_channel::<reth_stages_api::MetricEvent>();
        let persistence =
            reth_engine_tree::persistence::PersistenceHandle::<ArbPrimitives>::spawn_service::<
                reth_node_api::NodeTypesWithDBAdapter<
                    ArbNode,
                    Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>,
                >,
            >(factory.clone(), pruner, sync_metrics_tx);

        // ---- engine-tree wiring (all reth components) ----
        let evm_config = ArbEvmConfig::new(chain_id);
        let consensus: Arc<dyn reth_consensus::FullConsensus<ArbPrimitives>> =
            Arc::new(reth_consensus::noop::NoopConsensus::default());
        let runtime = Runtime::test();
        let changeset_cache = ChangesetCache::new();
        let tree_config = TreeConfig::default();

        let payload_validator = BasicEngineValidator::new(
            provider.clone(),
            consensus.clone(),
            evm_config.clone(),
            ArbPayloadValidator,
            tree_config.clone(),
            Box::new(NoopInvalidBlockHook::default()),
            changeset_cache.clone(),
            runtime.clone(),
        );

        let (to_payload_service, _payload_cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let payload_builder: PayloadBuilderHandle<arb_reth_engine::ArbPayloadTypes> =
            PayloadBuilderHandle::new(to_payload_service);

        let (to_tree, mut from_tree) = EngineApiTreeHandler::spawn_new(
            provider.clone(),
            consensus,
            payload_validator,
            persistence,
            payload_builder,
            canonical_in_memory,
            tree_config,
            EngineApiKind::Ethereum,
            evm_config.clone(),
            changeset_cache,
            runtime,
        );

        // Drain events on a background task so the tree channel never blocks; record every
        // canonicalized block (number -> hash) as observed.
        let (obs_tx, mut obs_rx) =
            tokio::sync::mpsc::unbounded_channel::<(u64, B256)>();
        tokio::spawn(async move {
            use reth_engine_primitives::ConsensusEngineEvent;
            while let Some(ev) = from_tree.recv().await {
                if let EngineApiEvent::BeaconConsensus(ce) = ev {
                    match ce {
                        ConsensusEngineEvent::CanonicalChainCommitted(header, _) => {
                            let _ = obs_tx.send((header.number, header.hash()));
                        }
                        ConsensusEngineEvent::CanonicalBlockAdded(block, _) => {
                            let rb = block.recovered_block();
                            let _ = obs_tx.send((rb.header().number, rb.hash()));
                        }
                        _ => {}
                    }
                }
            }
        });

        let msgs: Vec<BroadcastFeedMessage> = FEED
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("parse feed message"))
            .collect();

        let mut tip = genesis_tip;
        let mut mismatches: Vec<String> = Vec::new();
        let mut last_ok = 0u64;

        for m in &msgs {
            if m.sequence_number == 0 {
                continue; // Initialize message => genesis (already seeded)
            }
            let bn = m.sequence_number;
            if bn > TARGET {
                break;
            }

            // 1. Produce the block against the tree overlay at the current tip (legacy path:
            //    two independent `state_by_block_hash(parent)` providers).
            let (exec_sp, trie_sp) = match (
                provider.state_by_block_hash(tip.hash()),
                provider.state_by_block_hash(tip.hash()),
            ) {
                (Ok(a), Ok(b)) => (a, b),
                _ => {
                    mismatches.push(format!("block {bn} state_by_block_hash ERROR"));
                    break;
                }
            };
            let built = match produce(&evm_config, chain_id, &tip, m, exec_sp, trie_sp) {
                Ok(b) => b,
                Err(e) => {
                    mismatches.push(format!("block {bn} produce ERROR: {e:?}"));
                    break;
                }
            };
            let new_hash = built.recovered_block.hash();
            let new_header = built.recovered_block.header().clone();
            let got_root = format!("{:#x}", new_header.state_root);

            // 2. Feed it to the tree (no re-execution) ...
            to_tree
                .send(FromEngine::Request(EngineApiRequest::InsertExecutedBlock(built)))
                .expect("send InsertExecutedBlock");

            // 3. ... and drive canonicalization via ForkchoiceUpdated (head = new block).
            let (fcu_tx, fcu_rx) = tokio::sync::oneshot::channel();
            let fcu_state = alloy_rpc_types_engine::ForkchoiceState {
                head_block_hash: new_hash,
                safe_block_hash: new_hash,
                finalized_block_hash: B256::ZERO,
            };
            to_tree
                .send(FromEngine::Request(EngineApiRequest::Beacon(
                    BeaconEngineMessage::ForkchoiceUpdated {
                        state: fcu_state,
                        payload_attrs: None,
                        tx: fcu_tx,
                    },
                )))
                .expect("send ForkchoiceUpdated");
            let fcu_res = fcu_rx.await.expect("FCU response channel");
            let fcu_res = fcu_res.expect("FCU RethResult");
            let fcu_status = fcu_res.await; // OnForkChoiceUpdated: Future<Output=ForkChoiceUpdateResult>
            if let Err(e) = &fcu_status {
                mismatches.push(format!("block {bn} FCU error: {e:?}"));
                break;
            }

            // 4. Wait for the tree to actually canonicalize the block (bounded).
            let canonicalized =
                wait_for_head(&provider, &provider.canonical_in_memory_state(), &mut obs_rx, bn, new_hash)
                    .await;
            if !canonicalized {
                mismatches.push(format!(
                    "block {bn} was NOT canonicalized within timeout (head hash {new_hash:#x})"
                ));
                break;
            }

            // 5. Assert against the testnode.
            let exp = &expected[bn as usize];
            let exp_root = exp["stateRoot"].as_str().unwrap();
            let exp_hash = exp["hash"].as_str().unwrap();
            let got_hash = format!("{new_hash:#x}");
            let root_ok = got_root == exp_root;
            let hash_ok = got_hash == exp_hash;
            eprintln!(
                "block {bn:2}: root {} hash {}",
                if root_ok { "OK " } else { "BAD" },
                if hash_ok { "OK " } else { "BAD" },
            );
            if !root_ok {
                mismatches.push(format!("block {bn} stateRoot: got {got_root} want {exp_root}"));
            }
            if !hash_ok {
                mismatches.push(format!("block {bn} hash: got {got_hash} want {exp_hash}"));
            }
            if root_ok && hash_ok {
                last_ok = bn;
            }

            tip = SealedHeader::new(new_header, new_hash);
        }

        assert!(
            mismatches.is_empty(),
            "engine-tree Tier-1: matched blocks 1..={last_ok}; {} issue(s):\n  {}",
            mismatches.len(),
            mismatches.join("\n  ")
        );
        assert_eq!(
            last_ok, TARGET,
            "expected blocks 1..={TARGET} to all match through the engine tree (got 1..={last_ok})"
        );
    }
}
