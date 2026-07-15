//! Integration test relocated from the former `arb-reth-node::rpc` module when the RPC
//! layer was split into the `arb-reth-rpc` crate. Exercises `ArbLauncher` + RPC end-to-end,
//! so it lives with the node crate (which owns the launcher/node types), not with the RPC crate.

    use std::net::{Ipv4Addr, SocketAddr};

    use alloy_primitives::{address, U256};
    use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;
    use jsonrpsee::core::client::ClientT as _;
    use reth_chainspec::MAINNET;
    use reth_node_builder::{LaunchNode, NodeBuilder, NodeConfig};
    use reth_tasks::Runtime;

    use arb_reth_node::{ArbLauncher, ArbNode};

    /// `eth_*` JSON-RPC is live after node launch. Boots `ArbLauncher` with RPC on an
    /// ephemeral port, feeds two deposits, and verifies `eth_getBlockByNumber` and
    /// `eth_getBalance`.
    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_serves_eth_queries() {
        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures");
        let json = std::fs::read_to_string(fixtures_dir.join("deposit_message_only.json"))
            .expect("read fixture");
        let feed_msg: BroadcastFeedMessage =
            serde_json::from_str(&json).expect("parse BroadcastFeedMessage");

        let task_executor = Runtime::test();
        // The driver dedups by sequence number, so the two messages must be sequential (a fresh
        // genesis DB has genesis_block 0, so the first digested message is index 1).
        let (tx, rx) = tokio::sync::mpsc::channel::<BroadcastFeedMessage>(4);
        let mut m1 = feed_msg.clone();
        m1.sequence_number = 1;
        let mut m2 = feed_msg.clone();
        m2.sequence_number = 2;
        tx.send(m1).await.unwrap();
        tx.send(m2).await.unwrap();
        drop(tx);

        let datadir = reth_db::test_utils::tempdir_path();
        let db = reth_db::test_utils::create_test_rw_db_with_datadir(&datadir);

        let maybe_path = reth_node_core::dirs::MaybePlatformPath::<
            reth_node_core::dirs::DataDirPath,
        >::from(datadir.clone());
        let config = NodeConfig::test().with_chain(MAINNET.clone()).with_datadir_args(
            reth_node_core::args::DatadirArgs {
                datadir: maybe_path.clone(),
                ..Default::default()
            },
        );
        let data_dir = maybe_path.unwrap_or_chain_default(MAINNET.chain(), config.datadir.clone());

        let node_builder_with_components =
            NodeBuilder::new(config).with_database(db).node(ArbNode);

        let rpc_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let launcher = ArbLauncher {
            ctx: reth_node_builder::LaunchContext::new(task_executor.clone(), data_dir),
            chain_id: arb_reth_node::ARB_ONE_CHAIN_ID,
            genesis_block: 0,
            tuning: arb_reth_node::ArbEngineTuning::reth_defaults(),
            prune_builder: None,
            messages: rx,
            feed_latency: None,
            rpc_addr: Some(rpc_addr),
        };

        let handle = launcher
            .launch_node(node_builder_with_components)
            .await
            .expect("launch must succeed");

        let rpc_handle = handle.rpc_handle.expect("RPC server should be running");
        let http_url = rpc_handle.http_url().expect("HTTP URL must be present");

        let client = jsonrpsee::http_client::HttpClientBuilder::default()
            .build(&http_url)
            .expect("build http client");

        // The engine tree canonicalizes + persists asynchronously, so wait (bounded) until the
        // node has produced both feed blocks before querying `latest`. This is a robustness wait,
        // not an assertion change: the balance/number assertions below are unchanged.
        {
            use reth_provider::BlockNumReader;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while handle.provider.best_block_number().unwrap_or(0) < 2 {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }

        let block: serde_json::Value = client
            .request("eth_getBlockByNumber", jsonrpsee::rpc_params!["latest", false])
            .await
            .expect("eth_getBlockByNumber should succeed");
        assert!(block.get("number").is_some(), "block must have a number field");

        let deposit_recipient = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let balance_hex: String = client
            .request(
                "eth_getBalance",
                jsonrpsee::rpc_params![
                    format!("{:?}", deposit_recipient),
                    "latest"
                ],
            )
            .await
            .expect("eth_getBalance should succeed");

        let balance = U256::from_str_radix(balance_hex.trim_start_matches("0x"), 16)
            .expect("parse balance hex");
        assert!(balance > U256::ZERO, "deposit recipient must have non-zero balance");

        // --- Full RPC module fleet (Path B): before this change only `eth` was registered, so
        // net/web3/txpool/trace/debug returned method-not-found. Each call below fails if its
        // module is absent, proving the fleet is live on our engine-free, hashed-state node.

        // net module: chain id as a decimal string. Canonical `RpcAddOns` derives this from the
        // booted chain spec (MAINNET in this fixture); in production the spec is the Arbitrum spec,
        // so this reports 42161. Asserting against the fixture's id keeps the test spec-agnostic.
        let net_version: String = client
            .request("net_version", jsonrpsee::rpc_params![])
            .await
            .expect("net_version should respond (net module registered)");
        assert_eq!(net_version, MAINNET.chain().id().to_string());

        // web3 module: non-empty client version string.
        let client_version: String = client
            .request("web3_clientVersion", jsonrpsee::rpc_params![])
            .await
            .expect("web3_clientVersion should respond (web3 module registered)");
        assert!(!client_version.is_empty(), "client version must be non-empty");

        // txpool module: {pending, queued} (zero on our noop pool, but the module must serve).
        let txpool_status: serde_json::Value = client
            .request("txpool_status", jsonrpsee::rpc_params![])
            .await
            .expect("txpool_status should respond (txpool module registered)");
        assert!(txpool_status.get("pending").is_some(), "txpool_status must have a pending field");

        // eth filter path: getLogs over the produced range returns an array (empty is fine).
        let logs: serde_json::Value = client
            .request(
                "eth_getLogs",
                jsonrpsee::rpc_params![serde_json::json!({
                    "fromBlock": "0x0",
                    "toBlock": "latest"
                })],
            )
            .await
            .expect("eth_getLogs should respond");
        assert!(logs.is_array(), "eth_getLogs must return an array");

        // debug module: re-execute block 1 through arb_revm with the default struct tracer. This
        // is the meaningful proof that debug/trace execution works on our hashed-state node.
        let traces: serde_json::Value = client
            .request(
                "debug_traceBlockByNumber",
                jsonrpsee::rpc_params!["0x1", serde_json::json!({})],
            )
            .await
            .expect("debug_traceBlockByNumber should respond (debug module + arb_revm tracing)");
        assert!(traces.is_array(), "debug_traceBlockByNumber must return an array of traces");

        drop(rpc_handle);
    }
