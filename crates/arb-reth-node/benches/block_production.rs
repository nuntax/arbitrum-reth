//! Block-production throughput bench: the full `produce()` path.
//!
//! Unlike `arb-reth-evm`'s `exec_throughput` bench (execution only, no trie), this measures what
//! the live sync actually pays per block: execute the sequencer message's txs AND build the state
//! root against a trie-backed provider, then seal. `produce()` is the exact function the node's
//! block producer calls (`arb_reth_engine::produce`); it does not persist, so re-running it against
//! a fixed parent is a clean, repeatable criterion unit.
//!
//! Reported throughput is gas/s (criterion "thrpt" Elements/s == gas/s here); the per-iteration
//! time column is the block's production latency (its reciprocal is blocks/s).
//!
//! IMPORTANT — trie size dominates. State-root cost scales with the size of the state trie, not the
//! tx count. This bench runs against the *testnode* genesis (a few dozen accounts), so the numbers
//! are a small-state lower bound on production cost and are NOT representative of Arb One's
//! ~1.27M-account trie. To get mainnet-scale numbers, point a read-only `ProviderFactory` at a real
//! datadir and produce the tip+1 block (that path is intentionally left out here so the bench stays
//! portable / CI-runnable). See the module docs for the shape.
//!
//! Run: `cargo bench -p arb-reth-node --bench block_production`

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_primitives::{address, U256};
use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

use arb_reth_engine::produce;
use arb_reth_evm::ArbEvmConfig;
use arb_reth_node::{persist_executed_block, ArbNode};
use reth_primitives_traits::SealedHeader;
use reth_provider::providers::BlockchainProvider;
use reth_provider::test_utils::create_test_provider_factory_with_node_types;
use reth_provider::{HeaderProvider, StateProviderFactory};

/// Testnode chain id (ArbOS v40 fixture, matches the engine-tree integration test).
const CHAIN_ID: u64 = 412_346;
/// Feed messages sequences 0..=17 (0 is the Initialize/genesis message, skipped).
const FEED: &str = include_str!("../tests/fixtures/testnode_feed_seq0_17.ndjson");
/// How many blocks to build + benchmark (the fixture carries 17).
const TARGET: u64 = 17;

type TestDb = Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>;
type TestNodeTypes = reth_node_api::NodeTypesWithDBAdapter<ArbNode, TestDb>;
type TestFactory = reth_provider::ProviderFactory<TestNodeTypes>;

/// The testnode ArbOS chain spec (ArbOS v40, chain 412346), mirroring the integration test.
fn testnode_spec() -> Arc<reth_chainspec::ChainSpec> {
    use arb_reth_node::arb_chain_spec;
    use arb_revm::arbos_init::ArbosInitConfig;
    const CHAIN_CONFIG: &[u8] = include_bytes!("../tests/fixtures/testnode_l2_chain_config.json");
    let init = ArbosInitConfig {
        initial_arbos_version: 40,
        initial_chain_owner: address!("5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927"),
        chain_id: U256::from(CHAIN_ID),
        genesis_block_number: 0,
        initial_l1_base_fee: U256::from(167u64),
        serialized_chain_config: CHAIN_CONFIG.to_vec(),
        debug_precompiles: true,
    };
    Arc::new(arb_chain_spec(&init).expect("build ArbOS chain spec"))
}

/// One benchmarkable block: the parent header to produce on top of, the sequencer message that
/// defines the block, and the block's gas used (for the throughput denominator).
struct Sample {
    parent: SealedHeader<Header>,
    msg: BroadcastFeedMessage,
    gas_used: u64,
}

/// Stand up a temp-MDBX factory seeded with the testnode genesis, then produce+persist blocks
/// 1..=TARGET to build the real chain (and the accumulating trie). Returns one [`Sample`] per block
/// (parent = the tip before that block). Persisting each block lets a later block read its parent's
/// post-state via `state_by_block_hash`.
fn build_chain() -> (TestFactory, BlockchainProvider<TestNodeTypes>, ArbEvmConfig, Vec<Sample>) {
    let factory = create_test_provider_factory_with_node_types::<ArbNode>(testnode_spec());
    reth_db_common::init::init_genesis(&factory).expect("init ArbOS genesis block 0");
    let provider = BlockchainProvider::new(factory.clone()).expect("BlockchainProvider::new");
    let evm_config = ArbEvmConfig::new(CHAIN_ID);

    let msgs: Vec<BroadcastFeedMessage> = FEED
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse feed message"))
        .collect();

    let mut tip: SealedHeader<Header> = {
        let p = factory.provider().unwrap();
        let h = p.sealed_header(0).unwrap().expect("genesis header");
        drop(p);
        h
    };

    let mut samples = Vec::new();
    for m in msgs {
        if m.sequence_number == 0 || m.sequence_number > TARGET {
            continue;
        }
        let (exec_sp, trie_sp) = (
            provider.state_by_block_hash(tip.hash()).expect("exec state provider"),
            provider.state_by_block_hash(tip.hash()).expect("trie state provider"),
        );
        let built = produce(&evm_config, CHAIN_ID, &tip, &m, exec_sp, trie_sp)
            .unwrap_or_else(|e| panic!("produce block {} failed: {e:?}", m.sequence_number));

        let gas_used = built.execution_output.result.gas_used;
        let new_tip = SealedHeader::seal_slow(built.recovered_block.header().clone());
        let recovered = (*built.recovered_block).clone();
        persist_executed_block(
            &factory,
            recovered,
            built.execution_output.state.clone(),
            built.execution_output.result.receipts.clone(),
            gas_used,
        )
        .unwrap_or_else(|e| panic!("persist block {} failed: {e:?}", m.sequence_number));

        samples.push(Sample { parent: tip.clone(), msg: m, gas_used });
        tip = new_tip;
    }
    (factory, provider, evm_config, samples)
}

/// Produce one block from its (persisted) parent. The unit both benches share.
fn produce_one(provider: &BlockchainProvider<TestNodeTypes>, cfg: &ArbEvmConfig, s: &Sample) {
    // Two independent providers: sharing one corrupts execution reads vs the trie build (see
    // produce's contract).
    let exec_sp = provider.state_by_block_hash(s.parent.hash()).unwrap();
    let trie_sp = provider.state_by_block_hash(s.parent.hash()).unwrap();
    let built = produce(cfg, CHAIN_ID, &s.parent, &s.msg, exec_sp, trie_sp).unwrap();
    black_box(built);
}

fn bench_block_production(c: &mut Criterion) {
    let (_factory, provider, evm_config, samples) = build_chain();

    // Headline: produce the whole chain per iteration. thrpt = blocks/s (the "block production
    // speed" number); the time column is total wall-clock for `samples.len()` blocks.
    let mut headline = c.benchmark_group("block_production_chain");
    headline.throughput(Throughput::Elements(samples.len() as u64));
    headline.bench_function("produce_all", |b| {
        b.iter(|| {
            for s in &samples {
                produce_one(&provider, &evm_config, s);
            }
        });
    });
    headline.finish();

    // Per-block breakdown: thrpt = that block's gas/s (varies with block gas); time = its
    // production latency. Useful to see where cost concentrates as trie/txs grow.
    let mut per_block = c.benchmark_group("block_production_per_block");
    for sample in &samples {
        let block_number = sample.parent.header().number + 1;
        per_block.throughput(Throughput::Elements(sample.gas_used.max(1)));
        per_block.bench_with_input(BenchmarkId::from_parameter(block_number), sample, |b, s| {
            b.iter(|| produce_one(&provider, &evm_config, s));
        });
    }
    per_block.finish();
}

criterion_group!(benches, bench_block_production);
criterion_main!(benches);
