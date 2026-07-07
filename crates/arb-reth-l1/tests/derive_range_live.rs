//! Live end-to-end check of the catch-up orchestration ([`derive_range`]): fetch a real
//! L1 block range, resolve its batches, reconstruct any delayed messages, and assemble
//! the feed-message stream, then decode that stream back to transactions.
//!
//! L1 block 19000015 holds several batches; batch 497980 (the fixture-anchored one,
//! L2 blocks 170137322..628) is among them. We assert its 496 transactions appear as a
//! contiguous, correctly-ordered run inside the range's derived output, bounded by the
//! known first/last hashes. Combined with `canonical_parity_live.rs` (which proves those
//! 496 equal the chain), this pins the live fetch + assemble path to canonical Arbitrum
//! One. Requires `ARB_L1_RPC`; run with `-- --ignored`.

use alloy_primitives::B256;
use arbitrum_alloy_consensus::transactions::ArbTxEnvelope;
use arb_reth_l1::sync::{derive_range, resolve_batches, DEFAULT_DELAYED_WINDOW};
use arbitrum_alloy_sequencer::reader::parse_message;

const TARGET_BLOCK: u64 = 19_000_015;

#[tokio::test]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC and run with --ignored"]
async fn derive_range_reproduces_batch_497980_txs() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };

    use alloy_provider::ProviderBuilder;
    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let seq_reader = arb_reth_l1::SequencerInboxReader::mainnet(provider.clone());
    let delayed_reader = arb_reth_l1::DelayedInboxReader::mainnet(provider);

    // The range's start cursor is the previous batch's afterDelayedMessagesRead. Scan
    // back a small window for the most recent batch before the target block to learn it
    // (rather than assuming the target block's first batch is 497980).
    let prior = resolve_batches(&seq_reader, None, TARGET_BLOCK - 2_000, TARGET_BLOCK - 1)
        .await
        .expect("resolve prior batches");
    let start_delayed = prior
        .last()
        .map(|(b, _)| b.event.after_delayed_messages_read)
        .expect("a batch exists in the lookback window");

    let derived = derive_range(
        &seq_reader,
        &delayed_reader,
        None, // no blob batches in this calldata-era block
        TARGET_BLOCK,
        TARGET_BLOCK,
        start_delayed,
        DEFAULT_DELAYED_WINDOW,
    )
    .await
    .expect("derive_range");

    assert!(derived.batches >= 1, "block must contain at least one batch");

    // Flatten the derived feed-message stream back to transaction hashes.
    let mut hashes: Vec<B256> = Vec::new();
    for f in derived.messages {
        let txs = parse_message(f.message_with_meta_data.l1_incoming_message, 42161, 0)
            .expect("parse_message");
        hashes.extend(txs.iter().map(ArbTxEnvelope::tx_hash));
    }

    let first: B256 = "0x787617661fca412c1cf024747d6deca356fe7002a03d28c39c6a63d9a2d0d267"
        .parse()
        .unwrap();
    let last: B256 = "0xb2329d56c3a1f72f65ffd32db5f6dde822e4d24e9e0982c0fc9736c4c7275051"
        .parse()
        .unwrap();

    let start = hashes
        .iter()
        .position(|h| *h == first)
        .expect("batch 497980's first tx must appear in the derived stream");
    assert_eq!(
        hashes.get(start + 495),
        Some(&last),
        "batch 497980's 496 txs must form a contiguous, ordered run"
    );
}
