//! Diagnostic: dump the first N sequencer batches after the Nitro genesis, with their
//! timeBounds (min/max L1 block + timestamp), segment-kind histogram, and the per-message
//! L1 block / timestamp the multiplexer assigns. Root-causes the block-22207832
//! NUMBER-opcode divergence (per-message L1 block over-increment vs canonical flat).
//!
//! Run: ARB_L1_RPC=<url> cargo test -p arb-reth-l1 --test derive_dump -- --ignored --nocapture

use std::collections::BTreeMap;

use alloy_provider::ProviderBuilder;
use arb_reth_derive::batch::{self, segment_kind};
use arb_reth_derive::delayed::{DelayedMessage, DelayedSource};
use arb_reth_derive::multiplexer::extract_messages;

/// Returns a sentinel delayed message for any index, so extraction proceeds through a
/// batch's `Delayed` segments. We only care about the L2 messages' L1-block progression
/// (driven by `AdvL1`/`AdvTs`), not delayed content; delayed msgs show block_number 0.
struct StubDelayed(DelayedMessage);
impl DelayedSource for StubDelayed {
    fn message(&self, _index: u64) -> Option<&DelayedMessage> {
        Some(&self.0)
    }
}
use arb_reth_l1::reader::{BatchPayload, SequencerInboxReader};
use arb_reth_l1::{NITRO_GENESIS_BLOCK_MAINNET, SEQUENCER_INBOX_DEPLOY_BLOCK_MAINNET};

fn kind_name(k: u8) -> &'static str {
    match k {
        segment_kind::L2_MESSAGE => "L2",
        segment_kind::L2_MESSAGE_BROTLI => "L2Brotli",
        segment_kind::DELAYED_MESSAGES => "Delayed",
        segment_kind::ADVANCE_TIMESTAMP => "AdvTs",
        segment_kind::ADVANCE_L1_BLOCK => "AdvL1",
        _ => "Other",
    }
}

#[tokio::test]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC and run with --ignored --nocapture"]
async fn dump_first_batches() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };
    let want_msgs: usize = std::env::var("DUMP_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let reader = SequencerInboxReader::mainnet(provider);

    let stub = StubDelayed(DelayedMessage {
        kind: 0,
        sender: alloy_primitives::Address::ZERO,
        block_number: 0,
        timestamp: 0,
        inbox_seq_num: 0,
        base_fee_l1: alloy_primitives::U256::ZERO,
        data: Vec::new(),
        before_inbox_acc: alloy_primitives::B256::ZERO,
    });

    let batch0_block = reader
        .delivery_block_of_batch(0, SEQUENCER_INBOX_DEPLOY_BLOCK_MAINNET, 50_000)
        .await
        .expect("rpc")
        .expect("batch 0 not found");
    println!("batch 0 delivery block = {batch0_block}");

    let mut from = batch0_block;
    let mut emitted = 0usize;
    let mut delayed_cursor = 0u64;
    let mut global_msg_idx = 0u64;

    'outer: while emitted < want_msgs {
        let to = from + 1_000;
        let mut logs = reader.batch_logs(from, to).await.expect("batch_logs");
        logs.sort_by_key(|l| (l.block_number.unwrap_or(0), l.log_index.unwrap_or(0)));
        if logs.is_empty() {
            from = to + 1;
            continue;
        }
        for log in &logs {
            let dblock = log.block_number.unwrap_or(0);
            let batch = reader.resolve_batch(log).await.expect("resolve_batch");
            let h = batch.event.batch_header();
            let payload = match &batch.payload {
                BatchPayload::Calldata(p) => p.clone(),
                BatchPayload::None => Vec::new(),
                BatchPayload::Blob { .. } => {
                    println!("  seq {} BLOB (skipped)", batch.sequence_number);
                    continue;
                }
            };
            let segments = if payload.is_empty() {
                Vec::new()
            } else {
                let body = batch::decompress_payload(&payload).expect("decompress");
                batch::parse_segments(&body).expect("parse_segments")
            };
            let mut hist: BTreeMap<&str, usize> = BTreeMap::new();
            for s in &segments {
                *hist.entry(kind_name(s.kind)).or_default() += 1;
            }
            println!(
                "seq {:>4} @L1 {} | l1block[{}..{}] ts[{}..{}] afterDelayed {} | segs {} {:?}",
                batch.sequence_number,
                dblock,
                h.min_l1_block,
                h.max_l1_block,
                h.min_timestamp,
                h.max_timestamp,
                h.after_delayed_messages,
                segments.len(),
                hist,
            );

            match extract_messages(&h, &segments, delayed_cursor, &stub) {
                Ok(msgs) => {
                    for m in &msgs {
                        let l2 = NITRO_GENESIS_BLOCK_MAINNET + 1 + global_msg_idx;
                        println!(
                            "    msg#{:>3} -> L2 {} | l1block {} ts {} delayedRead {}",
                            global_msg_idx,
                            l2,
                            m.header.block_number,
                            m.header.timestamp,
                            m.delayed_messages_read,
                        );
                        global_msg_idx += 1;
                        emitted += 1;
                        if emitted >= want_msgs {
                            break 'outer;
                        }
                    }
                }
                Err(e) => {
                    println!("    (extract_messages needs delayed source: {e:?})");
                }
            }
            delayed_cursor = h.after_delayed_messages;
        }
        from = to + 1;
    }
}
