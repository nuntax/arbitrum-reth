//! Assembly pipeline: a run of resolved batches + a delayed source becomes the
//! ordered feed-message stream the driver executes. Validates the calldata batch
//! end to end (feed messages decode to the canonical-verified tx list) and the
//! BatchPostingReport stats injection by data_hash.

use std::collections::BTreeMap;
use std::fs;

use alloy_primitives::{b256, hex, B256};
use arb_alloy_consensus::transactions::ArbTxEnvelope;
use arb_reth_derive::batch::parse_sequencer_batch_delivered;
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_reth_derive::message::{DerivedMessage, L1IncomingMessageHeader};
use arb_sequencer_network::reader::parse_message;
use arb_sequencer_network::sequencer::feed::BatchDataStats;
use arb_reth_l1::assemble::{derived_to_feed, KIND_BATCH_POSTING_REPORT};
use arb_reth_l1::{assemble_feed_messages, extract_calldata_payload, BatchPayload, DeliveredBatch};

const EVENT_LOG_DATA: &str = concat!(
    "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
    "00000000000000000000000000000000000000000000000000000000001432cf",
    "0000000000000000000000000000000000000000000000000000000065a190f7",
    "0000000000000000000000000000000000000000000000000000000065a2f087",
    "000000000000000000000000000000000000000000000000000000000121d44f",
    "000000000000000000000000000000000000000000000000000000000121eadb",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

#[test]
fn assembles_calldata_batch_to_canonical_txs() {
    let input = hex::decode(
        fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/arb1_calldata_batch_497980_l1_tx_input.hex"
        ))
        .unwrap()
        .trim(),
    )
    .unwrap();
    let payload = extract_calldata_payload(&input).unwrap();
    let batch = DeliveredBatch {
        sequence_number: 497_980,
        before_acc: B256::ZERO,
        after_acc: B256::ZERO,
        event: parse_sequencer_batch_delivered(&hex::decode(EVENT_LOG_DATA).unwrap()).unwrap(),
        payload: BatchPayload::Calldata(payload.clone()),
    };

    // before-delayed cursor for this batch = the event's afterDelayedMessagesRead
    // (this batch consumes no delayed messages).
    let feed = assemble_feed_messages(&[(batch, payload)], &NoDelayed, 1_323_727).unwrap();

    let mut hashes: Vec<B256> = Vec::new();
    for f in feed {
        let txs = parse_message(f.message_with_meta_data.l1_incoming_message, 42161, 0).unwrap();
        hashes.extend(txs.iter().map(ArbTxEnvelope::tx_hash));
    }
    assert_eq!(hashes.len(), 496);

    // cross-check the first/last vs the chain-anchored fixture values
    assert_eq!(
        format!("{:#x}", hashes[0]),
        "0x787617661fca412c1cf024747d6deca356fe7002a03d28c39c6a63d9a2d0d267"
    );
    assert_eq!(
        format!("{:#x}", hashes[495]),
        "0xb2329d56c3a1f72f65ffd32db5f6dde822e4d24e9e0982c0fc9736c4c7275051"
    );
}

fn report_message(data_hash: B256) -> DerivedMessage {
    // BatchPostingReport body is fixed-width: [timestamp(32), poster(20),
    // data_hash(32), batch_num(32), l1_base_fee(32)]; data_hash lives at [52..84].
    let mut body = vec![0u8; 148];
    body[52..84].copy_from_slice(data_hash.as_slice());
    DerivedMessage {
        header: L1IncomingMessageHeader {
            kind: KIND_BATCH_POSTING_REPORT,
            poster: Default::default(),
            block_number: 1,
            timestamp: 1,
            request_id: None,
            l1_base_fee: Default::default(),
        },
        l2_msg: body,
        delayed_messages_read: 0,
    }
}

#[test]
fn report_message_gets_stats_from_known_batch() {
    let dh = b256!("0x20f772b46bc1baa25666f148b8a26de4d9ef1a7719e1a3fd2eeca5db4fbb3e48");
    let stats = BatchDataStats { length: 98_884, non_zeros: 12_345 };
    let mut map = BTreeMap::new();
    map.insert(dh, stats.clone());

    let feed = derived_to_feed(&report_message(dh), 0, &map).unwrap();
    let attached = feed.message_with_meta_data.l1_incoming_message.batch_data_stats;
    assert_eq!(attached, Some(stats));
}

#[test]
fn report_message_without_known_batch_errors() {
    let dh = b256!("0xdeadbeef00000000000000000000000000000000000000000000000000000000");
    let err = derived_to_feed(&report_message(dh), 0, &BTreeMap::new());
    assert!(matches!(err, Err(arb_reth_l1::L1Error::Missing(_))));
}
