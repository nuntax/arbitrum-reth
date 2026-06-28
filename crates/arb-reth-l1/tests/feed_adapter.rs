//! The `DerivedMessage -> BroadcastFeedMessage` adapter must feed the validated
//! decoder (`arb_sequencer_network::reader::parse_message`, the one `digest_message`
//! uses) exactly the transactions the canonical-verified derive path produces.
//!
//! Decodes the real calldata batch (seq 497980, all L2Message kind), runs each
//! derived message through the adapter + `parse_message`, and asserts the full
//! ordered tx-hash list equals what the derive path yields (which
//! `canonical_parity_live` pins to Arbitrum One itself).

use std::fs;

use alloy_primitives::{hex, B256};
use arb_alloy_consensus::transactions::ArbTxEnvelope;
use arb_reth_derive::batch::parse_sequencer_batch_delivered;
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_sequencer_network::reader::parse_message;
use arb_reth_l1::{
    decode_batch_messages, derived_to_feed_message, extract_calldata_payload, BatchPayload,
    DeliveredBatch,
};

const CHAIN_ID: u64 = 42161;
const EVENT_LOG_DATA: &str = concat!(
    "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
    "00000000000000000000000000000000000000000000000000000000001432cf",
    "0000000000000000000000000000000000000000000000000000000065a190f7",
    "0000000000000000000000000000000000000000000000000000000065a2f087",
    "000000000000000000000000000000000000000000000000000000000121d44f",
    "000000000000000000000000000000000000000000000000000000000121eadb",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

fn calldata_batch() -> DeliveredBatch {
    let input = hex::decode(
        fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/arb1_calldata_batch_497980_l1_tx_input.hex"
        ))
        .unwrap()
        .trim(),
    )
    .unwrap();
    DeliveredBatch {
        sequence_number: 497_980,
        before_acc: Default::default(),
        after_acc: Default::default(),
        event: parse_sequencer_batch_delivered(&hex::decode(EVENT_LOG_DATA).unwrap()).unwrap(),
        payload: BatchPayload::Calldata(extract_calldata_payload(&input).unwrap()),
    }
}

#[test]
fn adapter_feeds_decoder_same_txs_as_derive_path() {
    let msgs = decode_batch_messages(&calldata_batch(), 1_323_727, &NoDelayed).unwrap();

    // Derive-path hashes (canonical-verified in canonical_parity_live).
    let mut expected: Vec<B256> = Vec::new();
    for m in &msgs {
        expected.extend(parse_l2_message(&m.l2_msg).unwrap().tx_hashes());
    }

    // Adapter -> the decoder `digest_message` uses.
    let mut via_adapter: Vec<B256> = Vec::new();
    for (i, m) in msgs.iter().enumerate() {
        let feed = derived_to_feed_message(m, i as u64);
        let txs = parse_message(
            feed.message_with_meta_data.l1_incoming_message,
            CHAIN_ID,
            0,
        )
        .expect("parse_message on adapted feed message");
        via_adapter.extend(txs.iter().map(ArbTxEnvelope::tx_hash));
    }

    assert_eq!(via_adapter.len(), 496);
    assert_eq!(via_adapter, expected, "adapter path diverged from derive path");
}
