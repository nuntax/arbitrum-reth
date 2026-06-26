//! End-to-end decode of a real, pre-Dencun Arbitrum One calldata batch
//! (batch seq 497980, L1 tx 0x95413a09…, L1 block 19000015, dataLocation=0).
//!
//! Unlike blob batches, the calldata path delivers the batch payload directly in
//! the L1 transaction's calldata (`addSequencerL2BatchFromOrigin` `data` argument).
//! The 40-byte timeBounds header is NOT embedded in the calldata bytes; it comes
//! from the `SequencerBatchDelivered` L1 event. `SerializeSequencerInboxBatch` in
//! `nitro/arbnode/sequencer_inbox.go` prepends the header before passing to
//! `ParseSequencerMessage` — we do the same here.
//!
//! # Fixture layout
//! - `arb1_calldata_batch_497980_meta.json`  — event fields + expected counts
//! - `arb1_calldata_batch_497980_payload.bin` — raw calldata `data` bytes
//!   (98 844 bytes; byte 0 = 0x00 BROTLI flag; bytes 1.. = brotli-compressed segments)
//!
//! # Chain verification (Arbitrum One, 2026-06-26)
//! - First decoded tx hash `0x787617…` found at L2 block 170137322 (tx index 1).
//! - Last decoded tx hash  `0xb2329d…` found at L2 block 170137628 (tx index 5).
//! - Block span = 307 blocks = 307 L2 messages (matches).

use std::fs;

use arb_reth_derive::batch::{self, parse_sequencer_batch_delivered, BatchHeader};
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_reth_derive::multiplexer::extract_messages;

fn load_meta() -> serde_json::Value {
    let path =
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/arb1_calldata_batch_497980_meta.json");
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn load_payload() -> Vec<u8> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/arb1_calldata_batch_497980_payload.bin"
    );
    fs::read(path).unwrap_or_else(|e| panic!("read payload: {e}"))
}

/// Helper: construct the `BatchHeader` from the event log data in the meta JSON.
/// Mirrors `nitro/arbnode/sequencer_inbox.go SerializeSequencerInboxBatch`:
/// header = timeBounds from L1 event; rest = calldata `data` bytes.
fn header_from_meta(meta: &serde_json::Value) -> BatchHeader {
    let log_data_hex = meta["seq_batch_delivered_log_data"].as_str().unwrap();
    let log_data: Vec<u8> = (0..log_data_hex.len() / 2)
        .map(|i| u8::from_str_radix(&log_data_hex[i * 2..i * 2 + 2], 16).unwrap())
        .collect();
    parse_sequencer_batch_delivered(&log_data).unwrap().batch_header()
}

#[test]
fn decodes_real_calldata_batch() {
    let meta = load_meta();

    // 1. Reconstruct the BatchHeader from the L1 event data.
    let header = header_from_meta(&meta);
    let tb = &meta["time_bounds"];
    assert_eq!(header.min_timestamp, tb["min_timestamp"].as_u64().unwrap());
    assert_eq!(header.max_timestamp, tb["max_timestamp"].as_u64().unwrap());
    assert_eq!(header.min_l1_block, tb["min_l1_block"].as_u64().unwrap());
    assert_eq!(header.max_l1_block, tb["max_l1_block"].as_u64().unwrap());
    assert_eq!(
        header.after_delayed_messages,
        tb["after_delayed_messages"].as_u64().unwrap()
    );

    // 2. Load the calldata payload (flag byte + compressed segments).
    let payload = load_payload();
    assert_eq!(payload[0], batch::flag::BROTLI, "calldata payload flag byte");

    // 3. Build the full batch bytes = 40-byte header + calldata payload and verify
    //    that BatchHeader::parse round-trips cleanly.
    let mut full_batch = Vec::with_capacity(40 + payload.len());
    for v in [
        header.min_timestamp,
        header.max_timestamp,
        header.min_l1_block,
        header.max_l1_block,
        header.after_delayed_messages,
    ] {
        full_batch.extend_from_slice(&v.to_be_bytes());
    }
    full_batch.extend_from_slice(&payload);

    let (parsed_hdr, parsed_payload) = arb_reth_derive::batch::BatchHeader::parse(&full_batch)
        .expect("BatchHeader::parse on reconstructed full batch");
    assert_eq!(parsed_hdr, header);
    assert_eq!(parsed_payload, payload.as_slice());

    // 4. Brotli decompress -> RLP segment stream -> segments.
    let seg_bytes = batch::decompress_payload(parsed_payload).expect("brotli decompress");
    let segs = batch::parse_segments(&seg_bytes).expect("parse_segments");
    assert!(!segs.is_empty(), "expected at least one segment");

    // 5. Multiplexer pass -> messages. This batch contains no delayed-message segments
    //    (all segments are L2 or Advance* kinds).
    let msgs = extract_messages(
        &header,
        &segs,
        header.after_delayed_messages,
        &NoDelayed,
    )
    .expect("extract_messages");

    println!("L2 messages: {}", msgs.len());
    let expected_msg_count = meta["expected_message_count"].as_u64().unwrap() as usize;
    assert_eq!(msgs.len(), expected_msg_count, "message count");

    // 6. Structural sanity: all messages within the timeBounds; delayed cursor constant.
    for m in &msgs {
        assert!(!m.l2_msg.is_empty());
        assert!(
            m.header.timestamp >= header.min_timestamp
                && m.header.timestamp <= header.max_timestamp
        );
        assert!(
            m.header.block_number >= header.min_l1_block
                && m.header.block_number <= header.max_l1_block
        );
        assert_eq!(m.delayed_messages_read, header.after_delayed_messages);
    }

    // 7. Parse L2 messages -> signed tx encodings.
    let mut tx_hashes = Vec::new();
    for m in &msgs {
        let parsed = parse_l2_message(&m.l2_msg).expect("parse_l2_message");
        tx_hashes.extend(parsed.tx_hashes());
    }
    println!("signed txs: {}", tx_hashes.len());

    let expected_tx_count = meta["expected_tx_count"].as_u64().unwrap() as usize;
    assert_eq!(tx_hashes.len(), expected_tx_count, "decoded tx count");

    // 8. Chain-anchored hash checks. Both hashes confirmed live on Arbitrum One
    //    (2026-06-26) via eth_getTransactionByHash:
    //    first → L2 block 170137322 tx index 1
    //    last  → L2 block 170137628 tx index 5
    assert_eq!(
        format!("{:#x}", tx_hashes[0]),
        meta["first_tx_hash"].as_str().unwrap(),
        "first decoded tx hash"
    );
    assert_eq!(
        format!("{:#x}", tx_hashes[tx_hashes.len() - 1]),
        meta["last_tx_hash"].as_str().unwrap(),
        "last decoded tx hash"
    );
}
