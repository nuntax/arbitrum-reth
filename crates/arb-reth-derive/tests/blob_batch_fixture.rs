//! End-to-end decode of a real, delayed-message-free Arbitrum One blob batch
//! (batch seq 1277861, L1 tx 0x20eae1f4…, L1 block 25398052, 3 blobs), fetched
//! from blobscan. This batch consumed zero delayed messages, so the multiplexer
//! produces the complete message list with the real timeBounds from the L1 event.

use std::collections::BTreeMap;
use std::fs;

use arb_reth_derive::batch::{self, flag, segment_kind, BatchHeader};
use arb_reth_derive::blob::{decode_blobs, Blob, BYTES_PER_BLOB};
use arb_reth_derive::multiplexer::extract_messages;

fn load_blob(path: &str) -> Blob {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    assert_eq!(bytes.len(), BYTES_PER_BLOB);
    let mut b = [0u8; BYTES_PER_BLOB];
    b.copy_from_slice(&bytes);
    b
}

#[test]
fn decodes_real_delayed_free_blob_batch() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
    let blobs: Vec<Blob> = (0..3)
        .map(|i| load_blob(&format!("{dir}/arb1_cleanbatch_1277861_blob{i}.bin")))
        .collect();

    // timeBounds from the SequencerBatchDelivered event (see *_meta.json).
    let header = BatchHeader {
        min_timestamp: 1_782_345_239,
        max_timestamp: 1_782_432_407,
        min_l1_block: 25_390_852,
        max_l1_block: 25_398_116,
        after_delayed_messages: 2_484_028,
    };

    // 1. field-element decode -> batch payload, must be brotli-flagged.
    let payload = decode_blobs(&blobs).expect("decode_blobs");
    assert_eq!(payload[0], flag::BROTLI, "payload flag byte");

    // 2. brotli -> RLP segment stream -> segments.
    let seg_bytes = batch::decompress_payload(&payload).expect("brotli");
    let segs = batch::parse_segments(&seg_bytes).expect("segments");
    assert!(!segs.is_empty());

    let mut hist = BTreeMap::new();
    for s in &segs {
        *hist.entry(s.kind).or_insert(0usize) += 1;
    }
    println!("segment kinds: {hist:?}");
    // This batch is delayed-free: no DelayedMessages (kind 2) segments.
    assert_eq!(hist.get(&segment_kind::DELAYED_MESSAGES), None, "expected delayed-free batch");

    // 3. full multiplexer pass -> complete message list (no DelayedUnsupported).
    let msgs = extract_messages(&header, &segs, header.after_delayed_messages, &arb_reth_derive::delayed::NoDelayed)
        .expect("multiplex");
    println!("L2 messages: {}", msgs.len());
    assert!(!msgs.is_empty());

    // 4. structural checks: timestamps/blocks within the batch bounds; delayed cursor constant.
    let mut sub_kinds = BTreeMap::new();
    for m in &msgs {
        assert!(!m.l2_msg.is_empty());
        assert!(m.header.timestamp >= header.min_timestamp && m.header.timestamp <= header.max_timestamp);
        assert!(m.header.block_number >= header.min_l1_block && m.header.block_number <= header.max_l1_block);
        assert_eq!(m.delayed_messages_read, header.after_delayed_messages);
        *sub_kinds.entry(m.l2_msg[0]).or_insert(0usize) += 1;
    }
    // L2 sub-kinds: 0=UnsignedUserTx 1=ContractTx 3=Batch 4=SignedTx 7=SignedCompressedTx
    println!("L2 message sub-kinds: {sub_kinds:?}");

    // 5. Parse the L2 messages into signed transactions and hash them.
    use arb_reth_derive::l2message::parse_l2_message;
    let mut tx_hashes = Vec::new();
    let mut unsigned = 0usize;
    for m in &msgs {
        let parsed = parse_l2_message(&m.l2_msg).expect("parse l2 message");
        unsigned += parsed.unsigned_count;
        tx_hashes.extend(parsed.tx_hashes());
    }
    println!("signed txs: {}  unsigned/contract: {}", tx_hashes.len(), unsigned);

    // Chain-verified anchors (2026-06-26): a 30-hash spread of these was confirmed
    // to exist on Arbitrum One via eth_getTransactionByHash (L2 blocks 477357766..
    // 477358105). These assertions pin the whole decode pipeline to that ground truth.
    assert_eq!(tx_hashes.len(), 2984, "decoded tx count");
    assert_eq!(
        format!("{:#x}", tx_hashes[0]),
        "0xed816a893486194c1026e72062c32a8dda805086cdb83e5539d85fd9c68a32d5",
        "first decoded tx hash",
    );
    assert_eq!(
        format!("{:#x}", tx_hashes[tx_hashes.len() - 1]),
        "0x64dd4e32d368b5ccc14b4ab8396a7eec33a7fd56c5feb912aed17f568ec410ff",
        "last decoded tx hash",
    );
}
