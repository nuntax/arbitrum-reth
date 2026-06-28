//! L1 blob-path parity for Arbitrum One blob batch seq 1277861 (L1 tx 0x20eae1f4…,
//! block 25398052, 3 blobs, delayed-free).
//!
//! The offline test field-element-decodes the captured blob sidecars and runs the
//! shared `decode_payload_messages` glue, asserting the chain-anchored tx counts and
//! end hashes. The `#[ignore]`d live test sources the SequencerBatchDelivered log
//! from the posting tx's receipt, runs `resolve_batch` (blob branch) + the real
//! beacon sidecar fetch via `resolve_blob_payload`, and decodes. It needs both
//! `ARB_L1_RPC` and `ARB_L1_BEACON`; run with `-- --ignored`.

use std::fs;

use arb_reth_derive::batch::BatchHeader;
use arb_reth_derive::blob::{decode_blobs, Blob, BYTES_PER_BLOB};
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_reth_l1::decode_payload_messages;

const EXPECTED_TXS: usize = 2984;
const FIRST_TX: &str = "0xed816a893486194c1026e72062c32a8dda805086cdb83e5539d85fd9c68a32d5";
const LAST_TX: &str = "0x64dd4e32d368b5ccc14b4ab8396a7eec33a7fd56c5feb912aed17f568ec410ff";

fn blob_batch_header() -> BatchHeader {
    BatchHeader {
        min_timestamp: 1_782_345_239,
        max_timestamp: 1_782_432_407,
        min_l1_block: 25_390_852,
        max_l1_block: 25_398_116,
        after_delayed_messages: 2_484_028,
    }
}

fn load_fixture_blobs() -> Vec<Blob> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../arb-reth-derive/tests/fixtures");
    (0..3)
        .map(|i| {
            let bytes = fs::read(format!("{dir}/arb1_cleanbatch_1277861_blob{i}.bin")).unwrap();
            assert_eq!(bytes.len(), BYTES_PER_BLOB);
            let mut b = [0u8; BYTES_PER_BLOB];
            b.copy_from_slice(&bytes);
            b
        })
        .collect()
}

fn assert_payload_decodes(header: &BatchHeader, payload: &[u8]) {
    let msgs = decode_payload_messages(header, payload, header.after_delayed_messages, &NoDelayed)
        .expect("decode payload");
    let mut tx_hashes = Vec::new();
    for m in &msgs {
        tx_hashes.extend(parse_l2_message(&m.l2_msg).expect("parse_l2_message").tx_hashes());
    }
    assert_eq!(tx_hashes.len(), EXPECTED_TXS, "decoded tx count");
    assert_eq!(format!("{:#x}", tx_hashes[0]), FIRST_TX, "first tx hash");
    assert_eq!(format!("{:#x}", tx_hashes[tx_hashes.len() - 1]), LAST_TX, "last tx hash");
}

/// Field-element decode the captured sidecars and run them through the lib glue.
#[test]
fn decodes_blob_payload_from_fixture_sidecars() {
    let payload = decode_blobs(&load_fixture_blobs()).expect("decode_blobs");
    assert_eq!(payload[0], 0x00, "BROTLI flag byte");
    assert_payload_decodes(&blob_batch_header(), &payload);
}

/// Live: receipt -> resolve_batch (blob branch) -> beacon sidecar fetch -> decode.
#[tokio::test]
#[ignore = "hits live L1 + beacon RPCs; set ARB_L1_RPC and ARB_L1_BEACON, run with --ignored"]
async fn live_resolve_blob_batch_1277861() {
    use alloy_sol_types::SolEvent;

    let (Ok(el), Ok(beacon_url)) = (std::env::var("ARB_L1_RPC"), std::env::var("ARB_L1_BEACON"))
    else {
        eprintln!("ARB_L1_RPC / ARB_L1_BEACON unset; skipping");
        return;
    };

    use alloy_provider::{Provider, ProviderBuilder};
    let provider = ProviderBuilder::new().connect_http(el.parse().expect("parse ARB_L1_RPC"));
    let reader = arb_reth_l1::SequencerInboxReader::mainnet(provider.clone());
    let beacon = arb_reth_l1::BeaconClient::new(beacon_url);

    let tx_hash = "0x20eae1f4954809bb609ab11724c26742e3ca08073ff881f52da44c39784eaf81"
        .parse()
        .unwrap();
    let receipt = provider
        .get_transaction_receipt(tx_hash)
        .await
        .expect("get_transaction_receipt")
        .expect("receipt present");

    let sig = arb_reth_l1::contracts::SequencerBatchDelivered::SIGNATURE_HASH;
    let log = receipt
        .logs()
        .iter()
        .find(|l| l.inner.data.topics().first() == Some(&sig))
        .expect("SequencerBatchDelivered log");

    let batch = reader.resolve_batch(log).await.expect("resolve_batch");
    assert_eq!(batch.sequence_number, 1_277_861);

    let arb_reth_l1::BatchPayload::Blob { versioned_hashes, block_number } = &batch.payload else {
        panic!("expected blob payload, got {:?}", batch.payload);
    };
    assert_eq!(versioned_hashes.len(), 3);

    let payload = reader
        .resolve_blob_payload(*block_number, versioned_hashes, &beacon)
        .await
        .expect("resolve_blob_payload");

    let header = batch.event.batch_header();
    assert_payload_decodes(&header, &payload);
}
