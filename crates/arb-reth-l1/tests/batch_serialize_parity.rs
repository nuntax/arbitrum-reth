//! `serialize_batch` parity: `keccak256(serialize_batch(batch))` must equal the
//! `data_hash` field recorded in that batch's on-chain `BatchPostingReport`. The
//! expected hashes below were read from the report bodies in each batch's L1
//! posting tx (calldata batch 497980 in tx 0x95413a09…, blob batch 1277861 in tx
//! 0x20eae1f4…), so a match pins our serialization byte-for-byte to Nitro's.

use std::fs;

use alloy_primitives::{b256, hex, B256};
use arb_reth_derive::batch::{data_location, parse_sequencer_batch_delivered, SequencerBatchDeliveredData};
use arb_reth_l1::{batch_data_hash, batch_data_stats, serialize_batch, BatchPayload, DeliveredBatch};

// data_hash fields from the on-chain batch posting reports.
const CALLDATA_DATA_HASH: B256 =
    b256!("0x20f772b46bc1baa25666f148b8a26de4d9ef1a7719e1a3fd2eeca5db4fbb3e48");
const BLOB_DATA_HASH: B256 =
    b256!("0xb2135e1bb081f59c26e6e7686f390d40bb4bb3c399b33ad0bf33511ad865cbc7");

const CALLDATA_EVENT_LOG: &str = concat!(
    "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
    "00000000000000000000000000000000000000000000000000000000001432cf",
    "0000000000000000000000000000000000000000000000000000000065a190f7",
    "0000000000000000000000000000000000000000000000000000000065a2f087",
    "000000000000000000000000000000000000000000000000000000000121d44f",
    "000000000000000000000000000000000000000000000000000000000121eadb",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

#[test]
fn calldata_batch_serialization_matches_report_hash() {
    let input = hex::decode(
        fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/arb1_calldata_batch_497980_l1_tx_input.hex"
        ))
        .unwrap()
        .trim(),
    )
    .unwrap();
    let payload = arb_reth_l1::extract_calldata_payload(&input).unwrap();
    let payload_len = payload.len();
    let batch = DeliveredBatch {
        sequence_number: 497_980,
        before_acc: B256::ZERO,
        after_acc: B256::ZERO,
        event: parse_sequencer_batch_delivered(&hex::decode(CALLDATA_EVENT_LOG).unwrap()).unwrap(),
        payload: BatchPayload::Calldata(payload),
    };

    assert_eq!(batch_data_hash(&batch), CALLDATA_DATA_HASH);
    let stats = batch_data_stats(&serialize_batch(&batch));
    assert_eq!(stats.length, (40 + payload_len) as u64, "serialized length = header + data");
    assert!(stats.non_zeros > 0 && stats.non_zeros <= stats.length);
}

#[test]
fn blob_batch_serialization_matches_report_hash() {
    let versioned_hashes = vec![
        b256!("0x013b8fb7e8bd74d1e36856556b643d1f0aef18ab66b56a216481121e491aeed8"),
        b256!("0x014a6cc6614ad762335f1e04d5c66829955689e2ad2b91e277e0558eab2e4c40"),
        b256!("0x0157a4ce2d8a9f3a8b375768a6cd6324b239725df5eb7e250976c2089bd303dc"),
    ];
    let event = SequencerBatchDeliveredData {
        delayed_acc: B256::ZERO,
        after_delayed_messages_read: 2_484_028,
        min_timestamp: 1_782_345_239,
        max_timestamp: 1_782_432_407,
        min_l1_block: 25_390_852,
        max_l1_block: 25_398_116,
        data_location: data_location::BLOB_HASHES,
    };
    let batch = DeliveredBatch {
        sequence_number: 1_277_861,
        before_acc: B256::ZERO,
        after_acc: B256::ZERO,
        event,
        payload: BatchPayload::Blob { versioned_hashes, block_number: 25_398_052 },
    };

    assert_eq!(batch_data_hash(&batch), BLOB_DATA_HASH);
    // header(40) + flag(1) + 3 * 32-byte versioned hashes.
    assert_eq!(batch_data_stats(&serialize_batch(&batch)).length, 40 + 1 + 96);
}
