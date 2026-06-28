//! Differential parity against canonical Arbitrum One: the full, ordered list of
//! transactions a batch decodes to must equal the transactions the chain actually
//! contains in the L2 blocks that batch produced (i.e. what Nitro produced). This is
//! stronger than the count + first/last checks in the other fixtures.
//!
//! Each Arbitrum block carries an `ArbitrumInternalTx` (type 0x6a) at index 0 that is
//! not a derived user message, so it is dropped before comparison. Requires an
//! Arbitrum One RPC in `ARB_L2_RPC`; run with `-- --ignored`.

use std::fs;

use alloy_primitives::hex;
use arb_reth_derive::batch::{parse_sequencer_batch_delivered, BatchHeader};
use arb_reth_derive::blob::{decode_blobs, Blob, BYTES_PER_BLOB};
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_reth_l1::{
    decode_batch_messages, decode_payload_messages, extract_calldata_payload, BatchPayload,
    DeliveredBatch,
};

const DERIVE_FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../arb-reth-derive/tests/fixtures");

fn hashes_to_strings(msgs: &[arb_reth_derive::message::DerivedMessage]) -> Vec<String> {
    let mut out = Vec::new();
    for m in msgs {
        for h in parse_l2_message(&m.l2_msg).expect("parse_l2_message").tx_hashes() {
            out.push(format!("{h:#x}"));
        }
    }
    out
}

/// Fetch the user-tx hashes (dropping the index-0 internal tx) for the inclusive L2
/// block range, in block then index order.
async fn canonical_user_tx_hashes(rpc: &str, start: u64, end: u64) -> Vec<String> {
    let client = reqwest::Client::new();
    let mut out = Vec::new();
    let nums: Vec<u64> = (start..=end).collect();
    for chunk in nums.chunks(40) {
        let batch: Vec<serde_json::Value> = chunk
            .iter()
            .map(|n| {
                serde_json::json!({
                    "jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber",
                    "params": [format!("0x{n:x}"), true]
                })
            })
            .collect();
        let resp: Vec<serde_json::Value> = client
            .post(rpc)
            .json(&batch)
            .send()
            .await
            .expect("rpc send")
            .json()
            .await
            .expect("rpc json");
        let mut by_id = std::collections::BTreeMap::new();
        for r in resp {
            by_id.insert(r["id"].as_u64().unwrap(), r);
        }
        for n in chunk {
            let txs = by_id[n]["result"]["transactions"].as_array().unwrap();
            let drop_internal = txs
                .first()
                .map(|t| t["type"].as_str() == Some("0x6a"))
                .unwrap_or(false);
            for t in txs.iter().skip(usize::from(drop_internal)) {
                out.push(t["hash"].as_str().unwrap().to_string());
            }
        }
    }
    out
}

#[tokio::test]
#[ignore = "differential vs canonical Arbitrum One; set ARB_L2_RPC and run with --ignored"]
async fn calldata_batch_matches_canonical_chain() {
    let Ok(rpc) = std::env::var("ARB_L2_RPC") else {
        eprintln!("ARB_L2_RPC unset; skipping");
        return;
    };

    let input = hex::decode(
        fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/arb1_calldata_batch_497980_l1_tx_input.hex"
        ))
        .unwrap()
        .trim(),
    )
    .unwrap();
    let log_data = hex::decode(concat!(
        "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
        "00000000000000000000000000000000000000000000000000000000001432cf",
        "0000000000000000000000000000000000000000000000000000000065a190f7",
        "0000000000000000000000000000000000000000000000000000000065a2f087",
        "000000000000000000000000000000000000000000000000000000000121d44f",
        "000000000000000000000000000000000000000000000000000000000121eadb",
        "0000000000000000000000000000000000000000000000000000000000000000",
    ))
    .unwrap();
    let batch = DeliveredBatch {
        sequence_number: 497_980,
        before_acc: Default::default(),
        after_acc: Default::default(),
        event: parse_sequencer_batch_delivered(&log_data).unwrap(),
        payload: BatchPayload::Calldata(extract_calldata_payload(&input).unwrap()),
    };
    let decoded = hashes_to_strings(&decode_batch_messages(&batch, 1_323_727, &NoDelayed).unwrap());

    // batch 497980 produced L2 blocks 170137322..=170137628.
    let canonical = canonical_user_tx_hashes(&rpc, 170_137_322, 170_137_628).await;
    assert_eq!(decoded.len(), 496);
    assert_eq!(decoded, canonical, "calldata batch did not match canonical chain");
}

#[tokio::test]
#[ignore = "differential vs canonical Arbitrum One; set ARB_L2_RPC and run with --ignored"]
async fn blob_batch_matches_canonical_chain() {
    let Ok(rpc) = std::env::var("ARB_L2_RPC") else {
        eprintln!("ARB_L2_RPC unset; skipping");
        return;
    };

    let blobs: Vec<Blob> = (0..3)
        .map(|i| {
            let b = fs::read(format!("{DERIVE_FIXTURES}/arb1_cleanbatch_1277861_blob{i}.bin")).unwrap();
            let mut a = [0u8; BYTES_PER_BLOB];
            a.copy_from_slice(&b);
            a
        })
        .collect();
    let header = BatchHeader {
        min_timestamp: 1_782_345_239,
        max_timestamp: 1_782_432_407,
        min_l1_block: 25_390_852,
        max_l1_block: 25_398_116,
        after_delayed_messages: 2_484_028,
    };
    let payload = decode_blobs(&blobs).unwrap();
    let msgs =
        decode_payload_messages(&header, &payload, header.after_delayed_messages, &NoDelayed).unwrap();
    let decoded = hashes_to_strings(&msgs);

    // batch 1277861 produced L2 blocks 477357766..=477358105.
    let canonical = canonical_user_tx_hashes(&rpc, 477_357_766, 477_358_105).await;
    assert_eq!(decoded.len(), 2984);
    assert_eq!(decoded, canonical, "blob batch did not match canonical chain");
}
