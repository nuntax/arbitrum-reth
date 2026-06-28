//! L1 fetch + calldata-extraction parity for Arbitrum One calldata batch seq 497980
//! (L1 tx 0x95413a09…, block 19000015, dataLocation=0).
//!
//! The offline test feeds the captured raw L1 transaction input through
//! [`extract_calldata_payload`] and asserts the recovered payload matches the
//! ground-truth `data` arg (the same `payload.bin` the derive crate decodes), then
//! runs the full batch decode and checks the message/tx counts and end tx hashes.
//!
//! The `#[ignore]`d live test does the real `eth_getLogs` + `eth_getTransactionByHash`
//! against an L1 RPC (set `ARB_L1_RPC`); run with `cargo test -- --ignored`.

use std::fs;

use alloy_primitives::{hex, B256};
use arb_reth_derive::batch::parse_sequencer_batch_delivered;
use arb_reth_derive::delayed::NoDelayed;
use arb_reth_derive::l2message::parse_l2_message;
use arb_reth_l1::{
    decode_batch_messages, extract_calldata_payload, BatchPayload, DeliveredBatch,
    SEQUENCER_INBOX_MAINNET,
};

const EXPECTED_MESSAGES: usize = 307;
const EXPECTED_TXS: usize = 496;
const FIRST_TX: &str = "0x787617661fca412c1cf024747d6deca356fe7002a03d28c39c6a63d9a2d0d267";
const LAST_TX: &str = "0xb2329d56c3a1f72f65ffd32db5f6dde822e4d24e9e0982c0fc9736c4c7275051";
const AFTER_DELAYED: u64 = 1_323_727;

// Non-indexed data of the SequencerBatchDelivered event for this batch.
const EVENT_LOG_DATA: &str = concat!(
    "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
    "00000000000000000000000000000000000000000000000000000000001432cf",
    "0000000000000000000000000000000000000000000000000000000065a190f7",
    "0000000000000000000000000000000000000000000000000000000065a2f087",
    "000000000000000000000000000000000000000000000000000000000121d44f",
    "000000000000000000000000000000000000000000000000000000000121eadb",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

fn load_raw_tx_input() -> Vec<u8> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/arb1_calldata_batch_497980_l1_tx_input.hex"
    );
    let hexstr = fs::read_to_string(path).unwrap();
    hex::decode(hexstr.trim()).expect("decode tx input hex")
}

fn load_ground_truth_payload() -> Vec<u8> {
    // The derive crate already stores the extracted `data` arg for this batch.
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../arb-reth-derive/tests/fixtures/arb1_calldata_batch_497980_payload.bin"
    );
    fs::read(path).expect("read ground-truth payload")
}

fn batch_from_payload(payload: Vec<u8>) -> DeliveredBatch {
    let event =
        parse_sequencer_batch_delivered(&hex::decode(EVENT_LOG_DATA).unwrap()).unwrap();
    DeliveredBatch {
        sequence_number: 497_980,
        before_acc: B256::ZERO,
        after_acc: B256::ZERO,
        event,
        payload: BatchPayload::Calldata(payload),
    }
}

fn assert_decodes_to_fixture(batch: &DeliveredBatch) {
    let msgs = decode_batch_messages(batch, AFTER_DELAYED, &NoDelayed).expect("decode batch");
    assert_eq!(msgs.len(), EXPECTED_MESSAGES, "L2 message count");

    let mut tx_hashes = Vec::new();
    for m in &msgs {
        let parsed = parse_l2_message(&m.l2_msg).expect("parse_l2_message");
        tx_hashes.extend(parsed.tx_hashes());
    }
    assert_eq!(tx_hashes.len(), EXPECTED_TXS, "decoded tx count");
    assert_eq!(format!("{:#x}", tx_hashes[0]), FIRST_TX, "first tx hash");
    assert_eq!(format!("{:#x}", tx_hashes[tx_hashes.len() - 1]), LAST_TX, "last tx hash");
}

/// Recover the payload from the real L1 calldata and check it byte-for-byte against
/// the independently captured `data` arg, then decode end to end.
#[test]
fn extracts_calldata_payload_from_raw_l1_tx() {
    let input = load_raw_tx_input();
    let payload = extract_calldata_payload(&input).expect("extract payload");

    let ground_truth = load_ground_truth_payload();
    assert_eq!(payload.len(), ground_truth.len(), "payload length");
    assert_eq!(payload, ground_truth, "extracted payload matches ground truth");
    assert_eq!(payload[0], 0x00, "BROTLI flag byte");

    assert_decodes_to_fixture(&batch_from_payload(payload));
}

#[test]
fn rejects_truncated_calldata() {
    assert!(matches!(
        extract_calldata_payload(&[0x8f, 0x11]),
        Err(arb_reth_l1::L1Error::CalldataTooShort(2))
    ));
}

#[test]
fn rejects_unknown_selector() {
    let input = [0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00];
    assert!(matches!(
        extract_calldata_payload(&input),
        Err(arb_reth_l1::L1Error::UnknownSelector([0xde, 0xad, 0xbe, 0xef]))
    ));
}

/// Live, archive-free end-to-end: source the SequencerBatchDelivered log from the
/// posting tx's receipt (an indexed, non-archive call most public endpoints serve),
/// then run the real `resolve_batch` (calldata fetch + event parse) and decode.
/// Requires `ARB_L1_RPC`; skipped if unset. Run with `-- --ignored`.
#[tokio::test]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC and run with --ignored"]
async fn live_resolve_batch_497980_via_receipt() {
    use alloy_sol_types::SolEvent;

    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };

    use alloy_provider::{Provider, ProviderBuilder};
    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let reader = arb_reth_l1::SequencerInboxReader::mainnet(provider.clone());

    let tx_hash = "0x95413a0915c9730f9fb14644bd7b6b2838febf2e04bed8741c17aa9f78dd1f30"
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
        .expect("SequencerBatchDelivered log in receipt");

    let batch = reader.resolve_batch(log).await.expect("resolve_batch");
    assert_eq!(batch.sequence_number, 497_980);
    assert_eq!(batch.event.after_delayed_messages_read, AFTER_DELAYED);
    assert!(matches!(batch.payload, BatchPayload::Calldata(_)));
    assert_decodes_to_fixture(&batch);
}

/// Live end-to-end: fetch the batch log + posting tx from an L1 RPC and decode.
/// Requires `ARB_L1_RPC` and an archive-capable endpoint (historical `getLogs`);
/// skipped if unset. Run with `-- --ignored`.
#[tokio::test]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC and run with --ignored"]
async fn live_fetch_and_decode_batch_497980() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };

    use alloy_provider::ProviderBuilder;
    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let reader = arb_reth_l1::SequencerInboxReader::mainnet(provider);
    assert_eq!(reader.address(), SEQUENCER_INBOX_MAINNET);

    let logs = reader.batch_logs(19_000_015, 19_000_015).await.expect("batch_logs");
    // A single L1 block holds several batches; select ours by its seq topic before
    // doing the (rate-limited) transaction fetch, so we resolve exactly one batch.
    let target = logs
        .iter()
        .find(|l| {
            let t = l.inner.data.topics();
            t.len() >= 2 && u64::from_be_bytes(t[1].0[24..32].try_into().unwrap()) == 497_980
        })
        .expect("batch 497980 log in block 19000015");
    let batch = reader.resolve_batch(target).await.expect("resolve_batch");
    assert_eq!(batch.event.after_delayed_messages_read, AFTER_DELAYED);
    assert!(matches!(batch.payload, BatchPayload::Calldata(_)));
    assert_decodes_to_fixture(&batch);
}
