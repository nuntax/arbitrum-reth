//! Validates delayed-message reconstruction against Arbitrum One by replaying the
//! on-chain delayed-inbox accumulator chain.
//!
//! For a run of consecutive `MessageDelivered` events (Bridge 0x8315177a…), each
//! message's `keccak256(beforeAcc ‖ messageHash)` must equal the next message's
//! on-chain `beforeInboxAcc`. If the whole chain links, our `messageHash` byte
//! layout (`Messages.sol`) is exact — the load-bearing part of delayed decode.

use std::fs;

use alloy_primitives::{Address, B256, U256};
use arb_reth_derive::delayed::{accumulate, delayed_message_hash};

struct Md {
    index: u64,
    before_inbox_acc: B256,
    kind: u8,
    sender: Address,
    message_data_hash: B256,
    base_fee_l1: U256,
    timestamp: u64,
    l1_block: u64,
}

fn parse(m: &serde_json::Value) -> Md {
    let s = |k: &str| m[k].as_str().unwrap().to_string();
    Md {
        index: m["index"].as_u64().unwrap(),
        before_inbox_acc: s("before_inbox_acc").parse().unwrap(),
        kind: m["kind"].as_u64().unwrap() as u8,
        sender: s("sender").parse().unwrap(),
        message_data_hash: s("message_data_hash").parse().unwrap(),
        base_fee_l1: s("base_fee_l1").parse().unwrap(),
        timestamp: m["timestamp"].as_u64().unwrap(),
        l1_block: m["l1_block"].as_u64().unwrap(),
    }
}

#[test]
fn delayed_accumulator_chain_matches_arbitrum_one() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/arb1_delayed_messages.json");
    let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    assert!(msgs.len() >= 4, "need a few messages for a chain");

    let mut links = 0usize;
    for w in msgs.windows(2) {
        let m = parse(&w[0]);
        let next = parse(&w[1]);
        assert_eq!(next.index, m.index + 1, "events must be consecutive");

        let mh = delayed_message_hash(
            m.kind,
            m.sender,
            m.l1_block,
            m.timestamp,
            m.index,
            m.base_fee_l1,
            m.message_data_hash,
        );
        let acc = accumulate(m.before_inbox_acc, mh);
        assert_eq!(
            acc, next.before_inbox_acc,
            "accumulator chain broke at delayed index {}",
            m.index
        );
        links += 1;
    }
    println!("verified {links} on-chain delayed accumulator links (Arbitrum One)");
    assert!(links >= 3);
}
