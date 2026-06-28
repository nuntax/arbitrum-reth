//! Live delayed-inbox reconstruction against Arbitrum One.
//!
//! `fetch_delayed` over a real L1 range pairs each `MessageDelivered` with its body
//! and verifies `keccak256(body) == messageDataHash` internally; this test further
//! asserts the on-chain accumulator chains across the recovered run. Requires
//! `ARB_L1_RPC`; run with `-- --ignored`.

// A block window on L1 mainnet holding a consecutive delayed run (indices
// 2486839..=2486843 on Arbitrum One, captured 2026-06-28).
const FROM: u64 = 25_416_069;
const TO: u64 = 25_416_095;

#[tokio::test]
#[ignore = "hits a live archive L1 RPC; set ARB_L1_RPC and run with --ignored"]
async fn live_fetch_delayed_chain() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };

    use alloy_provider::ProviderBuilder;
    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let reader = arb_reth_l1::DelayedInboxReader::mainnet(provider);

    let msgs = reader.fetch_delayed(FROM, TO).await.expect("fetch_delayed");
    assert!(msgs.len() >= 3, "expected a few delayed messages, got {}", msgs.len());

    // Ascending, consecutive indices.
    for w in msgs.windows(2) {
        assert_eq!(w[1].inbox_seq_num, w[0].inbox_seq_num + 1, "indices not consecutive");
    }

    // The load-bearing check: bodies verified (inside fetch_delayed) AND the on-chain
    // accumulator links across the whole recovered run.
    assert!(
        arb_reth_l1::verify_accumulator_chain(&msgs),
        "on-chain delayed accumulator chain did not link"
    );

    let last = msgs.last().unwrap();
    println!(
        "recovered delayed {}..={} ({} msgs); last kind={} sender={}",
        msgs[0].inbox_seq_num,
        last.inbox_seq_num,
        msgs.len(),
        last.kind,
        last.sender,
    );
}
