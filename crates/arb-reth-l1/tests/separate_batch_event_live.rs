//! Live check of the `SeparateBatchEvent` (`dataLocation == 1`) resolve path plus the
//! empty-batch handling, against Arbitrum One batch 0.
//!
//! Batch 0 is the chain's only `SeparateBatchEvent` batch: its payload lives in a
//! `SequencerBatchData` event (not the posting tx calldata), and on mainnet that payload
//! is empty. Before the fix, `resolve_batches` errored `UnsupportedDataLocation(1)` and
//! (had it resolved) `derive_range` would error `Batch(Truncated)` on the empty payload.
//! After the fix, resolving must succeed with an empty payload and deriving must yield an
//! empty sequencer message (zero L2 messages), matching Nitro `ParseSequencerMessage`.
//!
//! Requires `ARB_L1_RPC` (archive `getLogs`); run with `-- --ignored`.

use arb_reth_derive::batch::data_location;
use arb_reth_l1::sync::{derive_range, resolve_batches, DEFAULT_DELAYED_WINDOW};

/// L1 block of Arbitrum One batch 0 (the `SequencerInbox` deploy block).
const BATCH0_BLOCK: u64 = 15_411_056;

#[tokio::test]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC and run with --ignored"]
async fn batch0_separate_event_derives_empty() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };

    use alloy_provider::ProviderBuilder;
    let provider = ProviderBuilder::new().connect_http(url.parse().expect("parse ARB_L1_RPC"));
    let seq = arb_reth_l1::SequencerInboxReader::mainnet(provider.clone());
    let delayed = arb_reth_l1::DelayedInboxReader::mainnet(provider);

    // resolve_batches must handle dataLocation=1 by reading the SequencerBatchData event
    // (previously errored UnsupportedDataLocation before the separate-event path landed).
    let resolved =
        resolve_batches(&seq, None, BATCH0_BLOCK, BATCH0_BLOCK).await.expect("resolve batch 0");
    assert_eq!(resolved.len(), 1, "block 15411056 holds exactly batch 0");
    let (batch, payload) = &resolved[0];
    assert_eq!(batch.sequence_number, 0);
    assert_eq!(
        batch.event.data_location,
        data_location::SEPARATE_BATCH_EVENT,
        "batch 0 is a SeparateBatchEvent batch",
    );
    assert!(payload.is_empty(), "batch 0's SequencerBatchData payload is empty on mainnet");

    // derive_range must treat the empty payload as an empty sequencer message (0 messages),
    // not error Batch(Truncated). start_delayed = afterDelayedMessagesRead at genesis = 1,
    // and batch 0 reads no further delayed messages, so the cursor is unchanged.
    let derived =
        derive_range(&seq, &delayed, None, BATCH0_BLOCK, BATCH0_BLOCK, 1, DEFAULT_DELAYED_WINDOW)
            .await
            .expect("derive batch 0");
    assert_eq!(derived.batches, 1, "exactly one batch in the range");
    assert_eq!(derived.messages.len(), 0, "an empty batch yields no L2 messages");
    assert_eq!(derived.next_delayed_count, 1, "batch 0 reads no new delayed messages");
}
