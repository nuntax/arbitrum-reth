//! Stage F.3c.2: the L1-derivation catch-up runtime.
//!
//! Drives trustless sync. Walks L1 block ranges with [`arb_reth_l1::sync::derive_range`]
//! (SequencerInbox batches + delayed inbox + blob sidecars), and pushes each derived
//! [`BroadcastFeedMessage`] into the same feed channel the [`ArbChainDriver`] drains, so
//! derived blocks execute through the exact path validated against the testnode (Stage
//! G). It follows the L1 head once caught up.
//!
//! [`ArbChainDriver`]: crate::driver::ArbChainDriver
//!
//! ## Resuming
//!
//! `start_block` / `start_delayed_count` must be the L1 block and `delayedMessagesRead`
//! the L2 tip was built from. After a Stage-H snapshot import the tip's
//! `delayedMessagesRead` lives in its header nonce, and the L1 block is the batch
//! boundary that produced it; both must be supplied by the caller.
//!
//! ## Known limitation
//!
//! `version` is fixed for the whole run. Arbitrum One spans several ArbOS upgrades, so a
//! production catch-up across an upgrade boundary needs the version to advance per block
//! (derivable from the parent header's encoded ArbOS version). Tracked separately.

use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use arb_reth_l1::sync::{derive_range, DEFAULT_DELAYED_WINDOW};
use arb_reth_l1::{BeaconClient, DelayedInboxReader, SequencerInboxReader};
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::{eyre, Context as _};
use tokio::sync::mpsc::Sender;

/// Configuration for the L1-derivation catch-up runtime.
#[derive(Debug, Clone)]
pub struct L1SyncConfig {
    /// L1 execution-layer JSON-RPC endpoint (archive, with historical `getLogs`).
    pub l1_rpc: String,
    /// L1 beacon (consensus-layer) REST endpoint for blob sidecars. Required to derive
    /// post-Dencun blob batches; calldata-era ranges work without it.
    pub l1_beacon: Option<String>,
    /// `SequencerInbox` contract address.
    pub sequencer_inbox: Address,
    /// `Bridge` contract address (delayed-inbox metadata source).
    pub bridge: Address,
    /// First L1 block to derive from (the resume point's batch boundary).
    pub start_block: u64,
    /// Last L1 block to derive (inclusive). `None` follows the head indefinitely.
    pub end_block: Option<u64>,
    /// Delayed cursor before `start_block` (the L2 tip's `delayedMessagesRead`).
    pub start_delayed_count: u64,
    /// L1 blocks per `derive_range` call (bounds `getLogs` range per request).
    pub batch_window: u64,
    /// Backward-scan window for delayed-message coverage.
    pub delayed_window: u64,
    /// Stay this many blocks behind the L1 head (reorg safety margin).
    pub confirmations: u64,
    /// Poll interval when caught up to the safe head.
    pub poll_interval: Duration,
}

impl L1SyncConfig {
    /// Mainnet defaults: Arbitrum One `SequencerInbox`/`Bridge`, 1k-block windows, 8
    /// confirmations, 12s polling. `l1_rpc` and the resume point must still be set.
    pub fn mainnet(l1_rpc: String, start_block: u64, start_delayed_count: u64) -> Self {
        Self {
            l1_rpc,
            l1_beacon: None,
            sequencer_inbox: arb_reth_l1::SEQUENCER_INBOX_MAINNET,
            bridge: arb_reth_l1::BRIDGE_MAINNET,
            start_block,
            end_block: None,
            start_delayed_count,
            batch_window: 1_000,
            delayed_window: DEFAULT_DELAYED_WINDOW,
            confirmations: 8,
            poll_interval: Duration::from_secs(12),
        }
    }
}

/// Run the catch-up runtime: derive L1 ranges and push feed messages into `feed_tx`
/// until `end_block` is reached (or forever, following the head, when `end_block` is
/// `None`). Returns when the range is exhausted or the channel closes.
pub async fn run_l1_sync(
    cfg: L1SyncConfig,
    feed_tx: Sender<BroadcastFeedMessage>,
) -> eyre::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_http(cfg.l1_rpc.parse().wrap_err("invalid --l1-rpc URL")?);
    let seq_reader = SequencerInboxReader::new(provider.clone(), cfg.sequencer_inbox);
    let delayed_reader = DelayedInboxReader::new(provider.clone(), cfg.bridge);
    let beacon = cfg.l1_beacon.as_ref().map(|u| BeaconClient::new(u.clone()));

    let mut cursor = cfg.start_block;
    let mut delayed = cfg.start_delayed_count;

    loop {
        if cfg.end_block.is_some_and(|end| cursor > end) {
            break;
        }

        let head = provider
            .get_block_number()
            .await
            .map_err(|e| eyre!("L1 get_block_number: {e}"))?;
        let safe_head = head.saturating_sub(cfg.confirmations);

        // Nothing safe to derive yet: wait for the chain to advance past the
        // confirmation margin (a bounded run also respects it for reorg safety).
        if cursor > safe_head {
            tokio::time::sleep(cfg.poll_interval).await;
            continue;
        }

        let mut to = (cursor + cfg.batch_window - 1).min(safe_head);
        if let Some(end) = cfg.end_block {
            to = to.min(end);
        }

        let derived = derive_range(
            &seq_reader,
            &delayed_reader,
            beacon.as_ref(),
            cursor,
            to,
            delayed,
            cfg.delayed_window,
        )
        .await
        .wrap_err_with(|| format!("derive_range [{cursor}, {to}]"))?;

        if derived.batches > 0 {
            tracing::info!(
                target: "arb-reth::l1-sync",
                from = cursor, to, batches = derived.batches,
                messages = derived.messages.len(), next_delayed = derived.next_delayed_count,
                "derived L1 range",
            );
        }

        for msg in derived.messages {
            if feed_tx.send(msg).await.is_err() {
                tracing::warn!(target: "arb-reth::l1-sync", "feed channel closed; stopping L1 sync");
                return Ok(());
            }
        }

        delayed = derived.next_delayed_count;
        cursor = to + 1;
    }

    tracing::info!(target: "arb-reth::l1-sync", final_block = cursor - 1, "L1 sync reached end block");
    Ok(())
}
