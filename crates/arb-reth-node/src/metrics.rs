//! Metrics for the live sequencer-feed path.

use arb_reth_engine::ArbAppliedMessageTiming;
use reth_metrics::{
    Metrics,
    metrics::{Counter, Histogram},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

const MAX_TRACKED_MESSAGES: usize = 16_384;

/// End-to-end latency from receiving a WebSocket data frame to the corresponding block becoming
/// the canonical in-memory head.
#[derive(Metrics)]
#[metrics(scope = "arb_reth.feed")]
struct FeedLatencyMetrics {
    /// Time from receiving a sequencer-feed WebSocket frame to canonical in-memory state.
    frame_to_canonical_seconds: Histogram,
    /// WebSocket text/binary conversion and JSON decoding before a message is ready for the channel.
    frame_decode_seconds: Histogram,
    /// Channel send backpressure and time waiting in the driver input channel.
    channel_wait_seconds: Histogram,
    /// Time after driver dequeue before this sequence becomes eligible for in-order application.
    sequencing_wait_seconds: Histogram,
    /// ArbOS execution, state-root calculation, and block/header construction.
    block_production_seconds: Histogram,
    /// Parent-state provider setup before block production.
    block_parent_state_seconds: Histogram,
    /// Feed-message digesting and next-block environment construction.
    block_message_preparation_seconds: Histogram,
    /// Creation of revm's journaled state over the parent provider.
    block_state_setup_seconds: Histogram,
    /// ArbOS pre-execution and transaction execution.
    block_execution_seconds: Histogram,
    /// Block-builder creation, ArbOS pre-execution changes, and base-fee setup.
    block_execution_setup_seconds: Histogram,
    /// Construction of ArbOS's mandatory internal start-block transaction.
    block_start_block_transaction_construction_seconds: Histogram,
    /// Execution of ArbOS's mandatory internal start-block transaction.
    block_start_block_transaction_seconds: Histogram,
    /// Execution of derived user and retry transactions, including retry scheduling.
    block_derived_transactions_seconds: Histogram,
    /// Derived transaction execution and commit work, excluding retry scheduling.
    block_derived_transaction_execution_seconds: Histogram,
    /// Extraction and enqueueing of retries emitted by successful derived transactions.
    block_derived_retry_scheduling_seconds: Histogram,
    /// Remainder after named derived-transaction phases, retained for exact accounting.
    block_derived_transactions_unattributed_seconds: Histogram,
    /// Remainder after named block-execution phases, retained for exact accounting.
    block_execution_unattributed_seconds: Histogram,
    /// Total generic block finalization after ArbOS transactions complete.
    block_finish_seconds: Histogram,
    /// ArbOS executor finalization, principally reading post-execution header metadata.
    block_finish_executor_seconds: Histogram,
    /// Hashing the executed bundle into the post-state representation used by the trie.
    block_finish_hashed_state_seconds: Histogram,
    /// Computing the post-state root and trie updates.
    block_finish_state_root_seconds: Histogram,
    /// Transaction/receipt roots, logs bloom, and Arbitrum header/block assembly.
    block_finish_assembly_seconds: Histogram,
    /// Generic finalization work not assigned to one of the named phases.
    block_finish_unattributed_seconds: Histogram,
    /// Sending the executed block to reth's engine tree.
    engine_insert_seconds: Histogram,
    /// Forkchoice request and response through reth's engine tree.
    engine_forkchoice_seconds: Histogram,
    /// Waiting for the canonical in-memory provider state to observe the block.
    canonicalization_wait_seconds: Histogram,
    /// In-order apply-path work not covered by the named engine phases.
    engine_apply_overhead_seconds: Histogram,
    /// Samples that could not be tracked without blocking the feed or execution task.
    tracking_dropped_total: Counter,
}

struct FeedLatencyInner {
    messages: Mutex<BTreeMap<u64, FeedMessageTiming>>,
    metrics: OnceLock<FeedLatencyMetrics>,
}

#[derive(Clone, Copy)]
struct FeedMessageTiming {
    frame_received_at: Instant,
    ready_for_channel_at: Option<Instant>,
    driver_dequeued_at: Option<Instant>,
}

/// Correlates an inbound feed message with the point at which its block is canonical in reth's
/// shared in-memory state. Contention intentionally drops a sample instead of delaying either the
/// WebSocket reader or the engine driver.
#[derive(Clone)]
pub struct FeedLatencyTracker {
    inner: Arc<FeedLatencyInner>,
}

impl FeedLatencyTracker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(FeedLatencyInner {
                messages: Mutex::new(BTreeMap::new()),
                metrics: OnceLock::new(),
            }),
        }
    }

    /// Records the instant at which a WebSocket data frame was received, before parsing it.
    pub(crate) fn record_frame_arrival(&self, sequence_number: u64, received_at: Instant) {
        let mut messages = match self.inner.messages.try_lock() {
            Ok(messages) => messages,
            Err(_) => {
                self.metrics().tracking_dropped_total.increment(1);
                return;
            }
        };

        // Keep the first receipt for a sequence. A duplicated frame must not overwrite the
        // latency start of the message that was actually queued first.
        if messages.contains_key(&sequence_number) {
            return;
        }
        if messages.len() == MAX_TRACKED_MESSAGES {
            messages.pop_first();
            self.metrics().tracking_dropped_total.increment(1);
        }
        messages.insert(
            sequence_number,
            FeedMessageTiming {
                frame_received_at: received_at,
                ready_for_channel_at: None,
                driver_dequeued_at: None,
            },
        );
    }

    /// Records the instant after a WebSocket frame has been decoded and the message is ready to
    /// send through the driver channel.
    pub(crate) fn record_ready_for_channel(&self, sequence_number: u64, ready_at: Instant) {
        let mut messages = match self.inner.messages.try_lock() {
            Ok(messages) => messages,
            Err(_) => {
                self.metrics().tracking_dropped_total.increment(1);
                return;
            }
        };
        if let Some(timing) = messages.get_mut(&sequence_number) {
            timing.ready_for_channel_at = Some(ready_at);
        }
    }

    /// Records the instant at which the engine driver dequeues a message.
    pub(crate) fn record_driver_dequeue(&self, sequence_number: u64, dequeued_at: Instant) {
        let mut messages = match self.inner.messages.try_lock() {
            Ok(messages) => messages,
            Err(_) => {
                self.metrics().tracking_dropped_total.increment(1);
                return;
            }
        };
        if let Some(timing) = messages.get_mut(&sequence_number) {
            timing.driver_dequeued_at = Some(dequeued_at);
        }
    }

    /// Records the end of the measurement and each engine phase after reth has canonicalized the
    /// corresponding block.
    pub(crate) fn record_canonical(&self, sequence_number: u64, applied: ArbAppliedMessageTiming) {
        let timing = match self.inner.messages.try_lock() {
            Ok(mut messages) => messages.remove(&sequence_number),
            Err(_) => {
                self.metrics().tracking_dropped_total.increment(1);
                return;
            }
        };

        if let Some(timing) = timing {
            let metrics = self.metrics();
            metrics
                .frame_to_canonical_seconds
                .record(timing.frame_received_at.elapsed().as_secs_f64());
            if let Some(ready_at) = timing.ready_for_channel_at {
                metrics.frame_decode_seconds.record(
                    ready_at
                        .saturating_duration_since(timing.frame_received_at)
                        .as_secs_f64(),
                );
                if let Some(dequeued_at) = timing.driver_dequeued_at {
                    metrics.channel_wait_seconds.record(
                        dequeued_at
                            .saturating_duration_since(ready_at)
                            .as_secs_f64(),
                    );
                    metrics.sequencing_wait_seconds.record(
                        applied
                            .started_at
                            .saturating_duration_since(dequeued_at)
                            .as_secs_f64(),
                    );
                }
            }
            metrics
                .block_production_seconds
                .record(applied.block_production.as_secs_f64());
            metrics
                .block_parent_state_seconds
                .record(applied.block_parent_state.as_secs_f64());
            metrics
                .block_message_preparation_seconds
                .record(applied.block_message_preparation.as_secs_f64());
            metrics
                .block_state_setup_seconds
                .record(applied.block_state_setup.as_secs_f64());
            metrics
                .block_execution_seconds
                .record(applied.block_execution.as_secs_f64());
            metrics
                .block_execution_setup_seconds
                .record(applied.block_execution_setup.as_secs_f64());
            metrics
                .block_start_block_transaction_construction_seconds
                .record(
                    applied
                        .block_start_block_transaction_construction
                        .as_secs_f64(),
                );
            metrics
                .block_start_block_transaction_seconds
                .record(applied.block_start_block_transaction.as_secs_f64());
            metrics
                .block_derived_transactions_seconds
                .record(applied.block_derived_transactions.as_secs_f64());
            metrics
                .block_derived_transaction_execution_seconds
                .record(applied.block_derived_transaction_execution.as_secs_f64());
            metrics
                .block_derived_retry_scheduling_seconds
                .record(applied.block_derived_retry_scheduling.as_secs_f64());
            metrics
                .block_derived_transactions_unattributed_seconds
                .record(
                    applied
                        .block_derived_transactions_unattributed
                        .as_secs_f64(),
                );
            metrics
                .block_execution_unattributed_seconds
                .record(applied.block_execution_unattributed.as_secs_f64());
            metrics
                .block_finish_seconds
                .record(applied.block_finish.as_secs_f64());
            metrics
                .block_finish_executor_seconds
                .record(applied.block_finish_executor.as_secs_f64());
            metrics
                .block_finish_hashed_state_seconds
                .record(applied.block_finish_hashed_state.as_secs_f64());
            metrics
                .block_finish_state_root_seconds
                .record(applied.block_finish_state_root.as_secs_f64());
            metrics
                .block_finish_assembly_seconds
                .record(applied.block_finish_assembly.as_secs_f64());
            metrics
                .block_finish_unattributed_seconds
                .record(applied.block_finish_unattributed.as_secs_f64());
            metrics
                .engine_insert_seconds
                .record(applied.engine_insert.as_secs_f64());
            metrics
                .engine_forkchoice_seconds
                .record(applied.engine_forkchoice.as_secs_f64());
            metrics
                .canonicalization_wait_seconds
                .record(applied.canonicalization_wait.as_secs_f64());
            let named = applied.block_production
                + applied.engine_insert
                + applied.engine_forkchoice
                + applied.canonicalization_wait;
            metrics
                .engine_apply_overhead_seconds
                .record(applied.total.saturating_sub(named).as_secs_f64());
        }
    }

    fn metrics(&self) -> &FeedLatencyMetrics {
        // The live-feed task starts only after `with_prometheus_server` has installed reth's
        // recorder, so metric handles are never initialized against the no-op recorder.
        self.inner.metrics.get_or_init(FeedLatencyMetrics::default)
    }
}
