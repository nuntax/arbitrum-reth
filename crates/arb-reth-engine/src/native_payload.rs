//! Arbitrum's native Reth payload-builder integration.
//!
//! The engine tree owns construction of the optional sparse state-root task. This builder receives
//! that handle together with one ordered Arbitrum message, executes ArbOS exactly once over the
//! requested parent, and returns the resulting executed block to the engine tree.

use alloy_consensus::Header;
use alloy_primitives::{B256, U256};
use arb_reth_evm::ArbEvmConfig;
use reth_basic_payload_builder::{
    BuildArguments, BuildOutcome, MissingPayloadBehaviour, PayloadBuilder, PayloadConfig,
};
use reth_execution_cache::{CacheFillMode, CacheStats, CachedStateProvider};
use reth_payload_builder::{
    BuildNewPayload, KeepPayloadJobAlive, PayloadBuilderError, PayloadJob, PayloadJobGenerator,
};
use reth_payload_primitives::PayloadKind;
use reth_provider::StateProviderFactory;
use reth_revm::{cached::CachedReads, cancelled::CancelOnDrop};
use reth_storage_api::BlockReaderIdExt;
use reth_tasks::Runtime;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Instant,
};
use tokio::sync::oneshot;

use metrics::{Counter, Histogram};

use crate::{ArbBuiltPayload, ArbPayloadAttributes, engine::produce_with_timing};

/// Stable worker name for the serial ArbOS payload builder.
const ARB_PAYLOAD_BUILDER_THREAD: &str = "arb-payload-builder";

/// A serial payload builder for ordered ArbOS messages.
#[derive(Debug, Clone)]
pub struct ArbPayloadBuilder<P> {
    provider: P,
    evm_config: ArbEvmConfig,
    chain_id: u64,
}

impl<P> ArbPayloadBuilder<P> {
    /// Creates a payload builder over Reth's canonical/in-memory provider.
    pub const fn new(provider: P, evm_config: ArbEvmConfig, chain_id: u64) -> Self {
        Self {
            provider,
            evm_config,
            chain_id,
        }
    }

    fn build(
        &self,
        args: BuildArguments<ArbPayloadAttributes, ArbBuiltPayload>,
    ) -> Result<ArbBuiltPayload, PayloadBuilderError>
    where
        P: StateProviderFactory,
    {
        let parent = args.config.parent_header;
        let parent_hash = parent.hash();
        let supplied_cache = args.execution_cache;
        if let Some(cache) = supplied_cache
            .as_ref()
            .filter(|cache| cache.executed_block_hash() != parent_hash)
        {
            tracing::warn!(
                target: "arb-reth::engine",
                cache_parent = %cache.executed_block_hash(),
                %parent_hash,
                "ignoring execution cache for a different parent",
            );
        }
        let execution_cache =
            supplied_cache.filter(|cache| cache.executed_block_hash() == parent_hash);
        let cache_stats = execution_cache
            .as_ref()
            .map(|_| Arc::new(CacheStats::default()));
        if cache_stats.is_some() {
            // Resolve recorder handles before measured provider and execution work begins.
            let _ = execution_cache_metric_handles();
        }

        let started_at = Instant::now();
        let mut execution_state = self
            .provider
            .state_by_block_hash(parent_hash)
            .map_err(PayloadBuilderError::other)?;
        let trie_state = self
            .provider
            .state_by_block_hash(parent_hash)
            .map_err(PayloadBuilderError::other)?;
        let parent_state = started_at.elapsed();

        if let (Some(cache), Some(stats)) = (execution_cache, cache_stats.as_ref()) {
            execution_state = Box::new(CachedStateProvider::new_with_mode(
                execution_state,
                cache.cache().clone(),
                // Unlike Ethereum's builder, this serial ArbOS path has no separate prewarmer.
                // Populate misses so unchanged ArbOS slots and bytecode remain useful across
                // blocks; the tree updates the same parent-bound cache after every insertion.
                CacheFillMode::FillOnMiss,
                None,
                Some(Arc::clone(stats)),
            ));
        }

        let (executed, mut timing) = produce_with_timing(
            &self.evm_config,
            self.chain_id,
            &parent,
            &args.config.attributes.message,
            execution_state,
            trie_state,
            args.state_root_handle,
        )
        .map_err(|err| PayloadBuilderError::other(std::io::Error::other(err.to_string())))?;
        timing.parent_state = parent_state;
        timing.total = started_at.elapsed();

        Ok(ArbBuiltPayload::from_executed(
            executed,
            U256::ZERO,
            timing,
            cache_stats,
        ))
    }
}

/// A one-shot payload-job generator for ordered ArbOS messages.
///
/// Arbitrum has no mempool selection, replacement payloads, slots, or useful empty payload. A
/// request therefore starts exactly one ArbOS execution and resolves only with that result. The
/// engine still creates and owns the optional sparse state-root task; this generator merely passes
/// its handle into [`ArbPayloadBuilder`].
pub(crate) struct ArbPayloadJobGenerator<P> {
    provider: P,
    runtime: Runtime,
    builder: ArbPayloadBuilder<P>,
}

impl<P> ArbPayloadJobGenerator<P> {
    pub(crate) const fn new(provider: P, runtime: Runtime, builder: ArbPayloadBuilder<P>) -> Self {
        Self {
            provider,
            runtime,
            builder,
        }
    }
}

impl<P> PayloadJobGenerator for ArbPayloadJobGenerator<P>
where
    P: StateProviderFactory
        + BlockReaderIdExt<Header = Header>
        + Clone
        + Send
        + Sync
        + Unpin
        + 'static,
{
    type Job = ArbPayloadJob;

    fn new_payload_job(
        &self,
        input: BuildNewPayload<ArbPayloadAttributes>,
        id: alloy_rpc_types_engine::PayloadId,
    ) -> Result<Self::Job, PayloadBuilderError> {
        let parent_header = if input.parent_hash.is_zero() {
            self.provider
                .latest_header()?
                .ok_or(PayloadBuilderError::MissingParentHeader(B256::ZERO))?
        } else {
            self.provider
                .sealed_header_by_hash(input.parent_hash)?
                .ok_or(PayloadBuilderError::MissingParentHeader(input.parent_hash))?
        };
        let attributes = input.attributes;
        let config = PayloadConfig::new(Arc::new(parent_header), attributes.clone(), id);
        let (tx, rx) = oneshot::channel();
        let cancel = CancelOnDrop::default();
        let pending_cancel = cancel.clone();
        let builder = self.builder.clone();

        // `spawn_blocking_named_or_tokio` schedules the CPU work immediately on the stable worker
        // when it is available. This removes the generic builder's async permit, interval, and
        // deadline machinery without forcing this serial producer to wait for an unrelated job.
        self.runtime
            .spawn_blocking_named_or_tokio(ARB_PAYLOAD_BUILDER_THREAD, move || {
                let args = BuildArguments {
                    cached_reads: CachedReads::default(),
                    execution_cache: input.cache,
                    state_root_handle: input.state_root_handle,
                    config,
                    cancel,
                    best_payload: None,
                };
                let _ = tx.send(builder.build(args));
            });

        Ok(ArbPayloadJob {
            attributes,
            pending: Some(ArbPendingPayload {
                // Keep the cancellation signal alive until resolve transfers it to the caller's
                // future. Dropping that future correctly cancels unfinished builder work.
                _cancel: pending_cancel,
                payload: rx,
            }),
        })
    }
}

/// The single build started for an ArbOS payload request.
struct ArbPendingPayload {
    _cancel: CancelOnDrop,
    payload: oneshot::Receiver<Result<ArbBuiltPayload, PayloadBuilderError>>,
}

/// A deterministic payload job. It deliberately remains pending until the driver resolves it.
pub(crate) struct ArbPayloadJob {
    attributes: ArbPayloadAttributes,
    pending: Option<ArbPendingPayload>,
}

impl Future for ArbPayloadJob {
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The driver resolves every job immediately after its attributes FCU. Keeping the job in
        // the service until then preserves its cancellation guard and avoids generic rebuilds.
        Poll::Pending
    }
}

impl PayloadJob for ArbPayloadJob {
    type PayloadAttributes = ArbPayloadAttributes;
    type ResolvePayloadFuture = ArbPayloadResolve;
    type BuiltPayload = ArbBuiltPayload;

    fn best_payload(&self) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        // There is no valid ArbOS empty payload. Consumers must wait for the deterministic build.
        Err(PayloadBuilderError::MissingPayload)
    }

    fn payload_attributes(&self) -> Result<Self::PayloadAttributes, PayloadBuilderError> {
        Ok(self.attributes.clone())
    }

    fn payload_timestamp(&self) -> Result<u64, PayloadBuilderError> {
        Ok(self.attributes.timestamp)
    }

    fn resolve_kind(
        &mut self,
        _kind: PayloadKind,
    ) -> (Self::ResolvePayloadFuture, KeepPayloadJobAlive) {
        // `Earliest` and `WaitForPending` are equivalent for an ArbOS message: returning before
        // the one required execution finishes would create an invalid block.
        (
            ArbPayloadResolve {
                pending: self.pending.take(),
            },
            KeepPayloadJobAlive::No,
        )
    }
}

/// Future returned when the driver asks for the one valid ArbOS payload.
pub(crate) struct ArbPayloadResolve {
    pending: Option<ArbPendingPayload>,
}

impl Future for ArbPayloadResolve {
    type Output = Result<ArbBuiltPayload, PayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Err(PayloadBuilderError::MissingPayload));
        };

        match Pin::new(&mut pending.payload).poll(cx) {
            Poll::Ready(result) => {
                self.pending = None;
                Poll::Ready(result.map_err(Into::into).and_then(|result| result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct CacheAccessMetricHandles {
    hit_counter: Counter,
    miss_counter: Counter,
    hit_histogram: Histogram,
    miss_histogram: Histogram,
}

impl CacheAccessMetricHandles {
    fn new(kind: &'static str) -> Self {
        Self {
            hit_counter: metrics::counter!(
                "arb_reth.execution_cache_access_total",
                "kind" => kind,
                "result" => "hit",
            ),
            miss_counter: metrics::counter!(
                "arb_reth.execution_cache_access_total",
                "kind" => kind,
                "result" => "miss",
            ),
            hit_histogram: metrics::histogram!(
                "arb_reth.execution_cache_accesses_per_block",
                "kind" => kind,
                "result" => "hit",
            ),
            miss_histogram: metrics::histogram!(
                "arb_reth.execution_cache_accesses_per_block",
                "kind" => kind,
                "result" => "miss",
            ),
        }
    }
}

struct ExecutionCacheMetricHandles {
    account: CacheAccessMetricHandles,
    storage: CacheAccessMetricHandles,
    bytecode: CacheAccessMetricHandles,
}

fn execution_cache_metric_handles() -> &'static ExecutionCacheMetricHandles {
    static HANDLES: OnceLock<ExecutionCacheMetricHandles> = OnceLock::new();
    HANDLES.get_or_init(|| ExecutionCacheMetricHandles {
        account: CacheAccessMetricHandles::new("account"),
        storage: CacheAccessMetricHandles::new("storage"),
        bytecode: CacheAccessMetricHandles::new("bytecode"),
    })
}

/// Flush one block's cache statistics after the measured production interval has ended.
pub(crate) fn record_execution_cache_stats(stats: &CacheStats) {
    let handles = execution_cache_metric_handles();
    for (handles, hits, misses) in [
        (
            &handles.account,
            stats.account_hits(),
            stats.account_misses(),
        ),
        (
            &handles.storage,
            stats.storage_hits(),
            stats.storage_misses(),
        ),
        (&handles.bytecode, stats.code_hits(), stats.code_misses()),
    ] {
        handles.hit_counter.increment(hits as u64);
        handles.miss_counter.increment(misses as u64);
        handles.hit_histogram.record(hits as f64);
        handles.miss_histogram.record(misses as f64);
    }
}

impl<P> PayloadBuilder for ArbPayloadBuilder<P>
where
    P: StateProviderFactory + Clone + Send + Sync + 'static,
{
    type Attributes = ArbPayloadAttributes;
    type BuiltPayload = ArbBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> Result<BuildOutcome<Self::BuiltPayload>, PayloadBuilderError> {
        let payload = self.build(args)?;

        // An Arbitrum message deterministically defines one block. There is no transaction-pool
        // competition or later improvement cycle, so freeze the first completed result.
        Ok(BuildOutcome::Freeze(payload))
    }

    fn on_missing_payload(
        &self,
        _args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> MissingPayloadBehaviour<Self::BuiltPayload> {
        // The only valid payload for an Arbitrum message is the one being executed by this job.
        // Resolving immediately after FCU must wait for it instead of racing an empty block.
        MissingPayloadBehaviour::AwaitInProgress
    }

    fn build_empty_payload(
        &self,
        _config: PayloadConfig<Self::Attributes>,
    ) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        Err(PayloadBuilderError::MissingPayload)
    }
}
