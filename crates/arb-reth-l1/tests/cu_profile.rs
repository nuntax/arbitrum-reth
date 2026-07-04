//! CU profiler: drive several consecutive L1 windows through the real derive path
//! (`resolve_batches` + `derive_from_resolved`) exactly as `run_l1_sync` does, with a
//! transport-level method counter so we can see the true per-method RPC mix, including
//! the cross-window re-fetch waste that only shows up over consecutive windows.
//!
//! Run (calldata era, ~pre-Dencun):
//!   ARB_L1_RPC=<url> CU_START_BLOCK=19000000 CU_WINDOWS=20 \
//!     cargo test -p arb-reth-l1 --test cu_profile -- --ignored --nocapture
//! Run (blob era, post-Dencun) additionally set ARB_L1_BEACON=<beacon-url> and a
//! CU_START_BLOCK >= 19426587.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use alloy_json_rpc::{RequestPacket, ResponsePacket};
use alloy_provider::ProviderBuilder;
use alloy_transport::{TransportError, TransportFut};
use tower::{Layer, Service, ServiceExt};

use arb_reth_l1::sync::{
    derive_from_resolved, derive_from_resolved_cached, resolve_batches, DelayedCache,
    ReportStatsCache, DEFAULT_DELAYED_WINDOW,
};
use arb_reth_l1::{BeaconClient, DelayedInboxReader, SequencerInboxReader};

type Counts = Arc<Mutex<HashMap<String, u64>>>;

/// Global next-allowed-send instant, so concurrent calls serialize under the CUPS cap.
type Gate = Arc<tokio::sync::Mutex<tokio::time::Instant>>;

#[derive(Clone)]
struct CountingLayer {
    counts: Counts,
    gate: Gate,
    throttle: std::time::Duration,
}

impl<S> Layer<S> for CountingLayer {
    type Service = CountingService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CountingService {
            inner,
            counts: self.counts.clone(),
            gate: self.gate.clone(),
            throttle: self.throttle,
        }
    }
}

#[derive(Clone)]
struct CountingService<S> {
    inner: S,
    counts: Counts,
    gate: Gate,
    throttle: std::time::Duration,
}

impl<S> Service<RequestPacket> for CountingService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        // Count the logical request once (retries are transport overhead, not algorithm
        // calls, and a 429-rejected retry consumes no CU, so once is the right CU accounting).
        {
            let mut c = self.counts.lock().unwrap();
            match &req {
                RequestPacket::Single(r) => *c.entry(r.method().to_string()).or_default() += 1,
                RequestPacket::Batch(v) => {
                    for r in v {
                        *c.entry(r.method().to_string()).or_default() += 1;
                    }
                }
            }
        }
        let inner = self.inner.clone();
        let gate = self.gate.clone();
        let throttle = self.throttle;
        Box::pin(async move {
            // Proactively pace: claim the next send slot `throttle` after the previous one,
            // so total request rate stays under the free-tier CUPS cap and we don't 429.
            {
                let mut next = gate.lock().await;
                let now = tokio::time::Instant::now();
                let at = (*next).max(now);
                tokio::time::sleep_until(at).await;
                *next = at + throttle;
            }
            inner.oneshot(req).await
        })
    }
}

/// Rough per-method CU weights (Alchemy legacy CU) so the mix can be read as a CU share.
/// drpc/other providers weight differently, but the relative mix is what we optimize.
fn cu_weight(method: &str) -> u64 {
    match method {
        "eth_getLogs" => 75,
        "eth_getTransactionByHash" => 17,
        "eth_getBlockByNumber" => 16,
        "eth_getBlockByHash" => 21,
        "eth_blockNumber" => 10,
        "eth_chainId" => 0,
        _ => 20,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "hits a live L1 RPC; set ARB_L1_RPC + CU_START_BLOCK and run with --ignored --nocapture"]
async fn profile_cu_over_consecutive_windows() {
    let Ok(url) = std::env::var("ARB_L1_RPC") else {
        eprintln!("ARB_L1_RPC unset; skipping");
        return;
    };
    let start: u64 = std::env::var("CU_START_BLOCK")
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("set CU_START_BLOCK");
    let windows: u64 =
        std::env::var("CU_WINDOWS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let window: u64 =
        std::env::var("CU_WINDOW").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000);
    // Delayed backward-scan window. Production default is DEFAULT_DELAYED_WINDOW (10k), but
    // free-tier RPCs reject wide getLogs, so allow overriding it down for measurement.
    let delayed_window: u64 = std::env::var("CU_DELAYED_WINDOW")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DELAYED_WINDOW);
    let beacon_url = std::env::var("ARB_L1_BEACON").ok();

    let throttle_ms: u64 =
        std::env::var("CU_THROTTLE_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(220);
    let counts: Counts = Arc::new(Mutex::new(HashMap::new()));
    let gate: Gate = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));
    let client = alloy_rpc_client::ClientBuilder::default()
        .layer(CountingLayer {
            counts: counts.clone(),
            gate,
            throttle: std::time::Duration::from_millis(throttle_ms),
        })
        .http(url.parse().expect("parse ARB_L1_RPC"));
    let provider = ProviderBuilder::new().connect_client(client);

    let seq_reader = SequencerInboxReader::mainnet(provider.clone());
    let delayed_reader = DelayedInboxReader::mainnet(provider.clone());
    let beacon = beacon_url.map(BeaconClient::new);

    // Bootstrap the delayed cursor: the batch just before `start` sets it. Slide a fixed
    // `window`-wide getLogs backward (free tiers cap the getLogs span) until a batch shows.
    let mut delayed = 0u64;
    let mut hi = start - 1;
    for _ in 0..40 {
        let lo = hi.saturating_sub(window - 1);
        let prior = resolve_batches(&seq_reader, beacon.as_ref(), lo, hi)
            .await
            .expect("bootstrap resolve_batches");
        if let Some((b, _)) = prior.last() {
            delayed = b.event.after_delayed_messages_read;
            break;
        }
        if lo == 0 {
            break;
        }
        hi = lo - 1;
    }

    // Reset counts so the bootstrap doesn't pollute the measured run.
    counts.lock().unwrap().clear();

    // CU_CACHED=0 measures the old per-window path (fresh caches); default uses the
    // forward-carried caches threaded across windows.
    let cached = std::env::var("CU_CACHED").ok().as_deref() != Some("0");
    // CU_VERIFY=1: also run the reference (fresh-cache) path per window and assert the derived
    // output is byte-identical, proving the cache change preserves parity on real chain data.
    let verify = std::env::var("CU_VERIFY").ok().as_deref() == Some("1");
    let mut delayed_cache = DelayedCache::new();
    let mut report_cache = ReportStatsCache::new();

    let mut cursor = start;
    let mut total_batches = 0usize;
    let mut total_msgs = 0usize;
    let mut delayed_scan_windows = 0u64;

    for _ in 0..windows {
        let to = cursor + window - 1;
        let resolved = resolve_batches(&seq_reader, beacon.as_ref(), cursor, to)
            .await
            .expect("resolve_batches");
        let before = delayed;
        let reference = if verify {
            Some(
                derive_from_resolved(
                    &seq_reader,
                    &delayed_reader,
                    resolved.clone(),
                    to,
                    delayed,
                    delayed_window,
                )
                .await
                .expect("reference derive_from_resolved"),
            )
        } else {
            None
        };
        let derived = if cached {
            derive_from_resolved_cached(
                &seq_reader,
                &delayed_reader,
                resolved,
                to,
                delayed,
                delayed_window,
                &mut delayed_cache,
                &mut report_cache,
            )
            .await
            .expect("derive_from_resolved_cached")
        } else {
            derive_from_resolved(&seq_reader, &delayed_reader, resolved, to, delayed, delayed_window)
                .await
                .expect("derive_from_resolved")
        };
        if let Some(reference) = reference {
            assert_eq!(
                derived.next_delayed_count, reference.next_delayed_count,
                "next_delayed_count mismatch at window ending {to}"
            );
            assert_eq!(derived.batches, reference.batches, "batch count mismatch at window {to}");
            assert_eq!(
                format!("{:?}", derived.messages),
                format!("{:?}", reference.messages),
                "derived messages differ at window ending {to}"
            );
        }
        if derived.next_delayed_count > before {
            delayed_scan_windows += 1;
        }
        total_batches += derived.batches;
        total_msgs += derived.messages.len();
        delayed = derived.next_delayed_count;
        cursor = to + 1;
    }

    let c = counts.lock().unwrap();
    let mut rows: Vec<(String, u64)> = c.iter().map(|(k, v)| (k.clone(), *v)).collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    let total_calls: u64 = rows.iter().map(|(_, n)| n).sum();
    let total_cu: u64 = rows.iter().map(|(m, n)| n * cu_weight(m)).sum();

    eprintln!("\n===== CU PROFILE =====");
    eprintln!(
        "range L1 [{start}, {}]  windows={windows} window={window}  cached={cached}  beacon={}",
        cursor - 1,
        beacon.is_some()
    );
    eprintln!(
        "batches={total_batches}  msgs(blocks)={total_msgs}  windows_with_delayed_scan={delayed_scan_windows}"
    );
    eprintln!("--- per method (count, est CU, %CU) ---");
    for (m, n) in &rows {
        let cu = n * cu_weight(m);
        let pct = if total_cu > 0 { 100.0 * cu as f64 / total_cu as f64 } else { 0.0 };
        eprintln!("{m:38} {n:>6}   {cu:>8} CU   {pct:5.1}%");
    }
    eprintln!("--- totals ---");
    eprintln!("calls={total_calls}  est_CU={total_cu}");
    if total_msgs > 0 {
        eprintln!("est_CU/block = {:.3}", total_cu as f64 / total_msgs as f64);
    }
    eprintln!("======================\n");
}
