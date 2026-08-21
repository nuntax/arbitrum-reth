//! Concurrent sequencer-feed ingestion.
//!
//! Every configured WebSocket races to deliver the next sequence. A single coordinator forwards
//! only the first decoded copy to the engine, keeping duplicate work off the latency-sensitive
//! execution channel.

use crate::metrics::FeedLatencyTracker;
use arbitrum_alloy_sequencer::sequencer::feed::{BroadcastFeedMessage, Root};
use eyre::{Result, ensure, eyre};
use metrics::{Counter, Gauge, Histogram};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        Error as WebSocketError, Message,
        client::IntoClientRequest,
        handshake::client::Response,
        http::{HeaderValue, Request, StatusCode},
    },
};

const FEED_CLIENT_VERSION_HEADER: &str = "arbitrum-feed-client-version";
const REQUESTED_SEQUENCE_HEADER: &str = "arbitrum-requested-sequence-number";
const FEED_CLIENT_VERSION: &str = "2";
const MAX_FEED_SOURCES: usize = 64;
const MAX_RECENT_SEQUENCES: usize = 16_384;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(8);
const INITIAL_RATE_LIMIT_DELAY: Duration = Duration::from_secs(30);
const MAX_RATE_LIMIT_DELAY: Duration = Duration::from_secs(300);

/// Counted source-address declaration accepted by `--feed-source IP=COUNT`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FeedSourceSpec {
    pub(crate) local_ip: IpAddr,
    pub(crate) connections: usize,
}

impl FromStr for FeedSourceSpec {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (ip, count) = raw
            .rsplit_once('=')
            .ok_or_else(|| "expected IP=COUNT".to_string())?;
        let local_ip = ip
            .parse()
            .map_err(|err| format!("invalid source IP {ip:?}: {err}"))?;
        let connections = count
            .parse::<usize>()
            .map_err(|err| format!("invalid connection count {count:?}: {err}"))?;
        if connections == 0 {
            return Err("source connection count must be at least 1".to_string());
        }
        Ok(Self {
            local_ip,
            connections,
        })
    }
}

/// One independently raced WebSocket connection.
#[derive(Clone)]
pub(crate) struct FeedSource {
    ordinal: usize,
    endpoint: usize,
    connection: usize,
    url: String,
    display_endpoint: String,
    local_ip: Option<IpAddr>,
}

impl fmt::Debug for FeedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeedSource")
            .field("endpoint", &self.endpoint)
            .field("connection", &self.connection)
            .field("display_endpoint", &self.display_endpoint)
            .field("local_ip", &self.local_ip)
            .finish_non_exhaustive()
    }
}

/// Validate the configured endpoints and expand each into independently raced connections.
pub(crate) fn expand_feed_sources(
    urls: &[String],
    unbound_connections_per_endpoint: Option<usize>,
    source_specs: &[FeedSourceSpec],
) -> Result<Vec<FeedSource>> {
    ensure!(
        !urls.is_empty() || (unbound_connections_per_endpoint.is_none() && source_specs.is_empty()),
        "feed connections or sources require at least one --feed-url"
    );
    if urls.is_empty() {
        return Ok(Vec::new());
    }

    let unbound_connections =
        unbound_connections_per_endpoint.unwrap_or(usize::from(source_specs.is_empty()));
    let mut declared_ips = BTreeSet::new();
    let mut bound_connections = 0usize;
    for spec in source_specs {
        ensure!(
            declared_ips.insert(spec.local_ip),
            "--feed-source declares {} more than once; combine its counts",
            spec.local_ip
        );
        bound_connections = bound_connections
            .checked_add(spec.connections)
            .ok_or_else(|| eyre!("sequencer feed connection count overflow"))?;
    }
    let connections_per_endpoint = unbound_connections
        .checked_add(bound_connections)
        .ok_or_else(|| eyre!("sequencer feed connection count overflow"))?;
    ensure!(
        connections_per_endpoint > 0,
        "each --feed-url needs at least one unbound or source-bound connection"
    );
    let total = urls
        .len()
        .checked_mul(connections_per_endpoint)
        .ok_or_else(|| eyre!("sequencer feed connection count overflow"))?;
    ensure!(
        total <= MAX_FEED_SOURCES,
        "at most {MAX_FEED_SOURCES} sequencer feed connections are supported; configured {total}"
    );

    let mut sources = Vec::with_capacity(total);
    for (endpoint, raw_url) in urls.iter().enumerate() {
        let parsed = url::Url::parse(raw_url)
            .map_err(|err| eyre!("invalid --feed-url for endpoint {endpoint}: {err}"))?;
        ensure!(
            matches!(parsed.scheme(), "ws" | "wss") && parsed.host_str().is_some(),
            "--feed-url endpoint {endpoint} must be an absolute ws:// or wss:// URL"
        );

        // Avoid putting credentials, paths, or query strings in logs and metric labels.
        let host = parsed.host_str().expect("host checked above");
        let display_endpoint = match parsed.port() {
            Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
            None => format!("{}://{host}", parsed.scheme()),
        };
        let mut connection = 0usize;
        for _ in 0..unbound_connections {
            sources.push(FeedSource {
                ordinal: sources.len(),
                endpoint,
                connection,
                url: raw_url.clone(),
                display_endpoint: display_endpoint.clone(),
                local_ip: None,
            });
            connection = connection.saturating_add(1);
        }
        for spec in source_specs {
            for _ in 0..spec.connections {
                sources.push(FeedSource {
                    ordinal: sources.len(),
                    endpoint,
                    connection,
                    url: raw_url.clone(),
                    display_endpoint: display_endpoint.clone(),
                    local_ip: Some(spec.local_ip),
                });
                connection = connection.saturating_add(1);
            }
        }
    }
    Ok(sources)
}

pub(crate) struct FeedIngress {
    message: BroadcastFeedMessage,
    frame_received_at: Instant,
    ready_for_channel_at: Instant,
    metrics: Arc<FeedSourceMetrics>,
}

#[derive(Clone)]
struct FeedSourceMetrics {
    connected: Gauge,
    connection_attempts: Counter,
    connections: Counter,
    disconnects: Counter,
    errors: Counter,
    messages: Counter,
    wins: Counter,
    duplicates: Counter,
    stale: Counter,
    coordinator_delay: Histogram,
    duplicate_lag: Histogram,
}

impl FeedSourceMetrics {
    fn new(source: &FeedSource) -> Self {
        let endpoint = source.endpoint.to_string();
        let connection = source.connection.to_string();
        let local_ip = source
            .local_ip
            .map_or_else(|| "os-selected".to_string(), |ip| ip.to_string());
        macro_rules! metric {
            ($macro:ident, $name:literal) => {
                metrics::$macro!(
                    $name,
                    "endpoint" => endpoint.clone(),
                    "connection" => connection.clone(),
                    "local_ip" => local_ip.clone()
                )
            };
        }
        Self {
            connected: metric!(gauge, "arb_reth.feed.source_connected"),
            connection_attempts: metric!(counter, "arb_reth.feed.source_connection_attempts_total"),
            connections: metric!(counter, "arb_reth.feed.source_connections_total"),
            disconnects: metric!(counter, "arb_reth.feed.source_disconnects_total"),
            errors: metric!(counter, "arb_reth.feed.source_errors_total"),
            messages: metric!(counter, "arb_reth.feed.source_messages_total"),
            wins: metric!(counter, "arb_reth.feed.source_wins_total"),
            duplicates: metric!(counter, "arb_reth.feed.source_duplicates_total"),
            stale: metric!(counter, "arb_reth.feed.source_stale_total"),
            coordinator_delay: metric!(histogram, "arb_reth.feed.source_coordinator_delay_seconds"),
            duplicate_lag: metric!(histogram, "arb_reth.feed.source_duplicate_lag_seconds"),
        }
    }
}

#[derive(Clone, Copy)]
struct FirstSeen {
    ready_at: Instant,
}

enum Observation {
    First,
    Duplicate { winner_ready_at: Option<Instant> },
    Stale,
}

/// Bounded first-wins sequence tracking plus a conservative reconnect cursor.
struct SequenceRace {
    next_resume: u64,
    recent: BTreeMap<u64, FirstSeen>,
}

impl SequenceRace {
    fn new(next_resume: u64) -> Self {
        Self {
            next_resume,
            recent: BTreeMap::new(),
        }
    }

    fn observe(&mut self, sequence: u64, ready_at: Instant) -> Observation {
        if let Some(first) = self.recent.get(&sequence) {
            return Observation::Duplicate {
                winner_ready_at: Some(first.ready_at),
            };
        }
        if sequence < self.next_resume {
            return Observation::Stale;
        }

        self.recent.insert(sequence, FirstSeen { ready_at });
        while self.recent.contains_key(&self.next_resume) {
            let Some(next) = self.next_resume.checked_add(1) else {
                break;
            };
            self.next_resume = next;
        }
        while self.recent.len() > MAX_RECENT_SEQUENCES {
            self.recent.pop_first();
        }
        Observation::First
    }
}

/// Create the bounded worker-to-coordinator channel.
pub(crate) fn ingress_channel() -> (mpsc::Sender<FeedIngress>, mpsc::Receiver<FeedIngress>) {
    mpsc::channel(4096)
}

/// Forward the first decoded copy of every sequence to the engine channel.
pub(crate) async fn coordinate(
    mut ingress: mpsc::Receiver<FeedIngress>,
    output: mpsc::Sender<BroadcastFeedMessage>,
    feed_latency: FeedLatencyTracker,
    resume_sequence: Arc<AtomicU64>,
) {
    let mut race = SequenceRace::new(resume_sequence.load(Ordering::Acquire));
    while let Some(item) = ingress.recv().await {
        let sequence = item.message.sequence_number;
        match race.observe(sequence, item.ready_for_channel_at) {
            Observation::First => {
                item.metrics.wins.increment(1);
                item.metrics.coordinator_delay.record(
                    Instant::now()
                        .saturating_duration_since(item.ready_for_channel_at)
                        .as_secs_f64(),
                );
                resume_sequence.store(race.next_resume, Ordering::Release);
                feed_latency.record_frame_arrival(sequence, item.frame_received_at);
                feed_latency.record_ready_for_channel(sequence, item.ready_for_channel_at);
                if output.send(item.message).await.is_err() {
                    reth_tracing::tracing::warn!(
                        target: "arb-reth",
                        "feed channel closed; stopping sequencer feed coordinator"
                    );
                    return;
                }
            }
            Observation::Duplicate { winner_ready_at } => {
                item.metrics.duplicates.increment(1);
                if let Some(winner_ready_at) = winner_ready_at {
                    item.metrics.duplicate_lag.record(
                        item.ready_for_channel_at
                            .saturating_duration_since(winner_ready_at)
                            .as_secs_f64(),
                    );
                }
            }
            Observation::Stale => item.metrics.stale.increment(1),
        }
    }
}

/// Maintain one independently reconnecting WebSocket source.
pub(crate) async fn follow(
    source: FeedSource,
    ingress: mpsc::Sender<FeedIngress>,
    resume_sequence: Arc<AtomicU64>,
) {
    use futures_util::StreamExt;

    let metrics = Arc::new(FeedSourceMetrics::new(&source));
    let mut consecutive_failures = 0u32;
    // Spread the initial handshake burst. Several public relays rate-limit simultaneous upgrades
    // even when they are willing to keep the same number of established sockets open.
    tokio::time::sleep(initial_connect_delay(source.ordinal)).await;
    loop {
        let requested_sequence = resume_sequence.load(Ordering::Acquire);
        let request = match feed_request(&source.url, requested_sequence) {
            Ok(request) => request,
            Err(err) => {
                metrics.errors.increment(1);
                reth_tracing::tracing::error!(
                    target: "arb-reth",
                    endpoint = source.endpoint,
                    connection = source.connection,
                    err = %err,
                    "feed: invalid URL; source stopping"
                );
                return;
            }
        };

        metrics.connection_attempts.increment(1);
        let mut pushed = 0usize;
        let mut rate_limited = false;
        match connect_source(&source, request).await {
            Ok((mut websocket, _)) => {
                metrics.connected.set(1.0);
                metrics.connections.increment(1);
                reth_tracing::tracing::info!(
                    target: "arb-reth",
                    endpoint = source.endpoint,
                    connection = source.connection,
                    relay = %source.display_endpoint,
                    local_ip = ?source.local_ip,
                    from_seq = requested_sequence,
                    "feed: connected to sequencer feed"
                );

                while let Some(frame) = websocket.next().await {
                    let frame_received_at = Instant::now();
                    let text = match frame {
                        Ok(Message::Text(text)) => text.as_str().to_owned(),
                        Ok(Message::Binary(bytes)) => match core::str::from_utf8(bytes.as_ref()) {
                            Ok(text) => text.to_owned(),
                            Err(_) => {
                                metrics.errors.increment(1);
                                continue;
                            }
                        },
                        Ok(Message::Close(_)) => break,
                        Ok(_) => continue,
                        Err(err) => {
                            metrics.errors.increment(1);
                            reth_tracing::tracing::warn!(
                                target: "arb-reth",
                                endpoint = source.endpoint,
                                connection = source.connection,
                                err = %err,
                                "feed: websocket error"
                            );
                            break;
                        }
                    };

                    let root = match serde_json::from_str::<Root>(&text) {
                        Ok(root) => root,
                        Err(err) => {
                            metrics.errors.increment(1);
                            reth_tracing::tracing::debug!(
                                target: "arb-reth",
                                endpoint = source.endpoint,
                                connection = source.connection,
                                err = %err,
                                "feed: skipping unparsed frame"
                            );
                            continue;
                        }
                    };
                    let ready_for_channel_at = Instant::now();
                    for message in root.messages.into_iter().flatten() {
                        metrics.messages.increment(1);
                        let item = FeedIngress {
                            message,
                            frame_received_at,
                            ready_for_channel_at,
                            metrics: metrics.clone(),
                        };
                        if ingress.send(item).await.is_err() {
                            metrics.connected.set(0.0);
                            return;
                        }
                        pushed += 1;
                    }
                }

                metrics.connected.set(0.0);
                metrics.disconnects.increment(1);
                reth_tracing::tracing::warn!(
                    target: "arb-reth",
                    endpoint = source.endpoint,
                    connection = source.connection,
                    pushed,
                    "feed: disconnected"
                );
            }
            Err(err) => {
                rate_limited = is_rate_limited(&err);
                metrics.connected.set(0.0);
                metrics.errors.increment(1);
                reth_tracing::tracing::warn!(
                    target: "arb-reth",
                    endpoint = source.endpoint,
                    connection = source.connection,
                    relay = %source.display_endpoint,
                    rate_limited,
                    err = %err,
                    "feed: connection failed"
                );
            }
        }

        consecutive_failures = if pushed == 0 {
            consecutive_failures.saturating_add(1)
        } else {
            0
        };
        let delay = reconnect_delay(consecutive_failures, source.ordinal, rate_limited);
        tokio::time::sleep(delay).await;
    }
}

type FeedWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect one feed lane, optionally binding its TCP socket before DNS-selected connection and
/// TLS/WebSocket handshakes.
///
/// Merely assigning secondary addresses to an EC2 interface does not diversify the path: ordinary
/// source selection keeps every socket on the primary address. Binding here makes each declared
/// private-address/EIP mapping an actual network lane without an extra proxy on the ingress path.
async fn connect_source(
    source: &FeedSource,
    request: Request<()>,
) -> Result<(FeedWebSocket, Response), WebSocketError> {
    let Some(local_ip) = source.local_ip else {
        return tokio_tungstenite::connect_async(request).await;
    };

    let parsed = url::Url::parse(&source.url).map_err(|err| {
        WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid validated feed URL: {err}"),
        ))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "validated feed URL has no host",
        ))
    })?;
    let port = parsed.port_or_known_default().ok_or_else(|| {
        WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "validated feed URL has no known port",
        ))
    })?;
    let remotes = tokio::net::lookup_host((host, port))
        .await
        .map_err(WebSocketError::Io)?;
    let mut compatible = 0usize;
    let mut last_error = None;
    let mut stream = None;
    for remote in remotes.filter(|remote| remote.is_ipv4() == local_ip.is_ipv4()) {
        compatible = compatible.saturating_add(1);
        let socket = if local_ip.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(WebSocketError::Io)?;
        socket
            .bind(SocketAddr::new(local_ip, 0))
            .map_err(WebSocketError::Io)?;
        match socket.connect(remote).await {
            Ok(connected) => {
                connected.set_nodelay(true).map_err(WebSocketError::Io)?;
                stream = Some(connected);
                break;
            }
            Err(err) => last_error = Some(err),
        }
    }
    let stream = stream.ok_or_else(|| {
        WebSocketError::Io(last_error.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!(
                    "feed DNS returned {compatible} addresses compatible with local source {local_ip}"
                ),
            )
        }))
    })?;

    client_async_tls_with_config(request, stream, None, None).await
}

fn feed_request(url: &str, requested_sequence: u64) -> Result<Request<()>> {
    let mut request = url
        .into_client_request()
        .map_err(|err| eyre!("build WebSocket request: {err}"))?;
    request.headers_mut().insert(
        FEED_CLIENT_VERSION_HEADER,
        HeaderValue::from_static(FEED_CLIENT_VERSION),
    );
    request.headers_mut().insert(
        REQUESTED_SEQUENCE_HEADER,
        HeaderValue::from_str(&requested_sequence.to_string())
            .expect("a u64 is always a valid HTTP header value"),
    );
    Ok(request)
}

fn initial_connect_delay(ordinal: usize) -> Duration {
    // Bring one useful source up immediately, then avoid a simultaneous upgrade burst. A relay's
    // steady-state connection cap is handled separately by the long 429 retry delay below.
    Duration::from_secs((ordinal as u64).min(10))
}

fn is_rate_limited(err: &WebSocketError) -> bool {
    matches!(err, WebSocketError::Http(response) if response.status() == StatusCode::TOO_MANY_REQUESTS)
}

fn reconnect_delay(failures: u32, ordinal: usize, rate_limited: bool) -> Duration {
    let exponent = failures.saturating_sub(1).min(5);
    let (initial, maximum) = if rate_limited {
        (INITIAL_RATE_LIMIT_DELAY, MAX_RATE_LIMIT_DELAY)
    } else {
        (INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY)
    };
    let base = initial
        .checked_mul(1u32 << exponent)
        .unwrap_or(maximum)
        .min(maximum);
    // A small deterministic stagger avoids reconnecting every socket on the same scheduler tick.
    let jitter_ms = ((ordinal * 73) % 1_000) as u64;
    (base + Duration::from_millis(jitter_ms)).min(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_endpoints_into_parallel_connections_without_exposing_paths() {
        let sources = expand_feed_sources(
            &[
                "wss://first.example/feed?token=secret".to_string(),
                "ws://127.0.0.1:9642/private".to_string(),
            ],
            Some(2),
            &[],
        )
        .unwrap();

        assert_eq!(sources.len(), 4);
        assert_eq!(sources[0].endpoint, 0);
        assert_eq!(sources[1].connection, 1);
        assert_eq!(sources[2].endpoint, 1);
        assert_eq!(sources[0].display_endpoint, "wss://first.example");
        assert_eq!(sources[2].display_endpoint, "ws://127.0.0.1:9642");
        assert!(!format!("{:?}", sources[0]).contains("secret"));
    }

    #[test]
    fn rejects_invalid_or_excessive_source_sets() {
        assert!(expand_feed_sources(&["https://example.com".into()], Some(1), &[]).is_err());
        assert!(expand_feed_sources(&["wss://example.com".into()], Some(0), &[]).is_err());
        assert!(expand_feed_sources(&["wss://example.com".into()], Some(65), &[]).is_err());
        assert!(expand_feed_sources(&[], Some(1), &[]).is_err());
        assert!(
            expand_feed_sources(
                &["wss://example.com".into()],
                None,
                &[
                    "192.0.2.1=1".parse().unwrap(),
                    "192.0.2.1=2".parse().unwrap(),
                ],
            )
            .is_err()
        );
        assert!("192.0.2.1=0".parse::<FeedSourceSpec>().is_err());
    }

    #[test]
    fn expands_counted_sources_without_a_hidden_default_lane() {
        let sources = expand_feed_sources(
            &["wss://example.com/feed".into()],
            None,
            &[
                "192.0.2.10=2".parse().unwrap(),
                "192.0.2.11=1".parse().unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0].local_ip, Some("192.0.2.10".parse().unwrap()));
        assert_eq!(sources[1].local_ip, Some("192.0.2.10".parse().unwrap()));
        assert_eq!(sources[2].local_ip, Some("192.0.2.11".parse().unwrap()));
    }

    #[test]
    fn default_and_mixed_connection_groups_are_explicit() {
        let default = expand_feed_sources(&["wss://example.com/feed".into()], None, &[]).unwrap();
        assert_eq!(default.len(), 1);
        assert_eq!(default[0].local_ip, None);

        let mixed = expand_feed_sources(
            &["wss://example.com/feed".into()],
            Some(2),
            &["192.0.2.10=3".parse().unwrap()],
        )
        .unwrap();
        assert_eq!(mixed.len(), 5);
        assert!(mixed[..2].iter().all(|source| source.local_ip.is_none()));
        assert!(
            mixed[2..]
                .iter()
                .all(|source| source.local_ip == Some("192.0.2.10".parse().unwrap()))
        );
    }

    #[test]
    fn first_copy_wins_and_resume_only_crosses_a_contiguous_prefix() {
        let now = Instant::now();
        let mut race = SequenceRace::new(10);

        assert!(matches!(race.observe(11, now), Observation::First));
        assert_eq!(race.next_resume, 10, "sequence 10 is still missing");
        assert!(matches!(
            race.observe(11, now + Duration::from_millis(1)),
            Observation::Duplicate { .. }
        ));
        assert!(matches!(race.observe(10, now), Observation::First));
        assert_eq!(race.next_resume, 12);
        assert!(matches!(race.observe(9, now), Observation::Stale));
    }

    #[test]
    fn websocket_request_uses_the_live_resume_sequence() {
        let request = feed_request("wss://example.com/feed", 42).unwrap();
        assert_eq!(
            request.headers()[FEED_CLIENT_VERSION_HEADER],
            FEED_CLIENT_VERSION
        );
        assert_eq!(request.headers()[REQUESTED_SEQUENCE_HEADER], "42");
    }

    #[test]
    fn reconnects_are_staggered_and_bounded() {
        assert_eq!(initial_connect_delay(0), Duration::ZERO);
        assert_eq!(initial_connect_delay(4), Duration::from_secs(4));
        assert_eq!(reconnect_delay(1, 0, false), Duration::from_millis(250));
        assert!(reconnect_delay(1, 1, false) > reconnect_delay(1, 0, false));
        assert!(reconnect_delay(100, 0, false) <= MAX_RECONNECT_DELAY);
        assert_eq!(reconnect_delay(1, 0, true), Duration::from_secs(30));
        assert!(reconnect_delay(100, 0, true) <= MAX_RATE_LIMIT_DELAY);
    }

    #[tokio::test]
    async fn a_source_bound_socket_reaches_the_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            let _websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            peer.ip()
        });
        let source = FeedSource {
            ordinal: 0,
            endpoint: 0,
            connection: 0,
            url: format!("ws://{address}"),
            display_endpoint: format!("ws://{address}"),
            // macOS does not treat the whole 127/8 range as configured by default, so use the
            // portable loopback address here. The EC2 deployment check covers distinct ENI
            // addresses; this test exercises the explicit bind + WebSocket handshake path.
            local_ip: Some("127.0.0.1".parse().unwrap()),
        };
        let request = feed_request(&source.url, 42).unwrap();

        let (_websocket, _response) = connect_source(&source, request).await.unwrap();
        assert_eq!(
            server.await.unwrap(),
            "127.0.0.1".parse::<IpAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn coordinator_forwards_only_the_first_copy_without_reordering_sources() {
        let sources =
            expand_feed_sources(&["wss://example.com/feed".into()], Some(2), &[]).unwrap();
        let metrics = [
            Arc::new(FeedSourceMetrics::new(&sources[0])),
            Arc::new(FeedSourceMetrics::new(&sources[1])),
        ];
        let (ingress_tx, ingress_rx) = ingress_channel();
        let (output_tx, mut output_rx) = mpsc::channel(8);
        let resume = Arc::new(AtomicU64::new(10));
        let coordinator = tokio::spawn(coordinate(
            ingress_rx,
            output_tx,
            FeedLatencyTracker::new(),
            resume.clone(),
        ));

        let started = Instant::now();
        for (sequence, source) in [(10, 0), (10, 1), (12, 1), (11, 0)] {
            ingress_tx
                .send(FeedIngress {
                    message: BroadcastFeedMessage {
                        sequence_number: sequence,
                        ..Default::default()
                    },
                    frame_received_at: started,
                    ready_for_channel_at: Instant::now(),
                    metrics: metrics[source].clone(),
                })
                .await
                .unwrap();
        }
        drop(ingress_tx);

        let mut forwarded = Vec::new();
        while let Some(message) = output_rx.recv().await {
            forwarded.push(message.sequence_number);
        }
        coordinator.await.unwrap();

        assert_eq!(forwarded, vec![10, 12, 11]);
        assert_eq!(resume.load(Ordering::Acquire), 13);
    }
}
