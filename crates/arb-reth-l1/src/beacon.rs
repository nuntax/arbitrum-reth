//! Minimal beacon (consensus-layer) REST client for fetching EIP-4844 blob
//! sidecars, the data source for blob-located sequencer batches.
//!
//! Blob sidecars are addressed by slot. The slot for an execution block is
//! `(block_timestamp - genesis_time) / seconds_per_slot` (post-merge the execution
//! payload timestamp equals the slot time exactly). Sidecars are matched to a
//! batch's blob versioned hashes via `kzg_to_versioned_hash`.
//!
//! Beacon nodes retain sidecars for only ~18 days, so deep historic sync needs a
//! blob-archive endpoint.

use alloy_eips::eip4844::kzg_to_versioned_hash;
use alloy_primitives::B256;
use serde::Deserialize;

use arb_reth_derive::blob::{decode_blobs, Blob, BYTES_PER_BLOB};

use crate::L1Error;

/// Mainnet beacon-chain genesis time (Unix seconds).
pub const MAINNET_GENESIS_TIME: u64 = 1_606_824_023;
/// Seconds per slot (mainnet).
pub const SECONDS_PER_SLOT: u64 = 12;

/// A beacon REST client (e.g. an Alchemy/Lighthouse `/eth/v1/...` base URL).
#[derive(Debug, Clone)]
pub struct BeaconClient {
    http: reqwest::Client,
    base: String,
    genesis_time: u64,
    seconds_per_slot: u64,
}

impl BeaconClient {
    /// New client for `base` (the URL up to but not including `/eth/v1/...`), with
    /// mainnet slot timing.
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base.into().trim_end_matches('/').to_string(),
            genesis_time: MAINNET_GENESIS_TIME,
            seconds_per_slot: SECONDS_PER_SLOT,
        }
    }

    /// Override slot timing (for non-mainnet chains).
    pub fn with_timing(mut self, genesis_time: u64, seconds_per_slot: u64) -> Self {
        self.genesis_time = genesis_time;
        self.seconds_per_slot = seconds_per_slot;
        self
    }

    /// The slot containing the execution block with timestamp `ts`.
    pub fn slot_for_timestamp(&self, ts: u64) -> u64 {
        ts.saturating_sub(self.genesis_time) / self.seconds_per_slot
    }

    /// Fetch all blob sidecars for a slot.
    ///
    /// Transient failures (network/timeout errors, HTTP 429, and 5xx) are retried with exponential
    /// backoff so a brief provider hiccup does not kill an hours-long derivation. Alchemy's beacon
    /// blob endpoint in particular returns sporadic 503s under load. Permanent failures (e.g. 404)
    /// and body-decode errors return immediately.
    pub async fn blob_sidecars(&self, slot: u64) -> Result<Vec<BlobSidecar>, L1Error> {
        let url = format!("{}/eth/v1/beacon/blob_sidecars/{slot}", self.base);
        const MAX_ATTEMPTS: u32 = 12;
        let mut backoff = std::time::Duration::from_millis(500);
        for attempt in 1..=MAX_ATTEMPTS {
            let last: L1Error = match self.http.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let parsed: SidecarsResponse =
                            resp.json().await.map_err(|e| L1Error::Rpc(e.to_string()))?;
                        return parsed.data.into_iter().map(BlobSidecar::try_from).collect();
                    }
                    // 429 (rate limit) and 5xx (server) are transient; other statuses are permanent.
                    let transient = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status.is_server_error();
                    let err = L1Error::Rpc(format!("beacon blob_sidecars/{slot} -> {status}"));
                    if !transient {
                        return Err(err);
                    }
                    err
                }
                // Network/timeout errors are transient.
                Err(e) => L1Error::Rpc(e.to_string()),
            };
            if attempt == MAX_ATTEMPTS {
                return Err(last);
            }
            tracing::warn!(
                target: "arb-reth::l1-beacon",
                slot, attempt, backoff_ms = backoff.as_millis() as u64, err = %last,
                "transient beacon error, retrying blob sidecars",
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
        }
        unreachable!("loop returns on the final attempt")
    }

    /// Fetch the blobs for `versioned_hashes` (in order) at `slot` and decode them
    /// into the batch payload (the brotli-flagged byte stream the calldata path also
    /// yields).
    pub async fn blob_batch_payload(
        &self,
        slot: u64,
        versioned_hashes: &[B256],
    ) -> Result<Vec<u8>, L1Error> {
        let sidecars = self.blob_sidecars(slot).await?;
        let mut blobs: Vec<Blob> = Vec::with_capacity(versioned_hashes.len());
        for vh in versioned_hashes {
            let sc = sidecars
                .iter()
                .find(|s| s.versioned_hash == *vh)
                .ok_or(L1Error::Missing("blob sidecar for versioned hash"))?;
            blobs.push(*sc.blob);
        }
        decode_blobs(&blobs).map_err(|e| L1Error::Blob(format!("{e:?}")))
    }
}

/// A blob sidecar reduced to what derivation needs: its versioned hash and bytes.
#[derive(Debug, Clone)]
pub struct BlobSidecar {
    /// `kzg_to_versioned_hash(kzg_commitment)`; matches the tx's blob hashes.
    pub versioned_hash: B256,
    /// The 128 KiB blob.
    pub blob: Box<Blob>,
}

#[derive(Deserialize)]
struct SidecarsResponse {
    data: Vec<RawSidecar>,
}

#[derive(Deserialize)]
struct RawSidecar {
    kzg_commitment: String,
    blob: String,
}

impl TryFrom<RawSidecar> for BlobSidecar {
    type Error = L1Error;

    fn try_from(r: RawSidecar) -> Result<Self, L1Error> {
        let commitment = alloy_primitives::hex::decode(&r.kzg_commitment)
            .map_err(|e| L1Error::Rpc(format!("commitment hex: {e}")))?;
        let versioned_hash = kzg_to_versioned_hash(&commitment);
        let bytes = alloy_primitives::hex::decode(&r.blob)
            .map_err(|e| L1Error::Rpc(format!("blob hex: {e}")))?;
        if bytes.len() != BYTES_PER_BLOB {
            return Err(L1Error::Rpc(format!("blob length {} != {BYTES_PER_BLOB}", bytes.len())));
        }
        let mut blob = Box::new([0u8; BYTES_PER_BLOB]);
        blob.copy_from_slice(&bytes);
        Ok(BlobSidecar { versioned_hash, blob })
    }
}
