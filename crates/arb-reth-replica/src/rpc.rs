//! JSON-RPC backed [`HeadSource`].
//!
//! Uses raw `eth_blockNumber` / `eth_getBlockByNumber` requests decoded as JSON values
//! rather than typed RPC structs: the monitor only needs `number`, `hash`, and
//! `stateRoot`, and staying schema-agnostic keeps it compatible with both the local
//! `arb-reth` RPC and remote Nitro endpoints regardless of which optional fields either
//! side includes.

use crate::{BlockIdentity, HeadSource};
use alloy_primitives::U64;
use alloy_rpc_client::RpcClient;
use eyre::{WrapErr, eyre};

/// A chain head behind an HTTP JSON-RPC endpoint.
#[derive(Clone)]
pub struct RpcHeadSource {
    client: RpcClient,
    /// Endpoint label used in error messages ("local" / "canonical").
    label: &'static str,
}

impl RpcHeadSource {
    /// Connect to `url`. The transport is wrapped in the same reactive retry/backoff
    /// layer the L1 sync uses, so transient 429/5xx/timeouts do not surface as monitor
    /// errors.
    pub fn new(url: &str, label: &'static str) -> eyre::Result<Self> {
        let url = url
            .parse()
            .wrap_err_with(|| format!("invalid {label} RPC url: {url}"))?;
        let client = alloy_rpc_client::ClientBuilder::default()
            .layer(alloy_transport::layers::RetryBackoffLayer::new(
                10, 500, 660,
            ))
            .http(url);
        Ok(Self { client, label })
    }
}

impl HeadSource for RpcHeadSource {
    async fn head_number(&self) -> eyre::Result<u64> {
        let n: U64 = self
            .client
            .request("eth_blockNumber", ())
            .await
            .wrap_err_with(|| format!("{}: eth_blockNumber failed", self.label))?;
        Ok(n.to())
    }

    async fn block_identity(&self, number: u64) -> eyre::Result<Option<BlockIdentity>> {
        let block: Option<serde_json::Value> = self
            .client
            .request("eth_getBlockByNumber", (format!("0x{number:x}"), false))
            .await
            .wrap_err_with(|| format!("{}: eth_getBlockByNumber({number}) failed", self.label))?;
        let Some(block) = block else { return Ok(None) };

        let field = |key: &str| -> eyre::Result<String> {
            block
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
                .ok_or_else(|| eyre!("{}: block {number} missing `{key}`", self.label))
        };
        Ok(Some(BlockIdentity {
            number,
            hash: field("hash")?.to_lowercase(),
            state_root: field("stateRoot")?.to_lowercase(),
        }))
    }
}
