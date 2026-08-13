//! Best-effort local IPC for per-transaction ArbOS execution logs.
//!
//! The socket uses newline-delimited JSON. It is intentionally separate from RPC: callers receive
//! a transaction as soon as its EVM execution completes, before receipt/state-root hashing and
//! before the enclosing block becomes canonical.

use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
};

use alloy_primitives::{Address, B256, Bytes};
use arb_reth_engine::{ArbTxExecutionKind, ArbTxLogBroadcaster, ArbTxLogEvent};
use eyre::{Context, Result, bail};
use serde::Serialize;
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    sync::broadcast,
};

/// A local Unix-domain socket server for [`ArbTxLogEvent`] values.
pub(crate) struct MevTxLogIpc {
    listener: UnixListener,
    path: PathBuf,
    broadcaster: ArbTxLogBroadcaster,
}

impl MevTxLogIpc {
    /// Binds the requested local socket, replacing a stale socket from a previous shutdown.
    pub(crate) fn bind(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&path).wrap_err_with(|| {
            format!("bind MEV transaction-log IPC socket at {}", path.display())
        })?;
        Ok(Self {
            listener,
            path,
            broadcaster: ArbTxLogBroadcaster::new(),
        })
    }

    /// Returns the execution-side publisher passed to the native payload builder.
    pub(crate) fn broadcaster(&self) -> ArbTxLogBroadcaster {
        self.broadcaster.clone()
    }

    /// Socket location, used only for the launch log.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Serves local clients until the runtime begins graceful shutdown.
    pub(crate) async fn serve(self, mut shutdown: reth_tasks::shutdown::GracefulShutdown) {
        loop {
            tokio::select! {
                guard = &mut shutdown => {
                    drop(guard);
                    break;
                }
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let events = self.broadcaster.subscribe();
                        tokio::spawn(stream_client(stream, events));
                    }
                    Err(error) => {
                        reth_tracing::tracing::warn!(
                            target: "arb-reth::mev",
                            %error,
                            path = %self.path.display(),
                            "MEV transaction-log IPC accept failed"
                        );
                    }
                },
            }
        }
    }
}

impl Drop for MevTxLogIpc {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            reth_tracing::tracing::warn!(
                target: "arb-reth::mev",
                %error,
                path = %self.path.display(),
                "failed to remove MEV transaction-log IPC socket"
            );
        }
    }
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).wrap_err_with(|| format!("inspect {}", path.display())),
    };
    if !metadata.file_type().is_socket() {
        bail!(
            "refusing to replace non-socket MEV transaction-log IPC path {}",
            path.display()
        );
    }
    fs::remove_file(path).wrap_err_with(|| format!("remove stale socket {}", path.display()))
}

async fn stream_client(mut stream: UnixStream, mut events: broadcast::Receiver<ArbTxLogEvent>) {
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                reth_tracing::tracing::warn!(
                    target: "arb-reth::mev",
                    skipped,
                    "disconnecting slow MEV transaction-log IPC client"
                );
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let encoded = match encode_event(&event) {
            Ok(encoded) => encoded,
            Err(error) => {
                reth_tracing::tracing::warn!(
                    target: "arb-reth::mev",
                    %error,
                    "failed to serialize MEV transaction-log event"
                );
                continue;
            }
        };
        if stream.write_all(&encoded).await.is_err() {
            return;
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireEvent<'a> {
    event: &'static str,
    block_number: u64,
    transaction_index: u64,
    transaction_hash: B256,
    kind: &'static str,
    success: bool,
    gas_used: u64,
    logs: Vec<WireLog<'a>>,
}

#[derive(Serialize)]
struct WireLog<'a> {
    address: Address,
    topics: &'a [B256],
    data: &'a Bytes,
}

fn encode_event(event: &ArbTxLogEvent) -> Result<Vec<u8>, serde_json::Error> {
    let event = WireEvent {
        event: "transactionLogs",
        block_number: event.block_number,
        transaction_index: event.transaction_index,
        transaction_hash: event.transaction_hash,
        kind: match event.kind {
            ArbTxExecutionKind::StartBlock => "startBlock",
            ArbTxExecutionKind::User => "user",
            ArbTxExecutionKind::ScheduledRetry => "scheduledRetry",
        },
        success: event.success,
        gas_used: event.gas_used,
        logs: event
            .logs
            .iter()
            .map(|log| WireLog {
                address: log.address,
                topics: log.data.topics(),
                data: &log.data.data,
            })
            .collect(),
    };
    let mut encoded = serde_json::to_vec(&event)?;
    encoded.push(b'\n');
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Log, LogData, b256};
    use tokio::io::{AsyncBufReadExt, BufReader};

    #[test]
    fn encodes_one_newline_delimited_transaction_event() {
        let event = ArbTxLogEvent {
            block_number: 42,
            transaction_index: 3,
            transaction_hash: b256!(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            kind: ArbTxExecutionKind::User,
            success: true,
            gas_used: 21_000,
            logs: vec![Log {
                address: Address::repeat_byte(0x11),
                data: LogData::new_unchecked(
                    vec![b256!(
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    )],
                    Bytes::from_static(&[0x12, 0x34]),
                ),
            }],
        };

        let encoded = encode_event(&event).expect("event serializes");
        assert_eq!(encoded.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("valid JSON");
        assert_eq!(value["event"], "transactionLogs");
        assert_eq!(value["blockNumber"], 42);
        assert_eq!(value["transactionIndex"], 3);
        assert_eq!(value["kind"], "user");
        assert_eq!(value["logs"][0]["data"], "0x1234");
    }

    #[test]
    fn only_replaces_an_existing_socket() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("mev.sock");
        fs::write(&path, b"not a socket").expect("write regular file");

        assert!(remove_stale_socket(&path).is_err());
        assert!(path.exists());
    }

    #[tokio::test]
    async fn connected_client_receives_transaction_event() {
        let broadcaster = ArbTxLogBroadcaster::new();
        let receiver = broadcaster.subscribe();
        let (writer, reader) = UnixStream::pair().expect("Unix stream pair");
        let client = tokio::spawn(stream_client(writer, receiver));

        broadcaster.publish(ArbTxLogEvent {
            block_number: 42,
            transaction_index: 3,
            transaction_hash: B256::ZERO,
            kind: ArbTxExecutionKind::User,
            success: true,
            gas_used: 21_000,
            logs: Vec::new(),
        });

        let mut line = Vec::new();
        BufReader::new(reader)
            .read_until(b'\n', &mut line)
            .await
            .expect("read event");
        let value: serde_json::Value = serde_json::from_slice(&line).expect("valid JSON");
        assert_eq!(value["blockNumber"], 42);
        assert_eq!(value["transactionIndex"], 3);

        drop(broadcaster);
        client.await.expect("client task exits");
    }
}
