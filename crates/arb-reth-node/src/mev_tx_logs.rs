//! Best-effort local IPC for per-transaction ArbOS execution logs.
//!
//! The socket uses a compact, length-delimited binary frame. It is intentionally separate from
//! RPC: callers receive a transaction as soon as its EVM execution completes, before receipt/
//! state-root hashing and before the enclosing block becomes canonical.

use std::{
    fs,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
};

use alloy_primitives::B256;
use arb_reth_engine::{ArbTxExecutionKind, ArbTxLogBroadcaster, ArbTxLogEvent};
use eyre::{Context, Result, bail};
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
                    "failed to encode MEV transaction-log event"
                );
                continue;
            }
        };
        if stream.write_all(&encoded).await.is_err() {
            return;
        }
    }
}

/// Version of the binary frame format documented in `docs/mev-tx-log-ipc.md`.
const FRAME_VERSION: u8 = 1;
const FIXED_BODY_LEN: usize = 64;

fn encode_event(event: &ArbTxLogEvent) -> Result<Vec<u8>> {
    let mut body_len = FIXED_BODY_LEN;
    for log in &event.logs {
        let topics = log.data.topics();
        if topics.len() > u8::MAX as usize {
            bail!("MEV transaction-log event has too many log topics")
        }
        body_len = body_len
            .checked_add(25 + topics.len() * B256::len_bytes() + log.data.data.len())
            .ok_or_else(|| eyre::eyre!("MEV transaction-log frame length overflow"))?;
    }
    let frame_len = u32::try_from(body_len)
        .map_err(|_| eyre::eyre!("MEV transaction-log frame exceeds 4 GiB"))?;

    let mut encoded = Vec::with_capacity(4 + body_len);
    encoded.extend_from_slice(&frame_len.to_be_bytes());
    encoded.push(FRAME_VERSION);
    encoded.push(match event.kind {
        ArbTxExecutionKind::StartBlock => 0,
        ArbTxExecutionKind::User => 1,
        ArbTxExecutionKind::ScheduledRetry => 2,
    });
    encoded.push(u8::from(event.success));
    encoded.push(0); // Reserved for future flags.
    encoded.extend_from_slice(&event.block_number.to_be_bytes());
    encoded.extend_from_slice(&event.transaction_index.to_be_bytes());
    encoded.extend_from_slice(&event.gas_used.to_be_bytes());
    encoded.extend_from_slice(event.transaction_hash.as_slice());
    encoded.extend_from_slice(&(event.logs.len() as u32).to_be_bytes());
    for log in &event.logs {
        let topics = log.data.topics();
        encoded.extend_from_slice(log.address.as_slice());
        encoded.push(topics.len() as u8);
        encoded.extend_from_slice(&(log.data.data.len() as u32).to_be_bytes());
        for topic in topics {
            encoded.extend_from_slice(topic.as_slice());
        }
        encoded.extend_from_slice(&log.data.data);
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, Log, LogData, b256};
    use tokio::io::AsyncReadExt;

    #[test]
    fn encodes_one_binary_transaction_event() {
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

        let encoded = encode_event(&event).expect("event encodes");
        assert_eq!(
            u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize,
            encoded.len() - 4
        );
        assert_eq!(encoded[4], FRAME_VERSION);
        assert_eq!(encoded[5], 1); // user
        assert_eq!(encoded[6], 1); // success
        assert_eq!(u64::from_be_bytes(encoded[8..16].try_into().unwrap()), 42);
        assert_eq!(u64::from_be_bytes(encoded[16..24].try_into().unwrap()), 3);
        assert_eq!(
            u64::from_be_bytes(encoded[24..32].try_into().unwrap()),
            21_000
        );
        assert_eq!(u32::from_be_bytes(encoded[64..68].try_into().unwrap()), 1);
        assert_eq!(&encoded[68..88], Address::repeat_byte(0x11).as_slice());
        assert_eq!(encoded[88], 1);
        assert_eq!(u32::from_be_bytes(encoded[89..93].try_into().unwrap()), 2);
        assert_eq!(&encoded[125..127], [0x12, 0x34]);
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

        let mut length = [0; 4];
        let mut reader = reader;
        reader
            .read_exact(&mut length)
            .await
            .expect("read frame length");
        let mut body = vec![0; u32::from_be_bytes(length) as usize];
        reader.read_exact(&mut body).await.expect("read frame");
        assert_eq!(body[4..12], 42u64.to_be_bytes());
        assert_eq!(body[12..20], 3u64.to_be_bytes());

        drop(broadcaster);
        client.await.expect("client task exits");
    }
}
