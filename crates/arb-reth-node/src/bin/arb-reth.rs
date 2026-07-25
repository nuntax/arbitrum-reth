//! `arb-reth`: single entrypoint for the Arbitrum (ArbOS-on-reth) toolchain.
//!
//! Dispatches clap subcommands into the per-command implementations in
//! [`arb_reth_node::commands`]:
//!
//! - `node`             the standalone Arbitrum node (feed / L1-derivation block producer + RPC)
//! - `snapshot import`  import a Nitro genesis-state stream into reth MDBX
//! - `snapshot import-full`  convert a full-snapshot stream (blocks + history + state)
//! - `snapshot read`    read hashed-state from a converted snapshot
//! - `genesis verify`   verify the Arbitrum One Nitro-genesis state root from the classic export
//! - `genesis verify-export`  verify a `reth-export --mode state` stream (stdin)
//! - `rewind`           unwind the database to an earlier L2 block after a divergence
//! - `dump-blocks`      dump block headers + tx hashes + receipt status

#![allow(missing_docs)]

use arb_reth_node::commands::{
    self,
    dump_blocks::DumpBlocksArgs,
    genesis::{GenesisVerifyArgs, GenesisVerifyExportArgs},
    node::{ArbChainSpecParser, ArbNodeArgs},
    rewind::RewindArgs,
    snapshot::{
        SnapshotBuildPreimagesArgs, SnapshotImportArgs, SnapshotReadArgs,
        SnapshotRepairHistoryArgs,
    },
    snapshot_full::{SnapshotFinalizeArgs, SnapshotImportFullArgs},
};
use clap::{Args, Parser, Subcommand};
use reth_cli_commands::node::NodeCommand;
use reth_cli_runner::CliRunner;
use reth_node_core::{
    args::{DefaultEngineValues, DefaultLogArgs, LogArgs, OtlpInitStatus, TraceArgs},
    version::version_metadata,
};
use reth_node_metrics::recorder::install_prometheus_recorder;
use reth_tracing::{Layers, tracing::{info, warn}};

/// Stack-probe shim for x86_64: wasmer references `__rust_probestack` which recent
/// `compiler-builtins` no longer exports; this satisfies the linker. No-op on aarch64.
///
/// # Safety
///
/// Defined for the linker only; never called from Rust.
#[cfg(target_arch = "x86_64")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __rust_probestack() {}

#[derive(Debug, Parser)]
#[command(
    author,
    name = version_metadata().name_client.as_ref(),
    version = version_metadata().short_version.as_ref(),
    long_version = version_metadata().long_version.as_ref(),
    about = "Standalone Arbitrum node built on Reth",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Reth logging configuration.
    #[command(flatten)]
    logs: LogArgs,

    /// Reth OpenTelemetry tracing configuration.
    #[command(flatten)]
    traces: TraceArgs,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the standalone Arbitrum node.
    Node(Box<NodeCommand<ArbChainSpecParser, ArbNodeArgs>>),
    /// Snapshot import/read tools.
    Snapshot(SnapshotCmd),
    /// Genesis verification tools.
    Genesis(GenesisCmd),
    /// Unwind the database to an earlier L2 block.
    Rewind(RewindArgs),
    /// Dump block headers + tx hashes + receipt status.
    DumpBlocks(DumpBlocksArgs),
}

#[derive(Debug, Args)]
struct SnapshotCmd {
    #[command(subcommand)]
    command: SnapshotSub,
}

#[derive(Debug, Subcommand)]
enum SnapshotSub {
    /// Build Reth's slot-preimage sidecar from a Nitro Classic export.
    BuildPreimages(SnapshotBuildPreimagesArgs),
    /// Import a Nitro genesis state stream into reth MDBX and verify the state root.
    Import(SnapshotImportArgs),
    /// Convert a `reth-export --mode full-snapshot` stream into a reth datadir.
    ImportFull(SnapshotImportFullArgs),
    /// Finish a converted datadir that stopped after its state root.
    Finalize(SnapshotFinalizeArgs),
    /// Read hashed-state from a converted Arbitrum reth MDBX snapshot.
    Read(SnapshotReadArgs),
    /// Add missing history-boundary metadata to an existing snapshot import.
    RepairHistory(SnapshotRepairHistoryArgs),
}

#[derive(Debug, Args)]
struct GenesisCmd {
    #[command(subcommand)]
    command: GenesisSub,
}

#[derive(Debug, Subcommand)]
enum GenesisSub {
    /// Verify the Arbitrum One Nitro-genesis state root from the classic-state export.
    Verify(GenesisVerifyArgs),
    /// Verify the hashed state-trie root of a `reth-export --mode state` stream (stdin).
    VerifyExport(GenesisVerifyExportArgs),
}

fn main() -> eyre::Result<()> {
    // Ethereum's per-payload and per-commit INFO logs are too noisy for Arbitrum's block cadence.
    // Keep periodic progress, lifecycle events, warnings, and errors at INFO. Operators can still
    // opt into either hot-path target with the native log-filter flags.
    const ARB_NODE_LOG_FILTER: &str = "payload_builder=warn,reth_node_events::node=warn";
    const ARB_NODE_FILE_LOG_FILTER: &str = "info,payload_builder=warn,reth_node_events::node=warn";
    DefaultLogArgs::default()
        .with_log_stdout_filter(ARB_NODE_LOG_FILTER.to_string())
        .with_log_file_filter(ARB_NODE_FILE_LOG_FILTER.to_string())
        .try_init()
        .expect("arb-reth initializes log defaults before any CLI parsing");

    // Use Reth's native engine flags while retaining Arbitrum's empirically sensible defaults.
    // `try_init` must happen before clap evaluates the flag defaults.
    DefaultEngineValues::default()
        .with_persistence_threshold(2)
        .with_persistence_backpressure_threshold(16)
        .with_memory_block_buffer_target(0)
        .with_cross_block_cache_size(256)
        .with_share_execution_cache_with_payload_builder(true)
        .with_share_sparse_trie_with_payload_builder(false)
        .try_init()
        .expect("arb-reth initializes engine defaults before any CLI parsing");

    let mut cli = Cli::parse();
    if matches!(&cli.command, Command::Node(_)) {
        cli.logs.apply_node_defaults();
    }
    let runner = CliRunner::try_default_runtime()?;

    let mut layers = Layers::new();
    let otlp_status = runner.block_on(cli.traces.init_otlp_tracing(&mut layers))?;
    let _guard = cli.logs.init_tracing_with_layers(layers, false)?;
    match otlp_status {
        OtlpInitStatus::Started(endpoint) => {
            info!(target: "arb-reth", %endpoint, "OTLP trace export enabled");
        }
        OtlpInitStatus::NoFeature => {
            warn!(target: "arb-reth", "OTLP tracing requested without the otlp feature");
        }
        OtlpInitStatus::Disabled => {}
    }

    // Install the native recorder before any Arbitrum or Reth metric handle is initialized.
    install_prometheus_recorder();

    // rustls 0.23 carries both the aws-lc-rs and ring backends in our dep tree, so it can't pick a
    // process-default CryptoProvider on its own; the first wss:// feed connect (connect_async builds
    // a rustls ClientConfig) would otherwise panic with "no process-level CryptoProvider available".
    // Install the aws-lc-rs provider once here. Err just means one is already installed, so ignore.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    match cli.command {
        Command::Node(command) => runner.run_command_until_exit(move |ctx| {
            commands::node::run(ctx, *command)
        }),
        Command::Snapshot(cmd) => match cmd.command {
            SnapshotSub::BuildPreimages(args) => commands::snapshot::build_preimages(args),
            SnapshotSub::Import(args) => commands::snapshot::import(args),
            SnapshotSub::ImportFull(args) => commands::snapshot_full::import_full(args),
            SnapshotSub::Finalize(args) => commands::snapshot_full::finalize_datadir(args),
            SnapshotSub::Read(args) => commands::snapshot::read(args),
            SnapshotSub::RepairHistory(args) => commands::snapshot::repair_history(args),
        },
        Command::Genesis(cmd) => match cmd.command {
            GenesisSub::Verify(args) => commands::genesis::verify(args),
            GenesisSub::VerifyExport(args) => commands::genesis::verify_export(args),
        },
        Command::Rewind(args) => commands::rewind::run(args),
        Command::DumpBlocks(args) => commands::dump_blocks::run(args),
    }
}
