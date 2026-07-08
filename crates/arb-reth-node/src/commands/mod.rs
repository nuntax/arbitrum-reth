//! Subcommand implementations for the single `arb-reth` binary. Each submodule holds the clap
//! `Args` struct and `run` entrypoint for one command; the binary's dispatcher parses the top-level
//! CLI and calls into these.

pub mod dump_blocks;
pub mod genesis;
pub mod node;
pub mod rewind;
pub mod snapshot;
