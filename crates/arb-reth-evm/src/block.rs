//! `arb-reth-evm` Stage C — block executor + assembler for Arbitrum.
//!
//! Adapts `arb_revm`'s existing block-execution machinery (`executor::run::execute_message`,
//! `executor::hooks::ArbStartBlockDerived`) to reth v2.0.0's alloy-evm
//! [`BlockExecutor`]/[`BlockExecutorFactory`]/[`BlockAssembler`] trait surface, built on the
//! Stage B [`ArbEvm`](crate::ArbEvm)/[`ArbEvmFactory`](crate::ArbEvmFactory).
//!
//! Mirrors `alloy-op-evm`'s `OpBlockExecutor`/`OpBlockExecutorFactory` (the per-tx OP fee
//! accounting lives in `op_revm`'s handler; ours lives in `arb_revm::handler`). So this layer
//! does **not** re-implement the ArbOS gas/poster/tip math — it drives [`ArbEvm`] per tx (which
//! routes through `ArbHandler` inside `transact`), reads the resulting `poster_gas` off the chain
//! context for the receipt's `gas_used_for_l1`, and builds an [`ArbReceiptEnvelope`].
//!
//! Pre-execution mirrors `execute_message`'s prelude: the EIP-2935 history-storage parent-hash
//! system call (ArbOS v40+; a no-op state transition before that) followed by Nitro's
//! `InternalTxStartBlock` (typed internal tx `0x6a`), built via `arb_revm`'s
//! [`DefaultArbExecutionHooks`] so the `ArbosActs.startBlock(...)` calldata is identical.

use crate::tx::ArbTx;
use crate::{ArbEvm, ArbEvmFactory};
use alloc::boxed::Box;
use alloc::vec::Vec;
use alloy_consensus::{
    Block, BlockBody, EMPTY_OMMER_ROOT_HASH, Eip658Value, Header, Receipt, ReceiptWithBloom,
    TxReceipt, proofs,
};
use alloy_eips::{Encodable2718, Typed2718};
use alloy_eips::merge::BEACON_NONCE;
use alloy_evm::{
    Database, Evm, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        BlockExecutorFor, ExecutableTx, OnStateHook, StateChangePreBlockSource, StateChangeSource,
        StateDB, TxResult,
    },
};
use alloy_primitives::{Address, B256, Bytes, Log, U256, logs_bloom};
use arb_alloy_consensus::receipt::{ArbReceipt, ArbReceiptEnvelope};
use arb_alloy_consensus::transactions::ArbTxEnvelope;
use arb_revm::api::default_ctx::ArbContext;
use arb_revm::constants::{ARBITRUM_INTERNAL_TX_TYPE, HISTORY_STORAGE_ADDRESS};
use arb_revm::executor::hooks::{ArbExecutionHooks, ArbStartBlockDerived, DefaultArbExecutionHooks};
use arb_revm::executor::{ArbExecutionInput, ArbMessageEnvelope, ArbParentHeader};
use arb_revm::{ArbExecCfg, ArbTransaction};
use core::fmt::Debug;
use reth_evm::execute::{BlockAssembler, BlockAssemblerInput};
use revm::context::{Block as _, TxEnv, result::ResultAndState};
use revm::handler::SYSTEM_ADDRESS;
use revm::{DatabaseCommit, Inspector, context::result::ExecutionResult, primitives::TxKind};

/// Block-execution context for an Arbitrum block, beyond what the EVM env carries.
///
/// This is the analogue of `OpBlockExecutionCtx`. It provides the inputs the StartBlock prelude
/// (`InternalTxStartBlock`) and the EIP-2935 parent-hash system call need — values that are not
/// representable in alloy's [`EvmEnv`](alloy_evm::EvmEnv): the L1 base fee / L1 block number / poster
/// for this L2 block, and the parent block hash for the history-storage write.
///
/// Must be [`Clone`] (a [`BlockExecutorFactory`] requirement).
#[derive(Debug, Default, Clone)]
pub struct ArbBlockExecutionCtx {
    /// Parent (L2) block hash, written to the EIP-2935 history-storage contract pre-execution.
    pub parent_hash: B256,
    /// Parent block extra_data (carried through to the assembled header).
    pub extra_data: Bytes,
    /// L1 base fee (wei) for this block — the `l1BaseFee` arg of `ArbosActs.startBlock`.
    pub l1_base_fee_wei: U256,
    /// L1 block number for this L2 block — the `l1BlockNumber` arg of `ArbosActs.startBlock`,
    /// and the value the EVM `NUMBER` opcode returns during execution.
    pub l1_block_number: u64,
    /// Seconds elapsed since the parent block (`timeLastBlock` arg of `ArbosActs.startBlock`).
    pub time_last_block: u64,
    /// Sequencer feed sequence number for this message.
    pub sequence_number: Option<u64>,
    /// Batch poster / coinbase for the block (`message.poster`); receives the L1 poster fee.
    pub poster: Address,
}

/// Result of executing one Arbitrum transaction through the block executor.
///
/// Carries the `gas_used_for_l1` (the tx's ArbOS `poster_gas`, read off the chain context after
/// `transact`), the tx type byte (which selects the [`ArbReceiptEnvelope`] variant), and the
/// inner revm [`ResultAndState`].
#[derive(Debug)]
pub struct ArbTxResult<H> {
    /// Inner revm execution result + state delta.
    pub result: ResultAndState<H>,
    /// ArbOS L1 poster gas for this tx — the receipt's `gas_used_for_l1`.
    pub gas_used_for_l1: u64,
    /// Consensus tx type byte (selects the receipt envelope variant).
    pub tx_type: u8,
}

impl<H> TxResult for ArbTxResult<H> {
    type HaltReason = H;

    fn result(&self) -> &ResultAndState<Self::HaltReason> {
        &self.result
    }

    fn into_result(self) -> ResultAndState<Self::HaltReason> {
        self.result
    }
}

/// Block executor for Arbitrum. Mirrors `OpBlockExecutor`.
#[allow(missing_debug_implementations)]
pub struct ArbBlockExecutor<E, H = DefaultArbExecutionHooks> {
    /// The EVM (Stage B [`ArbEvm`]) the executor drives, one tx at a time.
    evm: E,
    /// Block-execution context (StartBlock prelude inputs, parent hash).
    ctx: ArbBlockExecutionCtx,
    /// `arb_revm` start-block hook set (produces the identical `ArbosActs.startBlock` calldata).
    hooks: H,
    /// Chain id (for the internal-tx env).
    chain_id: u64,
    /// Receipts of executed transactions, in order.
    receipts: Vec<ArbReceiptEnvelope<Log>>,
    /// Cumulative gas used across all executed transactions.
    gas_used: u64,
    /// Optional reth state hook, invoked after each committed state change.
    state_hook: Option<Box<dyn OnStateHook>>,
}

impl<E, H> ArbBlockExecutor<E, H> {
    /// Creates a new [`ArbBlockExecutor`].
    pub fn new(evm: E, ctx: ArbBlockExecutionCtx, hooks: H, chain_id: u64) -> Self {
        Self {
            evm,
            ctx,
            hooks,
            chain_id,
            receipts: Vec::new(),
            gas_used: 0,
            state_hook: None,
        }
    }

    /// Invokes the configured state hook, if any, with the given source + state delta.
    fn notify_state(&mut self, source: StateChangeSource, state: &revm::state::EvmState) {
        if let Some(hook) = self.state_hook.as_mut() {
            hook.on_state(source, state);
        }
    }
}

/// Builds an `ArbReceiptEnvelope<Log>` for a transaction of type `tx_type`.
///
/// The bloom is computed from the logs (so receipt encoding / receipts-root are correct), and the
/// Arbitrum-specific `gas_used_for_l1` (= the tx's `poster_gas`) is recorded on the receipt body.
fn build_arb_receipt<H>(
    tx_type: u8,
    result: ExecutionResult<H>,
    cumulative_gas_used: u64,
    gas_used_for_l1: u64,
) -> ArbReceiptEnvelope<Log> {
    let success = result.is_success();
    let logs = result.into_logs();
    let logs_bloom = logs_bloom(logs.iter());
    let receipt = ArbReceipt {
        inner: Receipt {
            status: Eip658Value::Eip658(success),
            cumulative_gas_used,
            logs,
        },
        gas_used_for_l1,
    };
    let rwb = ReceiptWithBloom { receipt, logs_bloom };
    receipt_envelope_for_type(tx_type, rwb)
}

#[inline]
fn receipt_envelope_for_type(
    tx_type: u8,
    rwb: ReceiptWithBloom<ArbReceipt<Log>>,
) -> ArbReceiptEnvelope<Log> {
    match tx_type {
        0x00 => ArbReceiptEnvelope::Legacy(rwb),
        0x01 => ArbReceiptEnvelope::Eip2930(rwb),
        0x02 => ArbReceiptEnvelope::Eip1559(rwb),
        0x03 => ArbReceiptEnvelope::Eip4844(rwb),
        0x04 => ArbReceiptEnvelope::Eip7702(rwb),
        0x64 => ArbReceiptEnvelope::Deposit(rwb),
        0x65 => ArbReceiptEnvelope::Unsigned(rwb),
        0x66 => ArbReceiptEnvelope::Contract(rwb),
        0x68 => ArbReceiptEnvelope::Retry(rwb),
        0x69 => ArbReceiptEnvelope::SubmitRetryable(rwb),
        0x6a => ArbReceiptEnvelope::Internal(rwb),
        // Unknown / future type bytes fall back to Legacy (matches alloy's bare-RLP convention).
        _ => ArbReceiptEnvelope::Legacy(rwb),
    }
}

impl<DB, I, H> BlockExecutor for ArbBlockExecutor<ArbEvm<DB, I>, H>
where
    DB: Database + DatabaseCommit + StateDB,
    I: Inspector<ArbContext<DB>>,
    H: ArbExecutionHooks,
{
    type Transaction = ArbTxEnvelope;
    type Receipt = ArbReceiptEnvelope<Log>;
    type Evm = ArbEvm<DB, I>;
    type Result = ArbTxResult<<ArbEvm<DB, I> as Evm>::HaltReason>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Mirror `execute_message`'s prelude. (1) EIP-2935 / Nitro ProcessParentBlockHash: write the
        // parent block hash into the history-storage contract under SYSTEM_ADDRESS. On pre-v40
        // chains where the contract is not installed this is a no-op state transition (and is also
        // what `execute_message` does). We commit it just like a system call.
        let result = self
            .evm
            .transact_system_call(
                SYSTEM_ADDRESS,
                HISTORY_STORAGE_ADDRESS,
                Bytes::copy_from_slice(self.ctx.parent_hash.as_slice()),
            )
            .map_err(|err| BlockExecutionError::evm(err, self.ctx.parent_hash))?;
        self.notify_state(
            StateChangeSource::PreBlock(StateChangePreBlockSource::BlockHashesContract),
            &result.state,
        );
        self.evm.db_mut().commit(result.state);

        // (2) Nitro's InternalTxStartBlock — built via `arb_revm`'s default hook so the
        // `ArbosActs.startBlock(l1BaseFee, l1BlockNumber, l2BlockNumber, timeLastBlock)` calldata is
        // byte-identical to the `execute_message` path. Driven as a typed internal tx (0x6a).
        let l2_block_number = self.evm.block().number().saturating_to::<u64>();
        let derived = ArbStartBlockDerived {
            l2_block_number,
            time_last_block: self.ctx.time_last_block,
        };
        let input = self.start_block_input(l2_block_number);
        if let Some(call) = self.hooks.start_block_prelude(&input, derived) {
            let mut tx = TxEnv::default();
            tx.tx_type = ARBITRUM_INTERNAL_TX_TYPE;
            tx.caller = call.caller;
            tx.kind = TxKind::Call(call.target);
            tx.data = call.data;
            tx.gas_limit = 0;
            tx.gas_price = 0;
            tx.nonce = 0;
            tx.chain_id = Some(self.chain_id);
            let start_block_tx = ArbTx(ArbTransaction::new(tx));

            let result = self
                .evm
                .transact_raw(start_block_tx)
                .map_err(|err| BlockExecutionError::evm(err, B256::ZERO))?;
            self.notify_state(
                StateChangeSource::PreBlock(StateChangePreBlockSource::BlockHashesContract),
                &result.state,
            );
            self.evm.db_mut().commit(result.state);
        }

        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, tx) = tx.into_parts();
        let tx_type = tx.tx().ty();

        let result = self
            .evm
            .transact(tx_env)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;

        // The ArbOS handler set `chain().poster_gas` during pre_execution of this tx; it is the
        // L2-gas equivalent of the L1 poster cost and is exactly the receipt's `gas_used_for_l1`.
        let gas_used_for_l1 = self.evm.ctx().chain.poster_gas;

        Ok(ArbTxResult {
            result,
            gas_used_for_l1,
            tx_type,
        })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        let ArbTxResult {
            result: ResultAndState { result, state },
            gas_used_for_l1,
            tx_type,
        } = output;

        self.notify_state(StateChangeSource::Transaction(self.receipts.len()), &state);

        let gas_used = result.gas_used();
        self.gas_used += gas_used;

        self.receipts.push(build_arb_receipt(
            tx_type,
            result,
            self.gas_used,
            gas_used_for_l1,
        ));

        self.evm.db_mut().commit(state);

        Ok(gas_used)
    }

    fn finish(
        self,
    ) -> Result<(Self::Evm, BlockExecutionResult<Self::Receipt>), BlockExecutionError> {
        let gas_used = self
            .receipts
            .last()
            .map(|r| r.cumulative_gas_used())
            .unwrap_or_default();
        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests: Default::default(),
                gas_used,
                blob_gas_used: 0,
            },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.state_hook = hook;
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &self.receipts
    }
}

impl<E, H> ArbBlockExecutor<E, H> {
    /// Reconstructs the `ArbExecutionInput` the `arb_revm` start-block hook expects, from the EVM
    /// env + the block-execution ctx. Only the `message` fields the hook reads are meaningful.
    fn start_block_input(&self, l2_block_number: u64) -> ArbExecutionInput {
        ArbExecutionInput::new(
            ArbParentHeader {
                number: l2_block_number.saturating_sub(1),
                ..ArbParentHeader::default()
            },
            ArbMessageEnvelope {
                sequence_number: self.ctx.sequence_number,
                l1_block_number: self.ctx.l1_block_number,
                l1_timestamp: 0,
                poster: self.ctx.poster,
                l1_base_fee_wei: self.ctx.l1_base_fee_wei,
                delayed_messages_read: 0,
                txs: Vec::new(),
            },
            ArbExecCfg {
                chain_id: self.chain_id,
                ..ArbExecCfg::default()
            },
        )
    }
}

/// Factory producing [`ArbBlockExecutor`]s. Mirrors `OpBlockExecutorFactory`.
///
/// `EvmFactory = ArbEvmFactory`, `Transaction = ArbTxEnvelope`,
/// `Receipt = ArbReceiptEnvelope<Log>`.
#[derive(Debug, Clone, Default)]
pub struct ArbBlockExecutorFactory<H = DefaultArbExecutionHooks> {
    evm_factory: ArbEvmFactory,
    hooks: H,
    chain_id: u64,
}

impl ArbBlockExecutorFactory<DefaultArbExecutionHooks> {
    /// Creates a new factory with the default Arbitrum start-block hook set.
    pub fn new(evm_factory: ArbEvmFactory, chain_id: u64) -> Self {
        Self {
            evm_factory,
            hooks: DefaultArbExecutionHooks,
            chain_id,
        }
    }
}

impl<H> ArbBlockExecutorFactory<H> {
    /// Creates a new factory with an explicit start-block hook set.
    pub const fn with_hooks(evm_factory: ArbEvmFactory, hooks: H, chain_id: u64) -> Self {
        Self {
            evm_factory,
            hooks,
            chain_id,
        }
    }

    /// The wrapped [`ArbEvmFactory`].
    pub const fn evm_factory_ref(&self) -> &ArbEvmFactory {
        &self.evm_factory
    }
}

impl<H> BlockExecutorFactory for ArbBlockExecutorFactory<H>
where
    H: ArbExecutionHooks + Clone + Debug + 'static,
{
    type EvmFactory = ArbEvmFactory;
    type ExecutionCtx<'a> = ArbBlockExecutionCtx;
    type Transaction = ArbTxEnvelope;
    type Receipt = ArbReceiptEnvelope<Log>;

    fn evm_factory(&self) -> &Self::EvmFactory {
        &self.evm_factory
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        mut evm: ArbEvm<DB, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: StateDB + 'a,
        I: Inspector<ArbContext<DB>> + 'a,
    {
        // Thread the block's L1 block number into the Arbitrum chain context, so the `NUMBER`
        // opcode (which `arb_revm` overrides to read `chain().l1_block_number`) returns the L1
        // block number, not 0. This is the Stage B/C deferral (`ArbEvmFactory::build_ctx` defaults
        // it) now resolved at the executor seam: `ConfigureEvm::context_for_block` populates
        // `ArbBlockExecutionCtx::l1_block_number` from `ArbHeaderInfo`, and it flows through here.
        evm.ctx_mut().chain.l1_block_number = ctx.l1_block_number;
        ArbBlockExecutor::new(evm, ctx, self.hooks.clone(), self.chain_id)
    }
}

// `ArbTx` must be constructible from a recovered `ArbTxEnvelope` (with and without encoded bytes)
// for the `BlockExecutor::Transaction = ArbTxEnvelope` wiring — proven by Stage B's `tx.rs`.
const _: fn() = || {
    fn assert_from<T: FromRecoveredTx<ArbTxEnvelope> + FromTxWithEncoded<ArbTxEnvelope>>() {}
    assert_from::<ArbTx>();
};

/// Block assembler for Arbitrum. Mirrors `OpBlockAssembler`.
///
/// Builds an [`ArbBlock`](arb_alloy_consensus::ArbBlock)-shaped `Block<ArbTxEnvelope>` from the
/// execution output: receipts root from the `ArbReceiptEnvelope`s, logs bloom from their logs,
/// gas used, and the post-execution state root.
///
/// Note: Arbitrum header `extra_data` / `mix_hash` carry `send_root` / `l1_block_number` /
/// `arbos_version` (decoded by `ArbHeaderInfo`); wiring those for byte-identical Nitro header
/// hashes is Stage D/E. This assembler produces a structurally-correct block with a correct
/// receipts root.
#[derive(Debug, Clone, Default)]
pub struct ArbBlockAssembler;

impl<F> BlockAssembler<F> for ArbBlockAssembler
where
    F: for<'a> BlockExecutorFactory<
            ExecutionCtx<'a> = ArbBlockExecutionCtx,
            Transaction = ArbTxEnvelope,
            Receipt = ArbReceiptEnvelope<Log>,
        >,
{
    type Block = Block<ArbTxEnvelope>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Self::Block, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            transactions,
            output:
                BlockExecutionResult {
                    receipts, gas_used, ..
                },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp().saturating_to();

        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = proofs::calculate_receipt_root(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        let header = Header {
            parent_hash: ctx.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: evm_env.block_env.beneficiary(),
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root: None,
            logs_bloom,
            difficulty: evm_env.block_env.difficulty(),
            number: evm_env.block_env.number().saturating_to(),
            gas_limit: evm_env.block_env.gas_limit(),
            gas_used: *gas_used,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao().unwrap_or_default(),
            nonce: BEACON_NONCE.into(),
            base_fee_per_gas: Some(evm_env.block_env.basefee()),
            extra_data: ctx.extra_data,
            parent_beacon_block_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_hash: None,
        };

        Ok(Block::new(
            header,
            BlockBody {
                transactions,
                ommers: Default::default(),
                withdrawals: None,
            },
        ))
    }
}

#[cfg(test)]
mod tests;
