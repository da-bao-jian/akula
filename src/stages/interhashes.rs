use crate::{
    accessors::{self, state::StateStorageKind},
    kv::{tables, traits::*},
    models::*,
    stagedsync::{
        stage::{ExecOutput, Stage, StageInput, UnwindInput, UnwindOutput},
        stages::*,
    },
    stages::stage_util::should_do_clean_promotion,
    trie::{increment_intermediate_hashes, regenerate_intermediate_hashes},
    StageId,
};
use anyhow::{format_err, Context};
use async_trait::async_trait;
use std::{cmp, marker::PhantomData, sync::Arc};
use tempfile::TempDir;
use tracing::info;

/// Generation of intermediate hashes for efficient computation of the state trie root
#[derive(Debug)]
pub struct Interhashes<S> {
    temp_dir: Arc<TempDir>,
    clean_promotion_threshold: u64,
    _marker: PhantomData<S>,
}

impl<S> Interhashes<S> {
    pub fn new(temp_dir: Arc<TempDir>, clean_promotion_threshold: Option<u64>) -> Self {
        Self {
            temp_dir,
            clean_promotion_threshold: clean_promotion_threshold.unwrap_or(1_000_000_000_000),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<'db, RwTx, S> Stage<'db, RwTx> for Interhashes<S>
where
    RwTx: MutableTransaction<'db>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
    tables::StorageChangeKey<S::Address>: TableObject,
{
    fn id(&self) -> StageId {
        INTERMEDIATE_HASHES
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let genesis = BlockNumber(0);
        let max_block = input
            .previous_stage
            .map(|tuple| tuple.1)
            .ok_or_else(|| format_err!("Cannot be first stage"))?;
        let past_progress = input.stage_progress.unwrap_or(genesis);

        if max_block > past_progress {
            let block_state_root = accessors::chain::header::read(
                tx,
                accessors::chain::canonical_hash::read(tx, max_block)
                    .await?
                    .ok_or_else(|| format_err!("No canonical hash for block {}", max_block))?,
                max_block,
            )
            .await?
            .ok_or_else(|| format_err!("No header for block {}", max_block))?
            .state_root;

            let trie_root = if should_do_clean_promotion(
                tx,
                genesis,
                past_progress,
                max_block,
                self.clean_promotion_threshold,
            )
            .await?
            {
                regenerate_intermediate_hashes(tx, self.temp_dir.as_ref(), Some(block_state_root))
                    .await
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                increment_intermediate_hashes::<_, S>(
                    tx,
                    self.temp_dir.as_ref(),
                    past_progress,
                    Some(block_state_root),
                )
                .await
                .with_context(|| "Failed to update interhashes")?
            };

            info!("Block #{} state root OK: {:?}", max_block, trie_root)
        };

        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(max_block, past_progress),
            done: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let _ = input;
        // TODO: proper unwind
        tx.clear_table(tables::TrieAccount).await?;
        tx.clear_table(tables::TrieStorage).await?;

        Ok(UnwindOutput {
            stage_progress: BlockNumber(0),
        })
    }
}
