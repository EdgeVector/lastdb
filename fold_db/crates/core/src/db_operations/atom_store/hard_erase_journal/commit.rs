//! Commit-time validation and apply for a hard-erase meter intent.

use super::*;

impl AtomStore {
    /// An intent written before this process started cannot be committed.
    pub(super) fn refuse_orphaned_intent(&self, key: &str) -> Result<(), SchemaError> {
        if self
            .keep_small_hard_erase_orphaned
            .lock()
            .map_err(|_| SchemaError::InvalidData("orphaned intent lock poisoned".to_string()))?
            .contains(key)
        {
            self.keep_small
                .mark_incomplete("hard_erase_orphaned_intent_reused");
            return Err(SchemaError::InvalidData(
                "hard-erase meter intent predates this process; replan the delete".to_string(),
            ));
        }
        Ok(())
    }

    /// Read and validate the intent row. `None` means a repeated confirmation
    /// that has nothing left to do.
    pub(super) async fn load_commit_candidate(
        &self,
        store: &crate::storage::TypedKvStore<dyn crate::storage::KvStore>,
        key: &str,
        delta: &KeepSmallHardEraseTotals,
    ) -> Result<Option<HardEraseJournalRow>, SchemaError> {
        let row = store
            .get_item::<HardEraseJournalRow>(key)
            .await
            .map_err(|error| {
                self.keep_small
                    .mark_incomplete("hard_erase_commit_read_failed");
                SchemaError::InvalidData(format!("read hard-erase meter intent: {error}"))
            })?
            .ok_or_else(|| {
                self.keep_small.mark_incomplete("hard_erase_intent_missing");
                SchemaError::InvalidData("hard-erase meter intent is missing".to_string())
            })?;
        if row.version != 1 || row.debit != *delta {
            self.keep_small
                .mark_incomplete("hard_erase_commit_mismatch");
            return Err(SchemaError::InvalidData(
                "hard-erase meter intent changed before commit".to_string(),
            ));
        }
        if row.reconciled {
            self.keep_small
                .mark_incomplete("hard_erase_intent_reconciled");
            return Err(SchemaError::InvalidData(
                "hard-erase meter intent was reconciled by a repair".to_string(),
            ));
        }
        if self
            .keep_small_hard_erase_fenced
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(SchemaError::InvalidData(
                "hard-erase meter journal requires repair after a failed commit".to_string(),
            ));
        }
        Ok((!row.committed).then_some(row))
    }

    /// Stamp the intent committed, flush it, then apply the debit to the
    /// live meters. The guard marks the meters incomplete if apply is cut.
    pub(super) async fn commit_intent_row(
        &self,
        store: &crate::storage::TypedKvStore<dyn crate::storage::KvStore>,
        key: &str,
        mut row: HardEraseJournalRow,
        plan: &KeepSmallHardErasePlan,
    ) -> Result<(), SchemaError> {
        let next_seq = self
            .keep_small_hard_erase_next_seq
            .load(std::sync::atomic::Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| {
                self.keep_small
                    .mark_incomplete("hard_erase_sequence_overflow");
                SchemaError::InvalidData("hard-erase journal sequence overflow".to_string())
            })?;
        self.keep_small_hard_erase_next_seq
            .store(next_seq, std::sync::atomic::Ordering::Relaxed);
        let mut apply_guard = IncompleteUnlessApplied {
            meters: &self.keep_small,
            fenced: &self.keep_small_hard_erase_fenced,
            cause: "hard_erase_commit_not_applied",
            applied: false,
        };
        row.committed = true;
        row.sequence = Some(next_seq);
        store.put_item(key, &row).await.map_err(|error| {
            self.keep_small
                .mark_incomplete("hard_erase_commit_put_failed");
            self.keep_small_hard_erase_fenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
            SchemaError::InvalidData(format!("commit hard-erase meter debit: {error}"))
        })?;
        store.inner().flush().await.map_err(|error| {
            self.keep_small
                .mark_incomplete("hard_erase_commit_flush_failed");
            self.keep_small_hard_erase_fenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
            SchemaError::InvalidData(format!("flush hard-erase meter debit: {error}"))
        })?;
        self.apply_keep_small_hard_erase(plan);
        if self
            .keep_small_hard_erase_replay_skipped
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // The live projection is a hint until a full store repair.
            // Keep the old checkpoint so a clean stop cannot prune rows
            // that this process did not replay from its dirty snapshot.
            self.keep_small
                .mark_incomplete("hard_erase_journal_requires_repair");
        } else {
            self.keep_small_hard_erase_applied_seq
                .store(next_seq, std::sync::atomic::Ordering::Relaxed);
        }
        self.keep_small_hard_erase_pending
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.keep_small_hard_erase_mutation_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        apply_guard.applied = true;
        Ok(())
    }
}
