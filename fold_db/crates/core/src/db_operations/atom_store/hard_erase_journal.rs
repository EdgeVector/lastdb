//! Small, idempotent meter debits for destructive batches.
//!
//! A pending row reaches durable storage before the delete. A committed row
//! reaches durable storage after the delete flush. An interrupted pending row
//! cannot prove whether the delete ran, so hydrate marks meter trust incomplete.
//! Rows hold aggregate amounts and a hash of a random ledger operation ID.
//! They never hold a subject key, atom UUID, tip key, or deleted content.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{AtomStore, KeepSmallHardEraseTotals};
use crate::schema::SchemaError;

const JOURNAL_PREFIX: &str = "keep_small:hard_erase_journal:";
const JOURNAL_PAGE_ROWS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HardEraseJournalRow {
    version: u32,
    committed: bool,
    #[serde(default)]
    reconciled: bool,
    /// Present only after a durable commit. Assigned under the snapshot lock.
    #[serde(default)]
    sequence: Option<u64>,
    debit: KeepSmallHardEraseTotals,
}

/// One hard erase's meter debit, resolved exactly once.
///
/// The tip debit for a molecule goes to the schema that the live meter binds
/// to that molecule. That binding is process-wide and mutable: `store_schema`
/// rebinds every molecule of the stored schema, and an expanded schema shares
/// its molecules with the schema it superseded. A purge stores its own schema
/// between the intent and the commit, so a second resolution can name a
/// different schema than the durable intent row and fail the commit forever.
/// The intent, the commit check, and the live apply all read this one plan.
#[derive(Debug, Clone)]
pub(crate) struct KeepSmallHardErasePlan {
    atom_debits: Vec<(String, String, u64)>,
    /// `(tip storage key, resolved schema, bytes)`.
    tip_debits: Vec<(String, String, u64)>,
    delta: KeepSmallHardEraseTotals,
}

impl KeepSmallHardErasePlan {
    fn is_empty(&self) -> bool {
        self.delta.by_schema.is_empty()
    }
}

pub(super) struct HardEraseJournalReplay {
    pub totals: KeepSmallHardEraseTotals,
    pub pending_keys: Vec<String>,
    pub max_seq: u64,
}

/// Fence the live meter on any cancellation or failure between a journal put
/// and its in-memory state change. The put or flush may already be durable.
struct IncompleteUnlessApplied<'a> {
    meters: &'a super::KeepSmallMeters,
    fenced: &'a std::sync::atomic::AtomicBool,
    cause: &'static str,
    applied: bool,
}

impl Drop for IncompleteUnlessApplied<'_> {
    fn drop(&mut self) {
        if !self.applied {
            self.meters.mark_incomplete(self.cause);
            self.fenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

fn journal_key(operation_id: &str) -> Result<String, SchemaError> {
    if operation_id.is_empty() {
        return Err(SchemaError::InvalidData(
            "hard-erase meter operation ID is empty".to_string(),
        ));
    }
    let digest = Sha256::digest(operation_id.as_bytes());
    Ok(format!("{JOURNAL_PREFIX}{digest:x}"))
}

fn add_debit(
    totals: &mut KeepSmallHardEraseTotals,
    delta: &KeepSmallHardEraseTotals,
) -> Result<(), SchemaError> {
    for (schema, add) in &delta.by_schema {
        let entry = totals.by_schema.entry(schema.clone()).or_default();
        for (value, amount) in [
            (&mut entry.atom_bytes, add.atom_bytes),
            (&mut entry.atom_count, add.atom_count),
            (&mut entry.tip_bytes, add.tip_bytes),
            (&mut entry.tip_count, add.tip_count),
        ] {
            *value = value.checked_add(amount).ok_or_else(|| {
                SchemaError::InvalidData("hard-erase journal total overflow".to_string())
            })?;
        }
    }
    Ok(())
}

impl AtomStore {
    pub(crate) fn hard_erase_mutation_epoch(&self) -> u64 {
        self.keep_small_hard_erase_mutation_epoch
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Resolve the debit for one hard erase. Call it once per operation and
    /// pass the same plan to the prepare and the commit.
    pub(crate) fn plan_keep_small_hard_erase(
        &self,
        fallback_schema: &str,
        atom_debits: &[(String, String, u64)],
        tip_debits: &[(String, u64)],
    ) -> Result<KeepSmallHardErasePlan, SchemaError> {
        let mut totals = KeepSmallHardEraseTotals::default();
        for (schema, _, bytes) in atom_debits {
            if *bytes == 0 {
                continue;
            }
            let entry = totals.by_schema.entry(schema.clone()).or_default();
            entry.atom_bytes = entry.atom_bytes.checked_add(*bytes).ok_or_else(|| {
                SchemaError::InvalidData("hard-erase atom byte debit overflow".to_string())
            })?;
            entry.atom_count = entry.atom_count.checked_add(1).ok_or_else(|| {
                SchemaError::InvalidData("hard-erase atom count debit overflow".to_string())
            })?;
        }
        let mut resolved_tips = Vec::with_capacity(tip_debits.len());
        for (key, bytes) in tip_debits {
            let molecule_uuid =
                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key);
            let schema = molecule_uuid
                .and_then(|uuid| self.keep_small.molecule_schema(uuid))
                .unwrap_or_else(|| fallback_schema.to_string());
            if *bytes > 0 {
                let entry = totals.by_schema.entry(schema.clone()).or_default();
                entry.tip_bytes = entry.tip_bytes.checked_add(*bytes).ok_or_else(|| {
                    SchemaError::InvalidData("hard-erase tip byte debit overflow".to_string())
                })?;
                entry.tip_count = entry.tip_count.checked_add(1).ok_or_else(|| {
                    SchemaError::InvalidData("hard-erase tip count debit overflow".to_string())
                })?;
            }
            resolved_tips.push((key.clone(), schema, *bytes));
        }
        Ok(KeepSmallHardErasePlan {
            atom_debits: atom_debits.to_vec(),
            tip_debits: resolved_tips,
            delta: totals,
        })
    }

    /// Persist an aggregate intent before any destructive store mutation.
    /// A repeated call with the same operation ID must describe the same debit.
    pub(crate) async fn prepare_keep_small_hard_erase_plan(
        &self,
        operation_id: &str,
        plan: &KeepSmallHardErasePlan,
    ) -> Result<(), SchemaError> {
        if plan.is_empty() {
            return Ok(());
        }
        let delta = &plan.delta;
        if self
            .keep_small_hard_erase_fenced
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(SchemaError::InvalidData(
                "hard-erase meter journal requires repair after a failed commit".to_string(),
            ));
        }
        let key = journal_key(operation_id)?;
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let _guard = self.keep_small_persist_lock.lock().await;
        if self
            .keep_small_hard_erase_fenced
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(SchemaError::InvalidData(
                "hard-erase meter journal requires repair after an ambiguous operation".to_string(),
            ));
        }

        let existing = store
            .get_item::<HardEraseJournalRow>(&key)
            .await
            .map_err(|error| {
                self.keep_small
                    .mark_incomplete("hard_erase_intent_read_failed");
                SchemaError::InvalidData(format!("read hard-erase meter intent: {error}"))
            })?;
        if let Some(existing) = existing {
            if existing.version != 1 || existing.debit != *delta {
                self.keep_small
                    .mark_incomplete("hard_erase_intent_mismatch");
                return Err(SchemaError::InvalidData(
                    "hard-erase meter operation ID has a different debit".to_string(),
                ));
            }
            // A prior attempt might have deleted none, some, or all of its
            // rows. The caller must inspect the store and use a new ledger ID.
            self.keep_small.mark_incomplete("hard_erase_intent_reused");
            return Err(SchemaError::InvalidData(format!(
                "hard-erase meter operation already exists (committed={}, reconciled={})",
                existing.committed, existing.reconciled
            )));
        }
        let row = HardEraseJournalRow {
            version: 1,
            committed: false,
            reconciled: false,
            sequence: None,
            debit: delta.clone(),
        };
        let mut intent_guard = IncompleteUnlessApplied {
            meters: &self.keep_small,
            fenced: &self.keep_small_hard_erase_fenced,
            cause: "hard_erase_intent_not_confirmed",
            applied: false,
        };
        store.put_item(&key, &row).await.map_err(|error| {
            self.keep_small
                .mark_incomplete("hard_erase_intent_put_failed");
            SchemaError::InvalidData(format!("persist hard-erase meter intent: {error}"))
        })?;
        store.inner().flush().await.map_err(|error| {
            self.keep_small
                .mark_incomplete("hard_erase_intent_flush_failed");
            SchemaError::InvalidData(format!("flush hard-erase meter intent: {error}"))
        })?;
        self.keep_small_hard_erase_pending
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.keep_small_hard_erase_mutation_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        intent_guard.applied = true;
        Ok(())
    }

    /// Confirm an intent after the destructive store flush. A repeated
    /// confirmation is a point-read no-op, so a retry cannot double debit.
    pub(crate) async fn commit_keep_small_hard_erase_plan(
        &self,
        operation_id: &str,
        plan: &KeepSmallHardErasePlan,
    ) -> Result<(), SchemaError> {
        if plan.is_empty() {
            return Ok(());
        }
        let delta = &plan.delta;
        let key = journal_key(operation_id)?;
        let _guard = self.keep_small_persist_lock.lock().await;
        if self
            .keep_small_hard_erase_orphaned
            .lock()
            .map_err(|_| SchemaError::InvalidData("orphaned intent lock poisoned".to_string()))?
            .contains(&key)
        {
            self.keep_small
                .mark_incomplete("hard_erase_orphaned_intent_reused");
            return Err(SchemaError::InvalidData(
                "hard-erase meter intent predates this process; replan the delete".to_string(),
            ));
        }
        if let Some(store) = self.keep_small_persist.as_ref() {
            let mut row = store
                .get_item::<HardEraseJournalRow>(&key)
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
            if row.committed {
                return Ok(());
            }
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
            store.put_item(&key, &row).await.map_err(|error| {
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
        } else {
            self.apply_keep_small_hard_erase(plan);
        }
        // The journal is durable. Only a safe checkpoint or shutdown may
        // write the large snapshot; erase traffic alone must not trigger it.
        self.keep_small_dirty
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn apply_keep_small_hard_erase(&self, plan: &KeepSmallHardErasePlan) {
        for (schema, atom_uuid, bytes) in &plan.atom_debits {
            self.keep_small.forget_pending_atom(atom_uuid);
            if *bytes > 0 {
                self.keep_small.record_atom_delete(schema, *bytes);
            }
        }
        for (key, schema, bytes) in &plan.tip_debits {
            let molecule_uuid =
                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key);
            if *bytes > 0 {
                self.keep_small
                    .record_tip_put(Some(schema.as_str()), 0, *bytes);
                if let Some(molecule_uuid) = molecule_uuid {
                    self.keep_small
                        .record_molecule_tip_delete(molecule_uuid, key, *bytes);
                }
            }
            self.keep_small.forget_key(key);
        }
    }

    /// Page through small journal rows. Never materialize the whole journal.
    pub(super) async fn read_keep_small_hard_erase_journal(
        &self,
        checkpoint_seq: u64,
    ) -> Result<HardEraseJournalReplay, SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(HardEraseJournalReplay {
                totals: KeepSmallHardEraseTotals::default(),
                pending_keys: Vec::new(),
                max_seq: checkpoint_seq,
            });
        };
        let (prefix, end) = crate::kind_partition::exact_prefix_bounds(JOURNAL_PREFIX);
        let mut cursor = prefix.into_bytes();
        let mut totals = KeepSmallHardEraseTotals::default();
        let mut pending_keys = Vec::new();
        let mut max_seq = checkpoint_seq;
        loop {
            let rows = store
                .inner()
                .scan_range_paged(&cursor, end.as_bytes(), JOURNAL_PAGE_ROWS)
                .await
                .map_err(|error| {
                    self.keep_small
                        .mark_incomplete("hard_erase_journal_scan_failed");
                    SchemaError::InvalidData(format!("scan hard-erase meter journal: {error}"))
                })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < JOURNAL_PAGE_ROWS;
            for (key, value) in &rows {
                if key == &cursor && cursor.as_slice() != JOURNAL_PREFIX.as_bytes() {
                    continue;
                }
                let row: HardEraseJournalRow = serde_json::from_slice(value).map_err(|error| {
                    self.keep_small
                        .mark_incomplete("hard_erase_journal_decode_failed");
                    SchemaError::InvalidData(format!("decode hard-erase meter journal: {error}"))
                })?;
                if row.version != 1 {
                    self.keep_small
                        .mark_incomplete("hard_erase_journal_version_unknown");
                    return Err(SchemaError::InvalidData(
                        "hard-erase meter journal has an unknown version".to_string(),
                    ));
                }
                if row.committed {
                    let sequence = row.sequence.ok_or_else(|| {
                        self.keep_small
                            .mark_incomplete("hard_erase_sequence_missing");
                        SchemaError::InvalidData(
                            "committed hard-erase journal row has no sequence".to_string(),
                        )
                    })?;
                    max_seq = max_seq.max(sequence);
                    if sequence > checkpoint_seq {
                        add_debit(&mut totals, &row.debit)?;
                    }
                } else if !row.reconciled {
                    pending_keys.push(String::from_utf8(key.clone()).map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "hard-erase journal key is not UTF-8: {error}"
                        ))
                    })?);
                }
            }
            if exhausted {
                break;
            }
            cursor = rows.last().expect("nonempty page").0.clone();
        }
        Ok(HardEraseJournalReplay {
            totals,
            pending_keys,
            max_seq,
        })
    }

    /// A full liveness repair measured the authoritative store. Retire only
    /// intents inherited from the prior process, after its repaired snapshot
    /// is durable. Current-process intents may still be in destructive work.
    pub(super) async fn reconcile_orphaned_hard_erase_intents(&self) -> Result<(), SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let keys: Vec<String> = self
            .keep_small_hard_erase_orphaned
            .lock()
            .map_err(|_| SchemaError::InvalidData("orphaned intent lock poisoned".to_string()))?
            .iter()
            .cloned()
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        for key in &keys {
            let mut row = store
                .get_item::<HardEraseJournalRow>(key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("read orphaned meter intent: {error}"))
                })?
                .ok_or_else(|| {
                    SchemaError::InvalidData("orphaned meter intent disappeared".to_string())
                })?;
            if row.committed {
                // A caller resumed the old ID against policy. The repaired
                // snapshot may not include its debit, so fail closed.
                self.keep_small
                    .mark_incomplete("orphaned_intent_committed_during_repair");
                return Err(SchemaError::InvalidData(
                    "orphaned meter intent changed during repair".to_string(),
                ));
            }
            row.reconciled = true;
            store.put_item(key, &row).await.map_err(|error| {
                SchemaError::InvalidData(format!("reconcile orphaned meter intent: {error}"))
            })?;
        }
        store.inner().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("flush reconciled meter intents: {error}"))
        })?;
        self.keep_small_hard_erase_orphaned
            .lock()
            .map_err(|_| SchemaError::InvalidData("orphaned intent lock poisoned".to_string()))?
            .clear();
        Ok(())
    }

    /// The caller holds the persist lock and already flushed a snapshot with
    /// `checkpoint_seq`. A crash after any delete below remains safe: the
    /// snapshot includes each removed debit, and hydrate ignores surviving
    /// rows at or below the checkpoint.
    pub(super) async fn prune_hard_erase_journal_locked(
        &self,
        checkpoint_seq: u64,
        max_rows: Option<usize>,
    ) -> Result<usize, SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(0);
        };
        let (prefix, end) = crate::kind_partition::exact_prefix_bounds(JOURNAL_PREFIX);
        let mut cursor = prefix.into_bytes();
        let mut pruned = 0usize;
        loop {
            let rows = store
                .inner()
                .scan_range_paged(&cursor, end.as_bytes(), JOURNAL_PAGE_ROWS)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("scan journal for checkpoint prune: {error}"))
                })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < JOURNAL_PAGE_ROWS;
            let mut deletes = Vec::new();
            for (key, value) in &rows {
                if key == &cursor && cursor.as_slice() != JOURNAL_PREFIX.as_bytes() {
                    continue;
                }
                let row: HardEraseJournalRow = serde_json::from_slice(value).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "decode journal for checkpoint prune: {error}"
                    ))
                })?;
                if row.reconciled
                    || (row.committed && row.sequence.is_some_and(|seq| seq <= checkpoint_seq))
                {
                    deletes.push(key.clone());
                    if max_rows.is_some_and(|limit| pruned + deletes.len() >= limit) {
                        break;
                    }
                }
            }
            let stop_after_batch = max_rows.is_some_and(|limit| pruned + deletes.len() >= limit);
            if !deletes.is_empty() {
                pruned += deletes.len();
                store.inner().batch_delete(deletes).await.map_err(|error| {
                    SchemaError::InvalidData(format!("prune checkpointed meter journal: {error}"))
                })?;
            }
            if stop_after_batch || exhausted {
                break;
            }
            cursor = rows.last().expect("nonempty page").0.clone();
        }
        Ok(pruned)
    }
}
