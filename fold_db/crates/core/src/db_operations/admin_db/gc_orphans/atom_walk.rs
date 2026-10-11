//! Ledger and prune-pass helpers for orphan atom GC.

use super::*;

impl AtomStore {
    /// Open or resume the delete ledger row for one `gc-atoms` run.
    pub(super) async fn open_gc_atoms_ledger(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        source_schema_name: Option<&str>,
        scan_started_at: DateTime<Utc>,
        checkpoint: &mut GcAtomsPruneCheckpoint,
    ) -> Result<Option<DeleteLedgerHandle>, SchemaError> {
        let ledger = if dry_run {
            None
        } else if source_schema_name.is_some() {
            let entry = AtomDeleteLedgerEntry::gc_atoms("gc-atoms", &scan_started_at.to_rfc3339());
            Some(self.begin_delete_ledger_row(storage_prefix, entry).await?)
        } else if let Some(key) = checkpoint.ledger_key.as_deref() {
            Some(self.resume_delete_ledger_row(key).await?)
        } else {
            let entry = AtomDeleteLedgerEntry::gc_atoms("gc-atoms", &scan_started_at.to_rfc3339());
            let handle = self.begin_delete_ledger_row(storage_prefix, entry).await?;
            checkpoint.ledger_key = Some(handle.key().to_string());
            checkpoint.ledger_tip_versions_pruned = 0;
            checkpoint.ledger_tips_skipped_changed = 0;
            self.put_gc_atoms_prune_checkpoint(storage_prefix, checkpoint)
                .await?;
            Some(handle)
        };
        Ok(ledger)
    }

    /// Run one bounded tip-version prune pass and bank its checkpoint.
    pub(super) async fn run_gc_atoms_prune_pass(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        source_schema_name: Option<&str>,
        scan_started_at: DateTime<Utc>,
        checkpoint: &mut GcAtomsPruneCheckpoint,
    ) -> Result<TipVersionPrunePlan, SchemaError> {
        let prune = if source_schema_name.is_some() {
            TipVersionPrunePlan::default()
        } else {
            let resume_at = if checkpoint.sweep_complete {
                None
            } else {
                checkpoint.physical_cursor.clone()
            };
            self.prune_tip_version_chains(
                dry_run,
                storage_prefix,
                prune_live_history,
                resume_at.as_ref(),
                Self::gc_prune_pass_max_keys(),
            )
            .await?
        };

        // Advance the durable cursor before the reference scan and the atom
        // walk, which are the long parts: the prune's writes are already
        // durable, and a pass killed later must not repeat them.
        if !dry_run && source_schema_name.is_none() {
            checkpoint.passes_completed = checkpoint.passes_completed.saturating_add(1);
            checkpoint.tips_chain_cleared_total = checkpoint
                .tips_chain_cleared_total
                .saturating_add(prune.tips_chain_cleared);
            checkpoint.tip_versions_pruned_total = checkpoint
                .tip_versions_pruned_total
                .saturating_add(prune.tip_versions_pruned);
            checkpoint.tips_skipped_changed_total = checkpoint
                .tips_skipped_changed_total
                .saturating_add(prune.tips_skipped_changed);
            checkpoint.ledger_tip_versions_pruned = checkpoint
                .ledger_tip_versions_pruned
                .saturating_add(prune.tip_versions_pruned);
            checkpoint.ledger_tips_skipped_changed = checkpoint
                .ledger_tips_skipped_changed
                .saturating_add(prune.tips_skipped_changed);
            checkpoint.last_pass_at = Some(scan_started_at.to_rfc3339());
            checkpoint.after_key = prune.next_after_key.clone();
            checkpoint.physical_cursor = prune.next_physical_cursor.clone();
            checkpoint.sweep_complete = !prune.more_remaining;
            self.put_gc_atoms_prune_checkpoint(storage_prefix, checkpoint)
                .await?;
        }

        Ok(prune)
    }

    /// Build the report that starts from the prune pass counters.
    pub(super) fn gc_atoms_initial_report(
        dry_run: bool,
        source_schema_name: Option<&str>,
        scan_started_at: DateTime<Utc>,
        prune: &TipVersionPrunePlan,
    ) -> AtomGcReport {
        AtomGcReport {
            dry_run,
            schema: source_schema_name.map(str::to_string),
            atoms_scanned: 0,
            atoms_referenced: 0,
            atoms_deleted: 0,
            bytes_freed_approx: 0,
            tips_chain_cleared: prune.tips_chain_cleared,
            tip_versions_pruned: prune.tip_versions_pruned,
            tip_version_bytes_approx: prune.tip_version_bytes_approx,
            tips_skipped_changed: prune.tips_skipped_changed,
            scan_started_at: scan_started_at.to_rfc3339(),
            atoms_skipped_recent: 0,
            atoms_skipped_undatable: 0,
            atoms_retained_incomplete_edges: 0,
            atoms_retained_active_edges: 0,
            tips_scanned: prune.keys_scanned,
            prune_more_remaining: prune.more_remaining,
            prune_next_after_key: prune.next_after_key.clone(),
            prune_physical_handles_visited: prune.physical_handles_visited,
            prune_physical_cold_shard_loads: prune.physical_cold_shard_loads,
        }
    }

    /// Clear the durable ledger resume pointer before the atom walk.
    pub(super) async fn clear_gc_atoms_ledger_resume(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &mut GcAtomsPruneCheckpoint,
    ) -> Result<(), SchemaError> {
        // The in-memory handle still confirms this lap. Clear its durable
        // resume pointer before the atom walk, so a crash after any delete
        // leaves this row unconfirmed and the next lap opens a new row.
        checkpoint.ledger_key = None;
        checkpoint.ledger_tip_versions_pruned = 0;
        checkpoint.ledger_tips_skipped_changed = 0;
        self.put_gc_atoms_prune_checkpoint(storage_prefix, checkpoint)
            .await?;
        Ok(())
    }

    /// Collect the referenced atom UUIDs, including the roots the caller supplies.
    pub(super) async fn collect_gc_atoms_roots(
        &self,
        storage_prefix: Option<&str>,
        dry_run: bool,
        prune: &TipVersionPrunePlan,
        extra_roots: &HashSet<String>,
    ) -> Result<HashSet<String>, SchemaError> {
        // dry_run: skip planned-delete tv keys when counting refs so freeable
        // atoms show up without applying the prune first.
        // execute: prune already applied; skip set is empty of live keys.
        let skip = if dry_run {
            &prune.tv_keys_skip
        } else {
            // After execute, those keys are gone; empty skip is fine.
            &prune.tv_keys_skip
        };
        let mut referenced = self
            .collect_referenced_atom_uuids(storage_prefix, skip, dry_run)
            .await?;
        // Roots from planes this type cannot scan (today: the sync engine's
        // durable pin log). Folded in before the count so `atoms_referenced`
        // stays the number of bodies this pass treated as live.
        referenced.extend(extra_roots.iter().cloned());
        Ok(referenced)
    }

    /// Drain the last queued deletes and commit the `gc-atoms` ledger row.
    pub(super) async fn finish_gc_atoms_ledger(
        &self,
        handle: DeleteLedgerHandle,
        pending: &mut Vec<String>,
        mut storage_keys_deleted: u64,
        report: &AtomGcReport,
        (ledger_tip_versions_pruned, ledger_tips_skipped_changed): (u64, u64),
    ) -> Result<(), SchemaError> {
        if !pending.is_empty() {
            storage_keys_deleted += self.drain_gc_deletes(pending).await?;
        }
        if storage_keys_deleted > 0 {
            let _ = self.flush().await;
        }
        let final_report = report.clone();
        self.commit_delete_ledger_row(handle, move |entry| {
            entry.atoms_deleted = final_report.atoms_deleted;
            entry.storage_keys_deleted = storage_keys_deleted;
            entry.atoms_scanned = final_report.atoms_scanned;
            entry.atoms_skipped_recent = final_report.atoms_skipped_recent;
            entry.atoms_skipped_undatable = final_report.atoms_skipped_undatable;
            // One row spans every bounded prologue pass in this lap.
            entry.tip_versions_pruned = ledger_tip_versions_pruned;
            entry.tips_skipped_changed = ledger_tips_skipped_changed;
        })
        .await;
        Ok(())
    }
}
