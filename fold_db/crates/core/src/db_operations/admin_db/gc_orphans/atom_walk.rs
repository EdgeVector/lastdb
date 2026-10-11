//! Ledger, prune pass and atom-plane walk helpers for orphan atom GC.

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

    /// Walk the atom plane page by page and delete the orphans. Returns the
    /// number of storage keys deleted inside the walk.
    ///
    /// Deletes drain INSIDE the walk. The previous shape queued every key of
    /// every orphan across the whole plane and deleted them after the scan
    /// finished, so a pass that ran out of client deadline mid-walk banked
    /// nothing at all — which is why the delete ledger on this owner's
    /// primary held 5,778 rows and not one of them was from `gc-atoms`. A
    /// drained batch is durable the moment it lands, so an interrupted pass
    /// keeps the bytes it already returned and the next pass re-derives the
    /// rest.
    pub(super) async fn walk_gc_atom_plane(
        &self,
        ctx: &GcAtomWalk<'_>,
        referenced: &HashSet<String>,
        report: &mut AtomGcReport,
        pending: &mut Vec<String>,
    ) -> Result<u64, SchemaError> {
        let (atom_prefix, atom_end) = Self::kind_plane_scan_bounds(ctx.storage_prefix, "atom:");
        let mut storage_keys_deleted: u64 = 0;
        let mut cursor: Option<PhysicalScanCursor> = None;
        let mut progress = GcAtomsProgress::start("atom-walk", ctx.dry_run);
        loop {
            let page = self
                .raw()
                .inner()
                .scan_range_physical_paged(
                    atom_prefix.as_bytes(),
                    atom_end.as_bytes(),
                    cursor.as_ref(),
                    Self::GC_PLANE_SCAN_PAGE,
                    1,
                )
                .await
                .map_err(|e| SchemaError::InvalidData(format!("gc scan atom: {e}")))?;
            if page.next_cursor.is_some() && page.next_cursor == cursor {
                return Err(SchemaError::InvalidData(
                    "gc atom physical cursor did not advance".into(),
                ));
            }
            let page_uuids = page
                .rows
                .iter()
                .filter_map(|(key, _)| {
                    let key = String::from_utf8_lossy(key);
                    Self::atom_uuid_from_body_key(&key).map(str::to_string)
                })
                .collect::<Vec<_>>();
            let _target_gates = self.lock_automatic_gc_atoms(&page_uuids).await;
            for (k, v) in page.rows {
                if ctx.source_schema_name.is_some_and(|schema| {
                    atom_row_source_schema_name(&v).as_deref() != Some(schema)
                }) {
                    continue;
                }
                report.atoms_scanned += 1;
                progress.walked(1);
                self.process_gc_atom_row(ctx, &k, &v, referenced, report, pending)
                    .await?;
            }
            if !ctx.dry_run && !pending.is_empty() {
                storage_keys_deleted += self.drain_gc_deletes(pending).await?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        progress.finish();
        Ok(storage_keys_deleted)
    }

    /// Decode an orphan candidate and list its blob edge keys. Returns `None`
    /// when the body is unreadable.
    pub(super) async fn readable_gc_atom_blob_edges(
        &self,
        uuid: &str,
        v: &[u8],
        storage_prefix: Option<&str>,
    ) -> Option<Vec<String>> {
        // Decode before any removal so the same ordered delete can
        // retire this atom's blob edges after its source body. An
        // unreadable body may name any blob; retain it and its edges.
        let atom = match self.decode_atom_bytes(v).await {
            Ok(atom) => atom,
            Err(error) => {
                tracing::warn!(
                    atom_uuid = uuid,
                    %error,
                    "gc-atoms retained an unreadable orphan candidate"
                );
                return None;
            }
        };
        let blob_edges: Vec<String> =
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                .into_iter()
                .map(|blob_ref| {
                    crate::db_operations::atom_store::BlobRefEdge::atom(uuid, &blob_ref)
                        .storage_key(storage_prefix)
                })
                .collect();
        Some(blob_edges)
    }

    /// Decide one atom row of the walk and queue its deletes when it is an orphan.
    pub(super) async fn process_gc_atom_row(
        &self,
        ctx: &GcAtomWalk<'_>,
        k: &[u8],
        v: &[u8],
        referenced: &HashSet<String>,
        report: &mut AtomGcReport,
        pending: &mut Vec<String>,
    ) -> Result<(), SchemaError> {
        let key = String::from_utf8_lossy(k);
        // Parse the uuid out of EITHER key shape. Under
        // `AtomKeyEncoding::PartitionPrefix` a body key is
        // `{ctx.storage_prefix}:atom:{partition}\0{uuid}`, and the previous
        // `rsplit_once("atom:")` returned the whole `{partition}\0{uuid}`
        // tail as the "uuid". That never matches a referenced uuid, so
        // every *live* prefixed body looked like an orphan and this loop
        // would have deleted the entire atom collection on the first
        // gc-atoms call after the encoding flipped. Kind-as-partition
        // flats are `atom\0{uuid}`; `rest_of` covers both.
        let Some(uuid) = Self::atom_uuid_from_body_key(&key) else {
            return Ok(());
        };
        if referenced.contains(uuid) {
            if ctx.source_schema_name.is_some() {
                report.atoms_referenced += 1;
            }
            return Ok(());
        }
        if !self.atom_ref_v2_reads_ready(ctx.storage_prefix).await? {
            report.atoms_retained_incomplete_edges =
                report.atoms_retained_incomplete_edges.saturating_add(1);
            return Ok(());
        }
        if self.has_active_atom_refs(uuid, ctx.storage_prefix).await?
            || self
                .has_any_pending_atom_refs(uuid, ctx.storage_prefix)
                .await?
        {
            report.atoms_retained_active_edges =
                report.atoms_retained_active_edges.saturating_add(1);
            return Ok(());
        }
        // Race guard. `referenced` was gathered before this scan, so it
        // cannot name a tip written since. Age the row against the instant
        // the run started and leave anything newer alone; a genuinely
        // orphaned atom is still orphaned on the next run, but a body
        // deleted out from under a live tip is unrecoverable.
        //
        // `created_at` is a plain top-level field — content sealing only
        // touches `content` — so this reads without opening the seal, and
        // without a second store round-trip: the value bytes are in hand.
        match atom_row_created_at(v) {
            Some(created_at) if created_at < ctx.scan_started_at => {}
            Some(_) => {
                report.atoms_skipped_recent += 1;
                return Ok(());
            }
            None => {
                report.atoms_skipped_undatable += 1;
                return Ok(());
            }
        }
        let Some(blob_edges) = self
            .readable_gc_atom_blob_edges(uuid, v, ctx.storage_prefix)
            .await
        else {
            return Ok(());
        };
        report.atoms_deleted += 1;
        report.bytes_freed_approx += k.len() as u64 + v.len() as u64;
        // A locator must not outlive the body it points at, or the
        // uuid-only read path keeps resolving to a key that is now empty.
        // Queued unconditionally: deleting an absent key is a no-op, so
        // this is also correct for a flat body that never had one.
        if !ctx.dry_run {
            // Source removal precedes derived edge removal. LastStore
            // preserves batch order and uses one durability barrier.
            pending.push(key.to_string());
            pending.extend(blob_edges);
            pending.push(build_storage_key(
                ctx.storage_prefix,
                &crate::atom::atom_locator_codec::locator_key(uuid),
            ));
        }
        Ok(())
    }

    /// Keys per `batch_delete_keys` call, and the point at which the walk
    /// drains what it has queued.
    pub(super) const GC_DELETE_BATCH: usize = 2000;

    /// Delete everything queued so far and clear the queue, returning how many
    /// storage keys went. Bounds both the batch handed to the store and the
    /// queue held between batches.
    pub(in super::super) async fn drain_gc_deletes(
        &self,
        pending: &mut Vec<String>,
    ) -> Result<u64, SchemaError> {
        let mut deleted = 0u64;
        for chunk in pending.chunks(Self::GC_DELETE_BATCH) {
            self.raw()
                .batch_delete_keys(chunk.to_vec())
                .await
                .map_err(|e| SchemaError::InvalidData(format!("gc delete atoms: {e}")))?;
            deleted += chunk.len() as u64;
        }
        pending.clear();
        Ok(deleted)
    }
}
