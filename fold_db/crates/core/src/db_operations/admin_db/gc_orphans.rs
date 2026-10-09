// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Referenced-atom collection and orphan protein/atom GC.

use super::*;

impl AtomStore {
    /// Collect atom UUIDs still referenced by tips (`mk:`), tip-version chain
    /// (`tv:`), history, conflicts, and legacy `ref:` molecule blobs.
    ///
    /// `skip_tv_keys`: full storage keys of `tv:` rows that will be (or were)
    /// pruned — dry-run uses this so freeable atoms are counted without first
    /// applying the prune.
    pub(super) async fn collect_referenced_atom_uuids(
        &self,
        storage_prefix: Option<&str>,
        skip_tv_keys: &HashSet<String>,
        dry_run: bool,
    ) -> Result<HashSet<String>, SchemaError> {
        // Every scan below streams in bounded pages (see
        // [`Self::for_each_row_under_prefix`]). The reference set itself is
        // unavoidably whole-store — it is the answer — but it holds uuid
        // strings, not plane bytes: on the primary this owner measures that is
        // ~1.4M uuids against a `mk:` plane of 3.82 GiB and an `atom:` plane
        // of 3.68 GiB. Materializing those planes to build it was the cost,
        // not the set.
        let mut refs = HashSet::new();

        // Tips: mk: → PerKeyRecord.entry.atom_uuid (current head only)
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        self.for_each_row_under_prefix(&mk_prefix, "gc scan mk", dry_run, |_k, v| {
            if let Ok(val) = serde_json::from_slice::<Value>(v) {
                if let Some(u) = val.pointer("/entry/atom_uuid").and_then(|x| x.as_str()) {
                    refs.insert(u.to_string());
                }
            }
        })
        .await?;

        // Tip version chain: tv: → AtomEntry.atom_uuid (historical values).
        // Skip keys scheduled for tombstone-chain prune.
        let tv_prefix = build_storage_key(storage_prefix, "tv:");
        self.for_each_row_under_prefix(&tv_prefix, "gc scan tv", dry_run, |k, v| {
            let key = String::from_utf8_lossy(k);
            if skip_tv_keys.contains(key.as_ref()) {
                return;
            }
            if let Ok(entry) = serde_json::from_slice::<crate::atom::AtomEntry>(v) {
                refs.insert(entry.atom_uuid);
            }
        })
        .await?;

        // History events still present after clear-history.
        //
        // Only the *head* atom of each remaining history row is required for
        // current tips/as-of-at-latest. Prior heads (`old_atom_uuid`) are the
        // bulk of orphaned historical field values (e.g. pre-migration Photo
        // file_bytes). We intentionally do not pin them — use a fuller backup
        // if you need deep history reconstruction.
        let hist_prefix = build_storage_key(storage_prefix, "history:");
        self.for_each_row_under_prefix(&hist_prefix, "gc scan history", dry_run, |_k, v| {
            if let Ok(ev) = serde_json::from_slice::<MutationEvent>(v) {
                refs.insert(ev.new_atom_uuid);
                if let Some(loser) = ev.conflict_loser_atom {
                    refs.insert(loser);
                }
            }
        })
        .await?;

        // Conflict records may name atom UUIDs
        let conflict_prefix = build_storage_key(storage_prefix, "conflict:");
        self.for_each_row_under_prefix(&conflict_prefix, "gc scan conflict", dry_run, |_k, v| {
            collect_atom_uuid_strings(v, &mut refs);
        })
        .await?;

        // Legacy ref: molecule blobs
        let ref_prefix = build_storage_key(storage_prefix, "ref:");
        self.for_each_row_under_prefix(&ref_prefix, "gc scan ref", dry_run, |_k, v| {
            collect_atom_uuid_strings(v, &mut refs);
        })
        .await?;

        Ok(refs)
    }

    /// Delete empty `protein:` rows that have no `molprot:` back-ref.
    ///
    /// A live protein is reachable through its member list and through at least
    /// one molecule back-ref. Historical client probe leaks created empty
    /// proteins, failed to bind them, and left no back-ref. Those rows carry no
    /// member state and can be reclaimed without touching fold state or tips.
    pub async fn gc_orphan_proteins(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<ProteinGcReport, SchemaError> {
        let backref_prefix = build_storage_key(storage_prefix, MEMBER_BACKREF_PREFIX);
        let backref_rows = self
            .raw()
            .inner()
            .scan_prefix(backref_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc scan molprot: {e}")))?;
        let mut referenced = HashSet::new();
        let mut report = ProteinGcReport {
            dry_run,
            molprot_backrefs_scanned: backref_rows.len() as u64,
            ..ProteinGcReport::default()
        };
        for (_, value) in backref_rows {
            if let Ok(protein_uuid) = serde_json::from_slice::<String>(&value) {
                referenced.insert(protein_uuid);
            }
        }

        let protein_prefix = build_storage_key(storage_prefix, PROTEIN_RECORD_PREFIX);
        let protein_rows = self
            .raw()
            .inner()
            .scan_prefix(protein_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc scan protein: {e}")))?;

        let mut to_delete = Vec::new();
        for (key, value) in protein_rows {
            report.proteins_scanned += 1;
            let protein: Protein = serde_json::from_slice(&value).map_err(|e| {
                SchemaError::InvalidData(format!(
                    "gc decode protein {}: {e}",
                    String::from_utf8_lossy(&key)
                ))
            })?;
            if !protein.members.is_empty() {
                report.proteins_with_members += 1;
                continue;
            }
            if referenced.contains(&protein.uuid) {
                report.proteins_referenced_by_backref += 1;
                continue;
            }
            report.orphan_proteins += 1;
            report.bytes_freed_approx += key.len() as u64 + value.len() as u64;
            to_delete.push(String::from_utf8_lossy(&key).into_owned());
        }

        if !dry_run && !to_delete.is_empty() {
            const CHUNK: usize = 2000;
            for chunk in to_delete.chunks(CHUNK) {
                self.raw()
                    .batch_delete_keys(chunk.to_vec())
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("gc delete proteins: {e}")))?;
            }
            report.proteins_deleted = report.orphan_proteins;
            let _ = self.flush().await;
        }

        Ok(report)
    }

    /// GC orphan atoms + tip-version chains.
    ///
    /// 1. Drop `tv:` chains and clear `prev_tip_id` on eligible tips:
    ///    - default: only tips with `meta.tombstoned`
    ///    - `prune_live_history`: every tip with a non-empty history chain
    ///      (drops `as_of` history; reclaim path for append-heavy live records)
    /// 2. Delete `atom:` rows not referenced by any remaining tip / `tv:` /
    ///    history / conflict / legacy ref.
    pub async fn gc_orphan_atoms(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with(dry_run, storage_prefix, false)
            .await
    }

    /// Like [`Self::gc_orphan_atoms`], with optional live tip-history prune.
    pub async fn gc_orphan_atoms_with(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots(
            dry_run,
            storage_prefix,
            prune_live_history,
            &HashSet::new(),
        )
        .await
    }

    /// Like [`Self::gc_orphan_atoms_with`], plus reference roots this store
    /// cannot reach on its own.
    ///
    /// The five roots below (`mk:`, `tv:`, `history:`, `conflict:`, `ref:`) are
    /// every plane an `AtomStore` owns, but they are not every referrer. A
    /// durable pin-log record carries `field_atom_uuids` into the atom plane
    /// and has its inline bodies stripped on that basis, and that plane lives
    /// in the sync engine's own namespace. Reading an atom as an orphan
    /// because the only referrer is one this type cannot see is how an acked
    /// write loses its cloud copy — so the composition happens one layer up,
    /// in `FoldDB::gc_orphan_atoms_with`, which owns both.
    ///
    /// An empty `extra_roots` reproduces the historical behavior exactly.
    pub async fn gc_orphan_atoms_with_roots(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            Utc::now(),
        )
        .await
    }

    /// Run atom GC with a cutoff captured before a deferred-persist barrier.
    ///
    /// The background janitor captures this time before it samples pending
    /// work. A body created after the sample is therefore recent, while an
    /// older body belongs to work that the sampled watermark must drain.
    pub(crate) async fn gc_orphan_atoms_with_roots_at_cut(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        scan_started_at: DateTime<Utc>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            None,
            Some(scan_started_at),
        )
        .await
    }

    /// Like [`Self::gc_orphan_atoms_with_roots`], but only decides atom bodies
    /// whose plain header names `source_schema_name`.
    pub async fn gc_orphan_atoms_with_roots_for_schema(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        source_schema_name: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            source_schema_name,
            None,
        )
        .await
    }

    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn gc_orphan_atoms_with_roots_for_schema_at_cut(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        source_schema_name: Option<&str>,
        scan_started_at: Option<DateTime<Utc>>,
    ) -> Result<AtomGcReport, SchemaError> {
        if source_schema_name.is_some() && prune_live_history {
            return Err(SchemaError::InvalidData(
                "schema-scoped gc-atoms does not support --prune-live-history".into(),
            ));
        }
        // Taken before the FIRST read of the run. Everything below — the tip
        // chain prune, the reference-set scans, the atom scan — is a snapshot
        // of a store that keeps accepting writes throughout: `gc-atoms` takes
        // no lock and the node is not quiesced. So the reference set is only
        // authoritative for atoms that already existed at this instant; an atom
        // written afterwards has a live tip no scan below could have seen.
        // Deleting on that basis is how a *live* record loses its body while
        // its tip survives — no tombstone, no purge, no trace.
        let scan_started_at = scan_started_at.unwrap_or_else(Utc::now);

        // The prologue is bounded, so resume where the last pass stopped rather
        // than replanning the plane. `sweep_complete` restarts at the head of
        // `mk:`: chains accrue behind the cursor as records are rewritten, so a
        // finished sweep is a lap, not a terminal state.
        let mut checkpoint = if source_schema_name.is_some() {
            GcAtomsPruneCheckpoint::default()
        } else {
            self.gc_atoms_prune_checkpoint(storage_prefix).await?
        };

        // One write-ahead row covers the whole bounded prologue lap. Opening a
        // fresh row on every partial pass adds a new physical handle to the
        // same main collection the cursor must cross. With one handle visited
        // per pass, that shape can never converge. Persist the shared row key
        // before the first prune delete, then resume it until the lap finishes.
        let ledger: Option<DeleteLedgerHandle> = if dry_run {
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
            self.put_gc_atoms_prune_checkpoint(storage_prefix, &checkpoint)
                .await?;
            Some(handle)
        };

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
            self.put_gc_atoms_prune_checkpoint(storage_prefix, &checkpoint)
                .await?;
        }

        let mut report = AtomGcReport {
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
        };

        // Do not start another global plane while the prologue still has a
        // durable page ahead. The automatic caller gives the whole operation
        // 120 seconds. Returning here makes each call either reclaim bytes or
        // bank prologue progress, and prevents `gc scan mk` from consuming the
        // remainder after a bounded prune page.
        if prune.more_remaining {
            return Ok(report);
        }

        let ledger_tip_versions_pruned = checkpoint.ledger_tip_versions_pruned;
        let ledger_tips_skipped_changed = checkpoint.ledger_tips_skipped_changed;
        if !dry_run && source_schema_name.is_none() {
            // The in-memory handle still confirms this lap. Clear its durable
            // resume pointer before the atom walk, so a crash after any delete
            // leaves this row unconfirmed and the next lap opens a new row.
            checkpoint.ledger_key = None;
            checkpoint.ledger_tip_versions_pruned = 0;
            checkpoint.ledger_tips_skipped_changed = 0;
            self.put_gc_atoms_prune_checkpoint(storage_prefix, &checkpoint)
                .await?;
        }

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
        if source_schema_name.is_none() {
            report.atoms_referenced = referenced.len() as u64;
        }
        let (atom_prefix, atom_end) = Self::kind_plane_scan_bounds(storage_prefix, "atom:");

        // Deletes drain INSIDE the walk. The previous shape queued every key of
        // every orphan across the whole plane and deleted them after the scan
        // finished, so a pass that ran out of client deadline mid-walk banked
        // nothing at all — which is why the delete ledger on this owner's
        // primary held 5,778 rows and not one of them was from `gc-atoms`. A
        // drained batch is durable the moment it lands, so an interrupted pass
        // keeps the bytes it already returned and the next pass re-derives the
        // rest.
        let mut storage_keys_deleted: u64 = 0;
        let mut pending: Vec<String> = Vec::new();
        let mut cursor: Option<PhysicalScanCursor> = None;
        let mut progress = GcAtomsProgress::start("atom-walk", dry_run);
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
                if source_schema_name.is_some_and(|schema| {
                    atom_row_source_schema_name(&v).as_deref() != Some(schema)
                }) {
                    continue;
                }
                report.atoms_scanned += 1;
                progress.walked(1);
                let key = String::from_utf8_lossy(&k);
                // Parse the uuid out of EITHER key shape. Under
                // `AtomKeyEncoding::PartitionPrefix` a body key is
                // `{storage_prefix}:atom:{partition}\0{uuid}`, and the previous
                // `rsplit_once("atom:")` returned the whole `{partition}\0{uuid}`
                // tail as the "uuid". That never matches a referenced uuid, so
                // every *live* prefixed body looked like an orphan and this loop
                // would have deleted the entire atom collection on the first
                // gc-atoms call after the encoding flipped. Kind-as-partition
                // flats are `atom\0{uuid}`; `rest_of` covers both.
                let Some(uuid) = Self::atom_uuid_from_body_key(&key) else {
                    continue;
                };
                if referenced.contains(uuid) {
                    if source_schema_name.is_some() {
                        report.atoms_referenced += 1;
                    }
                    continue;
                }
                if !self.atom_ref_v2_reads_ready(storage_prefix).await? {
                    report.atoms_retained_incomplete_edges =
                        report.atoms_retained_incomplete_edges.saturating_add(1);
                    continue;
                }
                if self.has_active_atom_refs(uuid, storage_prefix).await?
                    || self.has_any_pending_atom_refs(uuid, storage_prefix).await?
                {
                    report.atoms_retained_active_edges =
                        report.atoms_retained_active_edges.saturating_add(1);
                    continue;
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
                match atom_row_created_at(&v) {
                    Some(created_at) if created_at < scan_started_at => {}
                    Some(_) => {
                        report.atoms_skipped_recent += 1;
                        continue;
                    }
                    None => {
                        report.atoms_skipped_undatable += 1;
                        continue;
                    }
                }
                // Decode before any removal so the same ordered delete can
                // retire this atom's blob edges after its source body. An
                // unreadable body may name any blob; retain it and its edges.
                let atom = match self.decode_atom_bytes(&v).await {
                    Ok(atom) => atom,
                    Err(error) => {
                        tracing::warn!(
                            atom_uuid = uuid,
                            %error,
                            "gc-atoms retained an unreadable orphan candidate"
                        );
                        continue;
                    }
                };
                let blob_edges: Vec<_> =
                    crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                        .into_iter()
                        .map(|blob_ref| {
                            crate::db_operations::atom_store::BlobRefEdge::atom(uuid, &blob_ref)
                                .storage_key(storage_prefix)
                        })
                        .collect();
                report.atoms_deleted += 1;
                report.bytes_freed_approx += k.len() as u64 + v.len() as u64;
                // A locator must not outlive the body it points at, or the
                // uuid-only read path keeps resolving to a key that is now empty.
                // Queued unconditionally: deleting an absent key is a no-op, so
                // this is also correct for a flat body that never had one.
                if !dry_run {
                    // Source removal precedes derived edge removal. LastStore
                    // preserves batch order and uses one durability barrier.
                    pending.push(key.to_string());
                    pending.extend(blob_edges);
                    pending.push(build_storage_key(
                        storage_prefix,
                        &crate::atom::atom_locator_codec::locator_key(uuid),
                    ));
                }
            }
            if !dry_run && !pending.is_empty() {
                storage_keys_deleted += self.drain_gc_deletes(&mut pending).await?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        progress.finish();

        if let Some(handle) = ledger {
            if !pending.is_empty() {
                storage_keys_deleted += self.drain_gc_deletes(&mut pending).await?;
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
        }

        Ok(report)
    }

    /// Keys per `batch_delete_keys` call, and the point at which the walk
    /// drains what it has queued.
    const GC_DELETE_BATCH: usize = 2000;

    /// Delete everything queued so far and clear the queue, returning how many
    /// storage keys went. Bounds both the batch handed to the store and the
    /// queue held between batches.
    pub(super) async fn drain_gc_deletes(
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
