//! Atom-plane walk, orphan decision and queued-delete drain for orphan atom GC.

use super::*;

impl AtomStore {
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
            let _retired_reclaim = self.retired_intent_reclaim_guard().await;
            let _target_gates = self.lock_automatic_gc_atoms(&page_uuids).await;
            let retired_roots = self
                .retired_intent_atom_roots(&page_uuids, ctx.storage_prefix)
                .await?;
            for (k, v) in page.rows {
                if ctx.source_schema_name.is_some_and(|schema| {
                    atom_row_source_schema_name(&v).as_deref() != Some(schema)
                }) {
                    continue;
                }
                report.atoms_scanned += 1;
                progress.walked(1);
                if Self::atom_uuid_from_body_key(&String::from_utf8_lossy(&k))
                    .is_some_and(|uuid| retired_roots.contains(uuid))
                {
                    report.atoms_referenced += 1;
                    continue;
                }
                self.process_gc_atom_row(ctx, &k, &v, referenced, report, pending)
                    .await?;
            }
            if !ctx.dry_run && !pending.is_empty() {
                // The walk can run for a long time, and a peer mutation can be
                // applied during it. Ask the interlock again for every page;
                // when the answer changed, drop what this page queued and stop.
                if let Err(error) = self
                    .require_no_peer_apply_for_atom_delete(ctx.allow_after_peer_apply, "gc-atoms")
                    .await
                {
                    pending.clear();
                    return Err(error);
                }
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
        if !ctx.dry_run {
            // Source removal precedes derived edge removal. LastStore
            // preserves batch order and uses one durability barrier.
            pending.push(key.to_string());
            pending.extend(blob_edges);
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
