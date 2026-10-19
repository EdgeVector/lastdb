use super::gc_atoms_delete::{GcAtomsDeleteCandidate, GcAtomsDeletePassTotals};
use super::*;
use crate::db_operations::atom_store::{AtomGcCandidate, GraceGateVerdict};

impl AtomStore {
    /// Re-read markers and bodies under the candidate locks; keep the safe ones.
    ///
    /// The tally counts each candidate the pass kept, under the first gate that
    /// kept it, so an empty delete is never silent about its reason.
    pub(super) async fn revalidate_gc_atoms_delete_candidates(
        &self,
        storage_prefix: Option<&str>,
        probe_generation: u64,
        gate: Option<&AutomaticGcAtomsGraceGate>,
        candidates: Vec<GcAtomsDeleteCandidate>,
    ) -> Result<
        (
            Vec<(GcAtomsDeleteCandidate, Vec<String>)>,
            GcAtomsRevalidationTally,
        ),
        SchemaError,
    > {
        let candidate_ids: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.uuid.clone())
            .collect();
        let retired_roots = self
            .retired_intent_atom_roots(&candidate_ids, storage_prefix)
            .await?;
        let recheck_marker_keys = candidates
            .iter()
            .map(|candidate| {
                Self::automatic_gc_atoms_reference_marker_key(storage_prefix, &candidate.uuid)
            })
            .collect::<Vec<_>>();
        let rechecked_markers = self
            .raw()
            .get_items::<AutomaticGcAtomsProbeMarker>(&recheck_marker_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "revalidate automatic gc-atoms reference markers: {e}"
                ))
            })?;
        let rechecked_bodies = self
            .raw()
            .inner()
            .get_many(
                candidates
                    .iter()
                    .map(|candidate| candidate.key.clone())
                    .collect(),
            )
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("revalidate automatic gc-atoms bodies: {e}"))
            })?;

        // The zero-count rows of the whole batch, read once. Only a gated pass
        // needs them.
        let gate_rows = match gate {
            Some(_) => {
                self.load_atom_gc_candidates(&candidate_ids, storage_prefix)
                    .await?
            }
            None => vec![None; candidate_ids.len()],
        };
        let mut validated: Vec<(GcAtomsDeleteCandidate, Vec<String>)> = Vec::new();
        let mut tally = GcAtomsRevalidationTally::default();
        for (((candidate, marker), body), row) in candidates
            .into_iter()
            .zip(rechecked_markers)
            .zip(rechecked_bodies)
            .zip(gate_rows)
        {
            let referenced = marker.is_some_and(|marker| marker.generation == probe_generation);
            let changed = body.as_ref() != Some(&candidate.value);
            if retired_roots.contains(&candidate.uuid) {
                tally.retired_intent_root += 1;
            } else if referenced {
                tally.referenced_marker += 1;
            } else if changed {
                tally.body_changed += 1;
            } else if !Self::gc_atoms_grace_gate_clears(gate, row.as_ref(), &mut tally) {
                continue;
            } else if let Some(mut keys) = self
                .gc_atoms_delete_blob_keys(storage_prefix, &candidate, &mut tally)
                .await?
            {
                if let Some(row) = row.as_ref() {
                    keys.extend(Self::atom_gc_delete_pass_keys(row, storage_prefix));
                }
                validated.push((candidate, keys));
            }
        }
        Ok((validated, tally))
    }

    /// The grace and audit gate for one candidate. Counts the reason in the
    /// tally when it keeps the candidate. An ungated pass clears everything.
    fn gc_atoms_grace_gate_clears(
        gate: Option<&AutomaticGcAtomsGraceGate>,
        row: Option<&AtomGcCandidate>,
        tally: &mut GcAtomsRevalidationTally,
    ) -> bool {
        let Some(gate) = gate else {
            return true;
        };
        let Some(row) = row else {
            tally.no_zero_epoch += 1;
            return false;
        };
        match row.grace_gate_verdict(&gate.audit_id, gate.now_unix_nanos, gate.grace_window) {
            GraceGateVerdict::Clear => true,
            GraceGateVerdict::WithinGrace => {
                tally.within_grace += 1;
                false
            }
            GraceGateVerdict::AuditNotClear => {
                tally.audit_not_clear += 1;
                false
            }
        }
    }

    /// The edge, hold and decode gates for one candidate. `None` means the
    /// candidate is kept and the tally names the gate; `Some` carries the blob
    /// edge keys that go with the atom body.
    async fn gc_atoms_delete_blob_keys(
        &self,
        storage_prefix: Option<&str>,
        candidate: &GcAtomsDeleteCandidate,
        tally: &mut GcAtomsRevalidationTally,
    ) -> Result<Option<Vec<String>>, SchemaError> {
        if !self.atom_ref_v2_reads_ready().await? {
            tally.reads_not_ready += 1;
            return Ok(None);
        }
        if self
            .has_active_atom_refs(&candidate.uuid, storage_prefix)
            .await?
        {
            tally.active_edge += 1;
            return Ok(None);
        }
        // A conflict loser or history atom can hold only a legacy v1 edge.
        // An edge that cannot be read keeps the atom: it is never a pass.
        let legacy_edge = self
            .has_active_legacy_atom_ref_edge(&candidate.uuid, storage_prefix)
            .await;
        if legacy_edge.as_ref().map_or(true, |active| *active) {
            if let Err(error) = &legacy_edge {
                tracing::warn!(
                    atom_uuid = candidate.uuid,
                    %error,
                    "automatic gc-atoms kept a candidate whose legacy edges could not be read"
                );
            }
            tally.active_edge += 1;
            return Ok(None);
        }
        if self
            .has_any_pending_atom_refs(&candidate.uuid, storage_prefix)
            .await?
        {
            tally.pending_hold += 1;
            return Ok(None);
        }
        let atom = match self.decode_atom_bytes(&candidate.value).await {
            Ok(atom) => atom,
            Err(error) => {
                tracing::warn!(
                    atom_uuid = candidate.uuid,
                    %error,
                    "automatic gc-atoms retained an unreadable candidate"
                );
                tally.unreadable += 1;
                return Ok(None);
            }
        };
        let blob_keys =
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                .into_iter()
                .map(|blob_ref| {
                    crate::db_operations::atom_store::BlobRefEdge::atom(&candidate.uuid, &blob_ref)
                        .storage_key(storage_prefix)
                })
                .collect();
        Ok(Some(blob_keys))
    }

    /// Delete the validated atoms under a ledger row; return the key count.
    pub(super) async fn delete_validated_gc_atoms(
        &self,
        storage_prefix: Option<&str>,
        probe_started_at: &str,
        validated: &[(GcAtomsDeleteCandidate, Vec<String>)],
        stamp: impl FnOnce(&mut AtomDeleteLedgerEntry),
    ) -> Result<u64, SchemaError> {
        let ledger = self
            .begin_delete_ledger_row(
                storage_prefix,
                AtomDeleteLedgerEntry::gc_atoms("automatic-gc-atoms", probe_started_at),
            )
            .await?;
        let mut delete_keys = Vec::with_capacity(validated.len() * 2);
        for (candidate, blob_keys) in validated {
            delete_keys.push(build_storage_key(
                storage_prefix,
                &crate::atom::atom_locator_codec::locator_key(&candidate.uuid),
            ));
            delete_keys.push(String::from_utf8_lossy(&candidate.key).into_owned());
            // The atom body source leaves before its derived blob edges.
            delete_keys.extend(blob_keys.iter().cloned());
        }
        let storage_keys_deleted = self.drain_gc_deletes(&mut delete_keys).await?;
        let _ = self.flush().await;
        self.commit_delete_ledger_row(ledger, |entry| {
            entry.storage_keys_deleted = storage_keys_deleted;
            stamp(entry);
        })
        .await;
        Ok(storage_keys_deleted)
    }

    /// Add one pass to the delete checkpoint and close the lap at the plane end.
    pub(super) fn advance_gc_atoms_delete_checkpoint(
        &self,
        checkpoint: &mut AutomaticGcAtomsDeleteCheckpoint,
        totals: &GcAtomsDeletePassTotals,
        next_cursor: Option<crate::storage::traits::PhysicalScanCursor>,
    ) {
        checkpoint.passes_completed = checkpoint.passes_completed.saturating_add(1);
        checkpoint.rows_scanned = checkpoint.rows_scanned.saturating_add(totals.rows_decided);
        checkpoint.candidates_revalidated = checkpoint
            .candidates_revalidated
            .saturating_add(totals.candidates_revalidated);
        checkpoint.candidates_cleared_by_revalidation = checkpoint
            .candidates_cleared_by_revalidation
            .saturating_add(totals.candidates_cleared);
        checkpoint.candidates_retained.add(&totals.retained);
        checkpoint.atoms_deleted = checkpoint
            .atoms_deleted
            .saturating_add(totals.atoms_deleted);
        checkpoint.storage_keys_deleted = checkpoint
            .storage_keys_deleted
            .saturating_add(totals.storage_keys_deleted);
        checkpoint.bytes_freed_approx = checkpoint
            .bytes_freed_approx
            .saturating_add(totals.bytes_freed_approx);
        checkpoint.cursor = next_cursor;
        if checkpoint.cursor.is_none() {
            checkpoint.phase = AutomaticGcAtomsDeletePhase::Complete;
            checkpoint.completed_at = Some(Utc::now().to_rfc3339());
            self.set_automatic_gc_atoms_generation(0);
        }
    }

    /// The report for a call that finds the delete lap already complete.
    pub(super) fn gc_atoms_delete_idle_report(
        checkpoint: &AutomaticGcAtomsDeleteCheckpoint,
    ) -> AutomaticGcAtomsDeleteReport {
        AutomaticGcAtomsDeleteReport {
            generation: checkpoint.generation,
            phase: checkpoint.phase,
            next_cursor: None,
            rows_scanned_this_call: 0,
            candidates_revalidated_this_call: 0,
            candidates_cleared_by_revalidation_this_call: 0,
            candidates_retained_this_call: GcAtomsRevalidationTally::default(),
            locks_held_ms_this_call: 0,
            atoms_deleted_this_call: 0,
            storage_keys_deleted_this_call: 0,
            bytes_freed_approx_this_call: 0,
            result: Self::automatic_gc_atoms_delete_result(checkpoint),
        }
    }
}
