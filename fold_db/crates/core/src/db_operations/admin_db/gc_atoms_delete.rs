//! Automatic `gc-atoms` delete phase: checkpoint, persistence and the orphan-atom delete from a probe.

use super::*;

impl AtomStore {
    /// Load the bounded automatic atom-delete checkpoint.
    pub async fn automatic_gc_atoms_delete_checkpoint(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<AutomaticGcAtomsDeleteCheckpoint, SchemaError> {
        let key = build_storage_key(storage_prefix, GC_ATOMS_DELETE_CHECKPOINT_KEY);
        let raw = self.raw().get_item(&key).await.map_err(|e| {
            SchemaError::InvalidData(format!("load automatic gc-atoms delete: {e}"))
        })?;
        Ok(raw.unwrap_or_default())
    }

    pub(super) async fn persist_automatic_gc_atoms_delete(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &AutomaticGcAtomsDeleteCheckpoint,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, GC_ATOMS_DELETE_CHECKPOINT_KEY);
        self.raw()
            .put_item(&key, checkpoint)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("store automatic gc-atoms delete: {e}")))
    }

    pub(super) fn automatic_gc_atoms_delete_result(
        checkpoint: &AutomaticGcAtomsDeleteCheckpoint,
    ) -> Option<AutomaticGcAtomsDeleteResult> {
        (checkpoint.phase == AutomaticGcAtomsDeletePhase::Complete).then(|| {
            AutomaticGcAtomsDeleteResult {
                generation: checkpoint.generation,
                completed_at: checkpoint.completed_at.clone().unwrap_or_default(),
                passes_completed: checkpoint.passes_completed,
                rows_scanned: checkpoint.rows_scanned,
                candidates_revalidated: checkpoint.candidates_revalidated,
                candidates_cleared_by_revalidation: checkpoint.candidates_cleared_by_revalidation,
                atoms_deleted: checkpoint.atoms_deleted,
                storage_keys_deleted: checkpoint.storage_keys_deleted,
                bytes_freed_approx: checkpoint.bytes_freed_approx,
            }
        })
    }

    /// Delete one bounded batch from a completed automatic probe generation.
    ///
    /// The manual `gc-atoms` path is unchanged. This path advances a physical
    /// cursor, stops after `candidate_cap` orphan candidates, and records one
    /// write-ahead delete-ledger row for each batch that returns bytes.
    ///
    /// Candidate liveness is checked again under the same per-atom locks used
    /// by body and tip writers. A writer that reuses an old content-addressed
    /// atom writes the active generation marker in its durable batch. It either
    /// wins the lock and clears the candidate, or follows the delete and writes
    /// the body back before it can publish the tip.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn delete_orphan_atoms_from_probe(
        &self,
        options: AutomaticGcAtomsDeleteOptions,
    ) -> Result<AutomaticGcAtomsDeleteReport, SchemaError> {
        #[derive(Clone)]
        struct Candidate {
            key: Vec<u8>,
            value: Vec<u8>,
            uuid: String,
        }

        let storage_prefix = options.storage_prefix.as_deref();
        let max_rows = options
            .max_rows
            .unwrap_or(Self::GC_PLANE_SCAN_PAGE)
            .clamp(1, Self::GC_PLANE_SCAN_PAGE);
        let candidate_cap = options.candidate_cap.max(1);
        let probe = self
            .automatic_gc_atoms_probe_checkpoint(storage_prefix)
            .await?;
        if probe.version != 2 || probe.phase != AutomaticGcAtomsProbePhase::Complete {
            return Err(SchemaError::InvalidData(format!(
                "automatic gc-atoms delete requires a complete v2 probe; generation {} version {} is {:?}",
                probe.generation, probe.version, probe.phase
            )));
        }
        let probe_started_at = DateTime::parse_from_rfc3339(&probe.started_at)
            .map_err(|e| {
                SchemaError::InvalidData(format!("invalid automatic gc-atoms start time: {e}"))
            })?
            .with_timezone(&Utc);
        self.set_automatic_gc_atoms_generation(probe.generation);

        let mut checkpoint = self
            .automatic_gc_atoms_delete_checkpoint(storage_prefix)
            .await?;
        if checkpoint.version == 0 || checkpoint.generation != probe.generation {
            checkpoint = AutomaticGcAtomsDeleteCheckpoint {
                version: 1,
                generation: probe.generation,
                phase: AutomaticGcAtomsDeletePhase::Atoms,
                ..Default::default()
            };
        } else if checkpoint.version != 1 {
            return Err(SchemaError::InvalidData(format!(
                "unsupported automatic gc-atoms delete checkpoint version {}",
                checkpoint.version
            )));
        }
        if checkpoint.phase == AutomaticGcAtomsDeletePhase::Complete {
            self.set_automatic_gc_atoms_generation(0);
            return Ok(AutomaticGcAtomsDeleteReport {
                generation: checkpoint.generation,
                phase: checkpoint.phase,
                next_cursor: None,
                rows_scanned_this_call: 0,
                candidates_revalidated_this_call: 0,
                candidates_cleared_by_revalidation_this_call: 0,
                atoms_deleted_this_call: 0,
                storage_keys_deleted_this_call: 0,
                bytes_freed_approx_this_call: 0,
                result: Self::automatic_gc_atoms_delete_result(&checkpoint),
            });
        }

        let (atom_prefix, atom_end) = Self::kind_plane_scan_bounds(storage_prefix, "atom:");
        let page = self
            .raw()
            .inner()
            .scan_range_physical_paged(
                atom_prefix.as_bytes(),
                atom_end.as_bytes(),
                checkpoint.cursor.as_ref(),
                max_rows,
                1,
            )
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("automatic gc-atoms delete scan: {e}"))
            })?;
        let marker_keys = page
            .rows
            .iter()
            .map(|(key, _)| {
                let key = String::from_utf8_lossy(key);
                let uuid = Self::atom_uuid_from_body_key(&key).unwrap_or_default();
                Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid)
            })
            .collect::<Vec<_>>();
        let markers = self
            .raw()
            .get_items::<AutomaticGcAtomsProbeMarker>(&marker_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "read automatic gc-atoms delete reference markers: {e}"
                ))
            })?;

        let mut candidates = Vec::new();
        let mut rows_decided = 0usize;
        let mut last_decided_key = None;
        let mut atoms_skipped_recent = 0u64;
        let mut atoms_skipped_undatable = 0u64;
        for ((key, value), marker) in page.rows.iter().zip(markers.iter()) {
            if candidates.len() >= candidate_cap {
                break;
            }
            rows_decided += 1;
            last_decided_key = Some(key.clone());
            let key_text = String::from_utf8_lossy(key);
            let Some(uuid) = Self::atom_uuid_from_body_key(&key_text) else {
                continue;
            };
            if marker
                .as_ref()
                .is_some_and(|marker| marker.generation == probe.generation)
            {
                continue;
            }
            match atom_row_created_at(value) {
                Some(created_at) if created_at < probe_started_at => {
                    candidates.push(Candidate {
                        key: key.clone(),
                        value: value.clone(),
                        uuid: uuid.to_string(),
                    });
                }
                Some(_) => atoms_skipped_recent = atoms_skipped_recent.saturating_add(1),
                None => atoms_skipped_undatable = atoms_skipped_undatable.saturating_add(1),
            }
        }

        let stopped_by_cap = rows_decided < page.rows.len();
        let next_cursor = if stopped_by_cap {
            page.row_handle.clone().map(|mut cursor| {
                cursor.after_key = last_decided_key.clone();
                cursor
            })
        } else {
            page.next_cursor.clone()
        };

        let candidate_uuids = candidates
            .iter()
            .map(|candidate| candidate.uuid.clone())
            .collect::<Vec<_>>();
        let _candidate_guards = self.lock_automatic_gc_atoms(&candidate_uuids).await;
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

        let mut validated: Vec<(Candidate, Vec<String>)> = Vec::new();
        for ((candidate, marker), body) in candidates
            .into_iter()
            .zip(rechecked_markers)
            .zip(rechecked_bodies)
        {
            if marker.is_some_and(|marker| marker.generation == probe.generation)
                || body.as_ref() != Some(&candidate.value)
            {
                continue;
            }
            if !self.atom_ref_v2_reads_ready(storage_prefix).await?
                || self
                    .has_active_atom_refs(&candidate.uuid, storage_prefix)
                    .await?
                || self
                    .has_any_pending_atom_refs(&candidate.uuid, storage_prefix)
                    .await?
            {
                continue;
            }
            let atom = match self.decode_atom_bytes(&candidate.value).await {
                Ok(atom) => atom,
                Err(error) => {
                    tracing::warn!(
                        atom_uuid = candidate.uuid,
                        %error,
                        "automatic gc-atoms retained an unreadable candidate"
                    );
                    continue;
                }
            };
            let blob_keys =
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                    .into_iter()
                    .map(|blob_ref| {
                        crate::db_operations::atom_store::BlobRefEdge::atom(
                            &candidate.uuid,
                            &blob_ref,
                        )
                        .storage_key(storage_prefix)
                    })
                    .collect();
            validated.push((candidate, blob_keys));
        }

        let candidates_revalidated_this_call = candidate_uuids.len() as u64;
        let candidates_cleared_by_revalidation_this_call =
            candidates_revalidated_this_call.saturating_sub(validated.len() as u64);
        let atoms_deleted_this_call = validated.len() as u64;
        let bytes_freed_approx_this_call = validated.iter().fold(0u64, |total, (candidate, _)| {
            total.saturating_add(candidate.key.len() as u64 + candidate.value.len() as u64)
        });
        let mut storage_keys_deleted_this_call = 0u64;
        if !validated.is_empty() {
            let ledger = self
                .begin_delete_ledger_row(
                    storage_prefix,
                    AtomDeleteLedgerEntry::gc_atoms("automatic-gc-atoms", &probe.started_at),
                )
                .await?;
            let mut delete_keys = Vec::with_capacity(validated.len() * 2);
            for (candidate, blob_keys) in &validated {
                delete_keys.push(build_storage_key(
                    storage_prefix,
                    &crate::atom::atom_locator_codec::locator_key(&candidate.uuid),
                ));
                delete_keys.push(String::from_utf8_lossy(&candidate.key).into_owned());
                // The atom body source leaves before its derived blob edges.
                delete_keys.extend(blob_keys.iter().cloned());
            }
            storage_keys_deleted_this_call = self.drain_gc_deletes(&mut delete_keys).await?;
            let _ = self.flush().await;
            self.commit_delete_ledger_row(ledger, |entry| {
                entry.atoms_deleted = atoms_deleted_this_call;
                entry.storage_keys_deleted = storage_keys_deleted_this_call;
                entry.atoms_scanned = rows_decided as u64;
                entry.atoms_skipped_recent = atoms_skipped_recent;
                entry.atoms_skipped_undatable = atoms_skipped_undatable;
            })
            .await;
        }

        checkpoint.passes_completed = checkpoint.passes_completed.saturating_add(1);
        checkpoint.rows_scanned = checkpoint.rows_scanned.saturating_add(rows_decided as u64);
        checkpoint.candidates_revalidated = checkpoint
            .candidates_revalidated
            .saturating_add(candidates_revalidated_this_call);
        checkpoint.candidates_cleared_by_revalidation = checkpoint
            .candidates_cleared_by_revalidation
            .saturating_add(candidates_cleared_by_revalidation_this_call);
        checkpoint.atoms_deleted = checkpoint
            .atoms_deleted
            .saturating_add(atoms_deleted_this_call);
        checkpoint.storage_keys_deleted = checkpoint
            .storage_keys_deleted
            .saturating_add(storage_keys_deleted_this_call);
        checkpoint.bytes_freed_approx = checkpoint
            .bytes_freed_approx
            .saturating_add(bytes_freed_approx_this_call);
        checkpoint.cursor = next_cursor;
        if checkpoint.cursor.is_none() {
            checkpoint.phase = AutomaticGcAtomsDeletePhase::Complete;
            checkpoint.completed_at = Some(Utc::now().to_rfc3339());
            self.set_automatic_gc_atoms_generation(0);
        }
        self.persist_automatic_gc_atoms_delete(storage_prefix, &checkpoint)
            .await?;

        Ok(AutomaticGcAtomsDeleteReport {
            generation: checkpoint.generation,
            phase: checkpoint.phase,
            next_cursor: checkpoint.cursor.clone(),
            rows_scanned_this_call: rows_decided as u64,
            candidates_revalidated_this_call,
            candidates_cleared_by_revalidation_this_call,
            atoms_deleted_this_call,
            storage_keys_deleted_this_call,
            bytes_freed_approx_this_call,
            result: Self::automatic_gc_atoms_delete_result(&checkpoint),
        })
    }
}
