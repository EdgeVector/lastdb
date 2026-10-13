//! Automatic `gc-atoms` delete phase: checkpoint, persistence and the orphan-atom delete from a probe.

use super::*;

#[derive(Clone)]
pub(super) struct GcAtomsDeleteCandidate {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
    pub(super) uuid: String,
}

/// The candidates and counters from one page of the delete scan.
pub(super) struct GcAtomsDeleteSelection {
    pub(super) candidates: Vec<GcAtomsDeleteCandidate>,
    pub(super) rows_decided: usize,
    pub(super) atoms_skipped_recent: u64,
    pub(super) atoms_skipped_undatable: u64,
    pub(super) next_cursor: Option<crate::storage::traits::PhysicalScanCursor>,
}

/// The counters of one delete pass.
pub(super) struct GcAtomsDeletePassTotals {
    pub(super) rows_decided: u64,
    pub(super) candidates_revalidated: u64,
    pub(super) candidates_cleared: u64,
    pub(super) retained: GcAtomsRevalidationTally,
    pub(super) atoms_deleted: u64,
    pub(super) storage_keys_deleted: u64,
    pub(super) bytes_freed_approx: u64,
}

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
    ///
    /// A gated pass (`AutomaticGcAtomsDeleteOptions::gated`) deletes a candidate
    /// only when a zero-count row names it, that zero epoch is older than the
    /// grace window and the gate's audit cleared the epoch. The byte cap only
    /// decided that this pass should start. `ungated_emergency` is the
    /// operator's full-disk switch.
    pub async fn delete_orphan_atoms_from_probe(
        &self,
        options: AutomaticGcAtomsDeleteOptions,
    ) -> Result<AutomaticGcAtomsDeleteReport, SchemaError> {
        let storage_prefix = options.storage_prefix.as_deref();
        let (max_rows, candidate_cap) = self.admit_automatic_delete(&options).await?;
        let (probe, probe_started_at, mut checkpoint) =
            self.load_gc_atoms_delete_state(storage_prefix).await?;
        if checkpoint.phase == AutomaticGcAtomsDeletePhase::Complete {
            self.set_automatic_gc_atoms_generation(0);
            return Ok(Self::gc_atoms_delete_idle_report(&checkpoint));
        }

        let selection = self
            .select_gc_atoms_delete_candidates(
                storage_prefix,
                max_rows,
                candidate_cap,
                probe.generation,
                probe_started_at,
                checkpoint.cursor.as_ref(),
            )
            .await?;
        let GcAtomsDeleteSelection {
            candidates,
            rows_decided,
            atoms_skipped_recent,
            atoms_skipped_undatable,
            next_cursor,
        } = selection;

        let candidate_uuids = candidates
            .iter()
            .map(|candidate| candidate.uuid.clone())
            .collect::<Vec<_>>();
        let _retired_reclaim = self.retired_intent_reclaim_guard().await;
        let _candidate_guards = self.lock_automatic_gc_atoms(&candidate_uuids).await;
        // Reaper order: body guards, then count guards. A count change drops
        // the candidate row under them, so a row read below stays current.
        let _count_guards = self.lock_atom_ref_counts(&candidate_uuids).await;
        let locks_taken = std::time::Instant::now();
        let (validated, retained) = self
            .revalidate_gc_atoms_delete_candidates(
                storage_prefix,
                probe.generation,
                options.grace_gate.as_ref(),
                candidates,
            )
            .await?;

        let candidates_revalidated_this_call = candidate_uuids.len() as u64;
        let atoms_deleted_this_call = validated.len() as u64;
        let candidates_cleared_by_revalidation_this_call =
            candidates_revalidated_this_call.saturating_sub(atoms_deleted_this_call);
        let bytes_freed_approx_this_call = Self::gc_atoms_validated_bytes(&validated);
        let mut storage_keys_deleted_this_call = 0u64;
        if !validated.is_empty() {
            storage_keys_deleted_this_call = self
                .delete_validated_gc_atoms(storage_prefix, &probe.started_at, &validated, |entry| {
                    entry.atoms_deleted = atoms_deleted_this_call;
                    entry.atoms_scanned = rows_decided as u64;
                    entry.atoms_skipped_recent = atoms_skipped_recent;
                    entry.atoms_skipped_undatable = atoms_skipped_undatable;
                })
                .await?;
        }

        self.advance_gc_atoms_delete_checkpoint(
            &mut checkpoint,
            &GcAtomsDeletePassTotals {
                rows_decided: rows_decided as u64,
                candidates_revalidated: candidates_revalidated_this_call,
                candidates_cleared: candidates_cleared_by_revalidation_this_call,
                retained,
                atoms_deleted: atoms_deleted_this_call,
                storage_keys_deleted: storage_keys_deleted_this_call,
                bytes_freed_approx: bytes_freed_approx_this_call,
            },
            next_cursor,
        );
        self.persist_automatic_gc_atoms_delete(storage_prefix, &checkpoint)
            .await?;

        Ok(AutomaticGcAtomsDeleteReport {
            generation: checkpoint.generation,
            phase: checkpoint.phase,
            next_cursor: checkpoint.cursor.clone(),
            rows_scanned_this_call: rows_decided as u64,
            candidates_revalidated_this_call,
            candidates_cleared_by_revalidation_this_call,
            candidates_retained_this_call: retained,
            locks_held_ms_this_call: u64::try_from(locks_taken.elapsed().as_millis())
                .unwrap_or(u64::MAX),
            atoms_deleted_this_call,
            storage_keys_deleted_this_call,
            bytes_freed_approx_this_call,
            result: Self::automatic_gc_atoms_delete_result(&checkpoint),
        })
    }

    /// The scan page and batch this call may use, after the peer-apply
    /// interlock. The cadence checks the interlock first; this is the backstop.
    async fn admit_automatic_delete(
        &self,
        options: &AutomaticGcAtomsDeleteOptions,
    ) -> Result<(usize, usize), SchemaError> {
        self.require_no_peer_apply_for_atom_delete(
            options.allow_after_peer_apply,
            "automatic gc-atoms delete",
        )
        .await?;
        Ok(options.limits(Self::GC_PLANE_SCAN_PAGE))
    }

    /// Approximate bytes the validated atoms free: each body key and value.
    fn gc_atoms_validated_bytes(validated: &[(GcAtomsDeleteCandidate, Vec<String>)]) -> u64 {
        validated.iter().fold(0u64, |total, (candidate, _)| {
            total.saturating_add(candidate.key.len() as u64 + candidate.value.len() as u64)
        })
    }

    /// Load the complete probe and the matching delete checkpoint.
    async fn load_gc_atoms_delete_state(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<
        (
            AutomaticGcAtomsProbeCheckpoint,
            DateTime<Utc>,
            AutomaticGcAtomsDeleteCheckpoint,
        ),
        SchemaError,
    > {
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
        Ok((probe, probe_started_at, checkpoint))
    }

    /// Scan one page of atom rows and pick the unreferenced old candidates.
    async fn select_gc_atoms_delete_candidates(
        &self,
        storage_prefix: Option<&str>,
        max_rows: usize,
        candidate_cap: usize,
        probe_generation: u64,
        probe_started_at: DateTime<Utc>,
        cursor: Option<&crate::storage::traits::PhysicalScanCursor>,
    ) -> Result<GcAtomsDeleteSelection, SchemaError> {
        let (atom_prefix, atom_end) = Self::kind_plane_scan_bounds(storage_prefix, "atom:");
        let page = self
            .raw()
            .inner()
            .scan_range_physical_paged(
                atom_prefix.as_bytes(),
                atom_end.as_bytes(),
                cursor,
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
                .is_some_and(|marker| marker.generation == probe_generation)
            {
                continue;
            }
            match atom_row_created_at(value) {
                Some(created_at) if created_at < probe_started_at => {
                    candidates.push(GcAtomsDeleteCandidate {
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
        Ok(GcAtomsDeleteSelection {
            candidates,
            rows_decided,
            atoms_skipped_recent,
            atoms_skipped_undatable,
            next_cursor,
        })
    }
}
