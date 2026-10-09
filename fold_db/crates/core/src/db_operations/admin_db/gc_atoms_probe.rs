// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Automatic `gc-atoms` orphan-atom probe (one long scan function; helpers are in `gc_atoms_probe_support`).

use super::*;

impl AtomStore {
    /// Advance one physically bounded page of the automatic orphan-byte probe.
    ///
    /// The manual `gc-atoms --dry-run` contract stays unchanged and read-only.
    /// This separate state machine writes only internal probe checkpoints and
    /// keyed membership markers. It never rewrites tips or deletes `tv:`, atom,
    /// or locator rows.
    ///
    /// A result is returned only when all reference planes and the atom plane
    /// finish. A completed checkpoint stays terminal until the caller sets
    /// `restart_completed`, which lets the future daemon cadence open a new lap
    /// without making every status read restart the probe.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn probe_orphan_atoms_from_checkpoint(
        &self,
        options: AutomaticGcAtomsProbeOptions,
    ) -> Result<AutomaticGcAtomsProbeReport, SchemaError> {
        let storage_prefix = options.storage_prefix.as_deref();
        let max_rows = options
            .max_rows
            .unwrap_or(Self::GC_PLANE_SCAN_PAGE)
            .clamp(1, Self::GC_PLANE_SCAN_PAGE);
        let mut checkpoint = self
            .automatic_gc_atoms_probe_checkpoint(storage_prefix)
            .await?;

        if checkpoint.version != 2
            || (checkpoint.phase == AutomaticGcAtomsProbePhase::Complete
                && options.restart_completed)
        {
            checkpoint = AutomaticGcAtomsProbeCheckpoint {
                version: 2,
                generation: checkpoint.generation.saturating_add(1).max(1),
                phase: AutomaticGcAtomsProbePhase::ClearReferenceMarkers,
                started_at: Utc::now().to_rfc3339(),
                ..Default::default()
            };
            self.set_automatic_gc_atoms_generation(0);
        }

        if checkpoint.phase == AutomaticGcAtomsProbePhase::Complete {
            self.set_automatic_gc_atoms_generation(checkpoint.generation);
            return Ok(Self::automatic_gc_atoms_probe_report(&checkpoint, 0));
        }

        if matches!(
            checkpoint.phase,
            AutomaticGcAtomsProbePhase::ClearReferenceMarkers
                | AutomaticGcAtomsProbePhase::ClearTipVersionSkipMarkers
        ) {
            let marker_prefix = match checkpoint.phase {
                AutomaticGcAtomsProbePhase::ClearReferenceMarkers => {
                    GC_ATOMS_PROBE_REFERENCE_PREFIX
                }
                AutomaticGcAtomsProbePhase::ClearTipVersionSkipMarkers => {
                    GC_ATOMS_PROBE_TV_SKIP_PREFIX
                }
                _ => unreachable!(),
            };
            let prefix = build_storage_key(storage_prefix, marker_prefix);
            let end = FilterUtils::create_prefix_end(&prefix);
            let page = self
                .raw()
                .inner()
                .scan_range_physical_paged(
                    prefix.as_bytes(),
                    end.as_bytes(),
                    checkpoint.cursor.as_ref(),
                    max_rows,
                    1,
                )
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("clear automatic gc-atoms markers: {e}"))
                })?;
            let rows_scanned = page.rows.len() as u64;
            let keys = page
                .rows
                .into_iter()
                .map(|(key, _)| String::from_utf8_lossy(&key).into_owned())
                .collect::<Vec<_>>();
            if !keys.is_empty() {
                self.raw().batch_delete_keys(keys).await.map_err(|e| {
                    SchemaError::InvalidData(format!("delete automatic gc-atoms markers: {e}"))
                })?;
            }
            checkpoint.cursor = page.next_cursor;
            if checkpoint.cursor.is_none() {
                checkpoint.phase = checkpoint.phase.next();
                if checkpoint.phase == AutomaticGcAtomsProbePhase::Atoms {
                    self.set_automatic_gc_atoms_generation(checkpoint.generation);
                }
            }
            checkpoint.pages_completed = checkpoint.pages_completed.saturating_add(1);
            self.persist_automatic_gc_atoms_probe(storage_prefix, &checkpoint, Vec::new())
                .await?;
            return Ok(Self::automatic_gc_atoms_probe_report(
                &checkpoint,
                rows_scanned,
            ));
        }

        if checkpoint.phase == AutomaticGcAtomsProbePhase::Prologue {
            let prune = self
                .prune_tip_version_chains(
                    true,
                    storage_prefix,
                    false,
                    checkpoint.cursor.as_ref(),
                    max_rows,
                )
                .await?;
            let marker = serde_json::to_value(AutomaticGcAtomsProbeMarker {
                generation: checkpoint.generation,
            })
            .map_err(|e| {
                SchemaError::InvalidData(format!("encode automatic gc-atoms marker: {e}"))
            })?;
            let writes = prune
                .tv_keys_skip
                .iter()
                .map(|key| {
                    (
                        Self::automatic_gc_atoms_tv_skip_marker_key(storage_prefix, key),
                        marker.clone(),
                    )
                })
                .collect::<Vec<_>>();
            checkpoint.prologue_tips_scanned = checkpoint
                .prologue_tips_scanned
                .saturating_add(prune.keys_scanned);
            checkpoint.prologue_tip_versions_planned = checkpoint
                .prologue_tip_versions_planned
                .saturating_add(prune.tip_versions_pruned);
            checkpoint.cursor = prune.next_physical_cursor;
            if !prune.more_remaining {
                checkpoint.cursor = None;
                checkpoint.phase = checkpoint.phase.next();
            }
            checkpoint.pages_completed = checkpoint.pages_completed.saturating_add(1);
            self.persist_automatic_gc_atoms_probe(storage_prefix, &checkpoint, writes)
                .await?;
            return Ok(Self::automatic_gc_atoms_probe_report(
                &checkpoint,
                prune.keys_scanned,
            ));
        }

        if checkpoint.phase == AutomaticGcAtomsProbePhase::PinLogReferences {
            // Direct AtomStore users have no sync plane. The serving automatic
            // path always supplies a page when an engine exists and fails the
            // step before this call if that strict read fails.
            let page =
                options
                    .pin_log_reference_page
                    .unwrap_or(AutomaticGcAtomsPinLogReferencePage {
                        scan_complete: true,
                        ..Default::default()
                    });
            let marker = serde_json::to_value(AutomaticGcAtomsProbeMarker {
                generation: checkpoint.generation,
            })
            .map_err(|e| {
                SchemaError::InvalidData(format!("encode automatic gc-atoms marker: {e}"))
            })?;
            let writes = page
                .atom_uuids
                .iter()
                .map(|uuid| {
                    (
                        Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid),
                        marker.clone(),
                    )
                })
                .collect::<Vec<_>>();
            checkpoint.reference_rows_scanned = checkpoint
                .reference_rows_scanned
                .saturating_add(page.rows_scanned);
            checkpoint.reference_uuids_marked = checkpoint
                .reference_uuids_marked
                .saturating_add(page.atom_uuids.len() as u64);
            checkpoint.pin_log_after_key = page.next_after_key;
            if page.scan_complete {
                checkpoint.pin_log_after_key = None;
                checkpoint.phase = checkpoint.phase.next();
            }
            checkpoint.pages_completed = checkpoint.pages_completed.saturating_add(1);
            self.persist_automatic_gc_atoms_probe(storage_prefix, &checkpoint, writes)
                .await?;
            return Ok(Self::automatic_gc_atoms_probe_report(
                &checkpoint,
                page.rows_scanned,
            ));
        }

        if matches!(
            checkpoint.phase,
            AutomaticGcAtomsProbePhase::Tips
                | AutomaticGcAtomsProbePhase::TipVersions
                | AutomaticGcAtomsProbePhase::History
                | AutomaticGcAtomsProbePhase::Conflicts
                | AutomaticGcAtomsProbePhase::LegacyRefs
        ) {
            let plane_prefix = match checkpoint.phase {
                AutomaticGcAtomsProbePhase::Tips => "mk:",
                AutomaticGcAtomsProbePhase::TipVersions => "tv:",
                AutomaticGcAtomsProbePhase::History => "history:",
                AutomaticGcAtomsProbePhase::Conflicts => "conflict:",
                AutomaticGcAtomsProbePhase::LegacyRefs => "ref:",
                _ => unreachable!(),
            };
            let (prefix, end) = Self::kind_plane_scan_bounds(storage_prefix, plane_prefix);
            let page = self
                .raw()
                .inner()
                .scan_range_physical_paged(
                    prefix.as_bytes(),
                    end.as_bytes(),
                    checkpoint.cursor.as_ref(),
                    max_rows,
                    1,
                )
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("automatic gc-atoms scan {plane_prefix}: {e}"))
                })?;
            let rows_scanned = page.rows.len() as u64;
            let tv_skip_markers = if checkpoint.phase == AutomaticGcAtomsProbePhase::TipVersions {
                let keys = page
                    .rows
                    .iter()
                    .map(|(key, _)| {
                        Self::automatic_gc_atoms_tv_skip_marker_key(
                            storage_prefix,
                            String::from_utf8_lossy(key).as_ref(),
                        )
                    })
                    .collect::<Vec<_>>();
                self.raw()
                    .get_items::<AutomaticGcAtomsProbeMarker>(&keys)
                    .await
                    .map_err(|e| {
                        SchemaError::InvalidData(format!(
                            "read automatic gc-atoms tv skip markers: {e}"
                        ))
                    })?
            } else {
                vec![None; page.rows.len()]
            };

            let mut referenced = HashSet::new();
            for ((_, value), skip_marker) in page.rows.iter().zip(tv_skip_markers.iter()) {
                if skip_marker
                    .as_ref()
                    .is_some_and(|marker| marker.generation == checkpoint.generation)
                {
                    continue;
                }
                match checkpoint.phase {
                    AutomaticGcAtomsProbePhase::Tips => {
                        if let Ok(value) = serde_json::from_slice::<Value>(value) {
                            if let Some(uuid) = value
                                .pointer("/entry/atom_uuid")
                                .and_then(Value::as_str)
                                .filter(|uuid| !uuid.is_empty())
                            {
                                referenced.insert(uuid.to_string());
                            }
                        }
                    }
                    AutomaticGcAtomsProbePhase::TipVersions => {
                        if let Ok(entry) = serde_json::from_slice::<crate::atom::AtomEntry>(value) {
                            referenced.insert(entry.atom_uuid);
                        }
                    }
                    AutomaticGcAtomsProbePhase::History => {
                        if let Ok(event) = serde_json::from_slice::<MutationEvent>(value) {
                            referenced.insert(event.new_atom_uuid);
                            if let Some(loser) = event.conflict_loser_atom {
                                referenced.insert(loser);
                            }
                        }
                    }
                    AutomaticGcAtomsProbePhase::Conflicts
                    | AutomaticGcAtomsProbePhase::LegacyRefs => {
                        collect_atom_uuid_strings(value, &mut referenced);
                    }
                    _ => unreachable!(),
                }
            }

            let marker = serde_json::to_value(AutomaticGcAtomsProbeMarker {
                generation: checkpoint.generation,
            })
            .map_err(|e| {
                SchemaError::InvalidData(format!("encode automatic gc-atoms marker: {e}"))
            })?;
            let writes = referenced
                .iter()
                .map(|uuid| {
                    (
                        Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid),
                        marker.clone(),
                    )
                })
                .collect::<Vec<_>>();
            checkpoint.reference_rows_scanned = checkpoint
                .reference_rows_scanned
                .saturating_add(rows_scanned);
            checkpoint.reference_uuids_marked = checkpoint
                .reference_uuids_marked
                .saturating_add(referenced.len() as u64);
            checkpoint.cursor = page.next_cursor;
            if checkpoint.cursor.is_none() {
                checkpoint.phase = checkpoint.phase.next();
            }
            checkpoint.pages_completed = checkpoint.pages_completed.saturating_add(1);
            self.persist_automatic_gc_atoms_probe(storage_prefix, &checkpoint, writes)
                .await?;
            return Ok(Self::automatic_gc_atoms_probe_report(
                &checkpoint,
                rows_scanned,
            ));
        }

        debug_assert_eq!(checkpoint.phase, AutomaticGcAtomsProbePhase::Atoms);
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
            .map_err(|e| SchemaError::InvalidData(format!("automatic gc-atoms scan atom: {e}")))?;
        let rows_scanned = page.rows.len() as u64;
        let marker_keys = page
            .rows
            .iter()
            .map(|(key, _)| {
                let key = String::from_utf8_lossy(key);
                let uuid = Self::atom_uuid_from_body_key(&key).unwrap_or_default();
                Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid)
            })
            .collect::<Vec<_>>();
        let reference_markers = self
            .raw()
            .get_items::<AutomaticGcAtomsProbeMarker>(&marker_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("read automatic gc-atoms reference markers: {e}"))
            })?;
        let started_at = DateTime::parse_from_rfc3339(&checkpoint.started_at)
            .map_err(|e| {
                SchemaError::InvalidData(format!("invalid automatic gc-atoms start time: {e}"))
            })?
            .with_timezone(&Utc);
        for ((key, value), marker) in page.rows.iter().zip(reference_markers.iter()) {
            checkpoint.atoms_scanned = checkpoint.atoms_scanned.saturating_add(1);
            let key_text = String::from_utf8_lossy(key);
            let Some(_) = Self::atom_uuid_from_body_key(&key_text) else {
                continue;
            };
            if marker
                .as_ref()
                .is_some_and(|marker| marker.generation == checkpoint.generation)
            {
                checkpoint.atoms_referenced = checkpoint.atoms_referenced.saturating_add(1);
                continue;
            }
            match atom_row_created_at(value) {
                Some(created_at) if created_at < started_at => {
                    checkpoint.unreferenced_atoms = checkpoint.unreferenced_atoms.saturating_add(1);
                    checkpoint.unreferenced_bytes_approx = checkpoint
                        .unreferenced_bytes_approx
                        .saturating_add(key.len() as u64 + value.len() as u64);
                }
                Some(_) => {
                    checkpoint.atoms_skipped_recent =
                        checkpoint.atoms_skipped_recent.saturating_add(1);
                }
                None => {
                    checkpoint.atoms_skipped_undatable =
                        checkpoint.atoms_skipped_undatable.saturating_add(1);
                }
            }
        }
        checkpoint.cursor = page.next_cursor;
        if checkpoint.cursor.is_none() {
            checkpoint.phase = AutomaticGcAtomsProbePhase::Complete;
            checkpoint.completed_at = Some(Utc::now().to_rfc3339());
            self.set_automatic_gc_atoms_generation(checkpoint.generation);
        }
        checkpoint.pages_completed = checkpoint.pages_completed.saturating_add(1);
        self.persist_automatic_gc_atoms_probe(storage_prefix, &checkpoint, Vec::new())
            .await?;
        Ok(Self::automatic_gc_atoms_probe_report(
            &checkpoint,
            rows_scanned,
        ))
    }
}
