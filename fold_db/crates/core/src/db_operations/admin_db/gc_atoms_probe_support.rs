//! Automatic `gc-atoms` probe support: checkpoints, scan bounds, reference markers and result persistence.

use super::*;

impl AtomStore {
    /// Load the durable `gc-atoms` prologue checkpoint (empty if absent).
    pub async fn gc_atoms_prune_checkpoint(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<GcAtomsPruneCheckpoint, SchemaError> {
        let key = build_storage_key(storage_prefix, GC_ATOMS_PRUNE_CHECKPOINT_KEY);
        let raw: Option<GcAtomsPruneCheckpoint> = self.raw().get_item(&key).await.map_err(|e| {
            SchemaError::InvalidData(format!("load gc-atoms prune checkpoint: {e}"))
        })?;
        Ok(raw.unwrap_or_default())
    }

    /// Persist the `gc-atoms` prologue checkpoint.
    pub async fn put_gc_atoms_prune_checkpoint(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &GcAtomsPruneCheckpoint,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, GC_ATOMS_PRUNE_CHECKPOINT_KEY);
        self.raw().put_item(&key, checkpoint).await.map_err(|e| {
            SchemaError::InvalidData(format!("store gc-atoms prune checkpoint: {e}"))
        })?;
        Ok(())
    }

    /// Load the constant-size checkpoint for the automatic orphan-byte probe.
    pub async fn automatic_gc_atoms_probe_checkpoint(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<AutomaticGcAtomsProbeCheckpoint, SchemaError> {
        let key = build_storage_key(storage_prefix, GC_ATOMS_PROBE_CHECKPOINT_KEY);
        let raw =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("load automatic gc-atoms probe: {e}"))
            })?;
        Ok(raw.unwrap_or_default())
    }

    /// Inclusive start and exclusive end covering `kind\0` and `kind:` twins.
    ///
    /// `mk:` and `ref:` are not kind-as-partition kinds, so the range stays the
    /// colon prefix. Listed kinds start at `kind\0` and end after `kind:`.
    pub(crate) fn kind_plane_scan_bounds(
        storage_prefix: Option<&str>,
        colon_prefix: &str,
    ) -> (String, String) {
        let colon = build_storage_key(storage_prefix, colon_prefix);
        crate::kind_partition::colon_plane_bounds(&colon)
    }

    pub(crate) fn atom_uuid_from_body_key(key: &str) -> Option<&str> {
        let rest = crate::kind_partition::rest_of(key, "atom")?;
        let uuid = crate::atom::atom_key_codec::uuid_of_suffix(rest);
        (!uuid.is_empty()).then_some(uuid)
    }

    pub(super) fn automatic_gc_atoms_probe_result(
        checkpoint: &AutomaticGcAtomsProbeCheckpoint,
    ) -> Option<AutomaticGcAtomsProbeResult> {
        if checkpoint.phase != AutomaticGcAtomsProbePhase::Complete {
            return None;
        }
        Some(AutomaticGcAtomsProbeResult {
            generation: checkpoint.generation,
            started_at: checkpoint.started_at.clone(),
            completed_at: checkpoint.completed_at.clone().unwrap_or_default(),
            atoms_scanned: checkpoint.atoms_scanned,
            atoms_referenced: checkpoint.atoms_referenced,
            unreferenced_atoms: checkpoint.unreferenced_atoms,
            unreferenced_bytes_approx: checkpoint.unreferenced_bytes_approx,
            atoms_skipped_recent: checkpoint.atoms_skipped_recent,
            atoms_skipped_undatable: checkpoint.atoms_skipped_undatable,
        })
    }

    pub(super) fn automatic_gc_atoms_probe_report(
        checkpoint: &AutomaticGcAtomsProbeCheckpoint,
        rows_scanned_this_call: u64,
    ) -> AutomaticGcAtomsProbeReport {
        AutomaticGcAtomsProbeReport {
            generation: checkpoint.generation,
            phase: checkpoint.phase,
            next_cursor: checkpoint.cursor.clone(),
            rows_scanned_this_call,
            result: Self::automatic_gc_atoms_probe_result(checkpoint),
        }
    }

    pub(crate) fn automatic_gc_atoms_reference_marker_key(
        storage_prefix: Option<&str>,
        atom_uuid: &str,
    ) -> String {
        build_storage_key(
            storage_prefix,
            &format!("{GC_ATOMS_PROBE_REFERENCE_PREFIX}{atom_uuid}"),
        )
    }

    /// Return the candidate UUIDs reached by one completed automatic probe.
    ///
    /// The probe clears old markers before it starts. The generation check
    /// still makes this safe if a stale row survives a failed clear pass.
    #[cfg_attr(not(feature = "cloud-sync"), allow(dead_code))]
    pub(crate) async fn automatic_gc_atoms_referenced_candidates(
        &self,
        storage_prefix: Option<&str>,
        generation: u64,
        atom_uuids: &[String],
    ) -> Result<HashSet<String>, SchemaError> {
        let keys = atom_uuids
            .iter()
            .map(|uuid| Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid))
            .collect::<Vec<_>>();
        let markers = self
            .raw()
            .get_items::<AutomaticGcAtomsProbeMarker>(&keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read automatic gc-atoms candidate audit markers: {error}"
                ))
            })?;
        Ok(atom_uuids
            .iter()
            .zip(markers)
            .filter_map(|(uuid, marker)| {
                marker
                    .is_some_and(|marker| marker.generation == generation)
                    .then(|| uuid.clone())
            })
            .collect())
    }

    /// Atom UUIDs referenced by `mk:` rows in a pending molecule write batch.
    pub(crate) fn automatic_gc_tip_reference_uuids(
        &self,
        items: &[(String, Value)],
        storage_prefix: Option<&str>,
    ) -> Vec<String> {
        if self.automatic_gc_atoms_generation() == 0 {
            return Vec::new();
        }
        let logical_prefix = storage_prefix.map_or_else(String::new, |prefix| format!("{prefix}:"));
        let mut uuids = items
            .iter()
            .filter_map(|(key, value)| {
                let logical = key.strip_prefix(&logical_prefix).unwrap_or(key);
                logical
                    .starts_with("mk:")
                    .then(|| {
                        value
                            .pointer("/entry/atom_uuid")
                            .and_then(Value::as_str)
                            .filter(|uuid| !uuid.is_empty())
                            .map(str::to_string)
                    })
                    .flatten()
            })
            .collect::<Vec<_>>();
        uuids.sort_unstable();
        uuids.dedup();
        uuids
    }

    /// Generation markers that land in the same durable batch as live tips.
    pub(crate) fn automatic_gc_reference_marker_items(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let generation = self.automatic_gc_atoms_generation();
        if generation == 0 {
            return Ok(Vec::new());
        }
        let marker =
            serde_json::to_value(AutomaticGcAtomsProbeMarker { generation }).map_err(|e| {
                SchemaError::InvalidData(format!("encode automatic gc-atoms reference marker: {e}"))
            })?;
        Ok(atom_uuids
            .iter()
            .map(|uuid| {
                (
                    Self::automatic_gc_atoms_reference_marker_key(storage_prefix, uuid),
                    marker.clone(),
                )
            })
            .collect())
    }

    /// Restore the write-side guard after a daemon restart.
    pub(crate) async fn hydrate_automatic_gc_atoms_generation(
        &self,
    ) -> Result<(), crate::storage::StorageError> {
        let checkpoint = self
            .raw()
            .get_item::<AutomaticGcAtomsProbeCheckpoint>(GC_ATOMS_PROBE_CHECKPOINT_KEY)
            .await?;
        let generation = checkpoint
            .filter(|checkpoint| {
                checkpoint.version == 2
                    && !matches!(
                        checkpoint.phase,
                        AutomaticGcAtomsProbePhase::ClearReferenceMarkers
                    )
            })
            .map_or(0, |checkpoint| checkpoint.generation);
        self.set_automatic_gc_atoms_generation(generation);
        Ok(())
    }

    pub(super) fn automatic_gc_atoms_tv_skip_marker_key(
        storage_prefix: Option<&str>,
        tv_key: &str,
    ) -> String {
        build_storage_key(
            storage_prefix,
            &format!("{GC_ATOMS_PROBE_TV_SKIP_PREFIX}{tv_key}"),
        )
    }

    pub(super) async fn persist_automatic_gc_atoms_probe(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &AutomaticGcAtomsProbeCheckpoint,
        mut writes: Vec<(String, Value)>,
    ) -> Result<(), SchemaError> {
        let checkpoint_key = build_storage_key(storage_prefix, GC_ATOMS_PROBE_CHECKPOINT_KEY);
        let checkpoint_value = serde_json::to_value(checkpoint).map_err(|e| {
            SchemaError::InvalidData(format!("encode automatic gc-atoms probe: {e}"))
        })?;
        writes.push((checkpoint_key, checkpoint_value));
        self.raw().batch_put_items(writes).await.map_err(|e| {
            SchemaError::InvalidData(format!("store automatic gc-atoms probe: {e}"))
        })?;
        Ok(())
    }
}
