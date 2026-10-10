//! v1 molecule history upgrade page and the isolated-copy completion proof.

use super::*;

impl AtomStore {
    /// Advance one bounded page of the per-molecule mutation-history upgrade.
    ///
    /// A v1 replay-complete manifest already proves `mk:` and `tv:` coverage.
    /// This keyed range adds edges for legacy `history:{molecule}:` rows. The
    /// live history writer dual-maintains new rows, so reaching the range end
    /// makes the v2 manifest authoritative without a schema or global scan.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn upgrade_atom_ref_molecule_history_page(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        page: usize,
    ) -> Result<AtomRefHistoryUpgrade, SchemaError> {
        let Some(manifest) = self
            .atom_ref_molecule_manifest(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(AtomRefHistoryUpgrade::default());
        };
        if manifest.version == ATOM_REF_MANIFEST_VERSION_HISTORY && manifest.replay_complete {
            return Ok(AtomRefHistoryUpgrade {
                complete: true,
                ..AtomRefHistoryUpgrade::default()
            });
        }
        if manifest.version != ATOM_REF_MANIFEST_VERSION_TIPS || !manifest.replay_complete {
            return Ok(AtomRefHistoryUpgrade::default());
        }

        let progress_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_history_upgrade_key(molecule_uuid),
        );
        let mut progress: AtomRefHistoryUpgrade = self
            .main_store
            .get_item(&progress_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("load atom history upgrade: {e}")))?
            .unwrap_or_default();
        if progress.complete {
            return Ok(progress);
        }

        let prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::history_molecule_prefix(molecule_uuid),
        );
        let end = FilterUtils::create_prefix_end(&prefix);
        let start = progress
            .after_history_key
            .clone()
            .unwrap_or_else(|| prefix.clone());
        let raw_limit = page.max(1) + usize::from(progress.after_history_key.is_some());
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), raw_limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "upgrade atom history edges for molecule {molecule_uuid}: {e}"
                ))
            })?;
        let range_exhausted = rows.len() < raw_limit;
        let mut writes = Vec::new();
        let mut walked = 0u64;
        let mut edges_written = 0u64;
        for (key, value) in rows {
            let event_key = String::from_utf8_lossy(&key).into_owned();
            if progress.after_history_key.as_deref() == Some(event_key.as_str()) {
                continue;
            }
            let event: MutationEvent = serde_json::from_slice(&value).map_err(|e| {
                SchemaError::InvalidData(format!("decode mutation history {event_key}: {e}"))
            })?;
            if event.molecule_uuid != molecule_uuid {
                return Err(SchemaError::InvalidData(format!(
                    "history row {event_key} belongs to molecule {}, expected {molecule_uuid}",
                    event.molecule_uuid
                )));
            }
            let edge_items = mutation_history_edge_items(&event_key, &event, storage_prefix)?;
            edges_written = edges_written.saturating_add(edge_items.len() as u64);
            writes.extend(edge_items);
            progress.after_history_key = Some(event_key);
            walked = walked.saturating_add(1);
        }
        progress.rows_walked = progress.rows_walked.saturating_add(walked);
        progress.edges_written = progress.edges_written.saturating_add(edges_written);
        if range_exhausted {
            progress.complete = true;
            push_manifest_item_with_version(
                &mut writes,
                molecule_uuid,
                manifest.mutation_watermark_nanos,
                true,
                ATOM_REF_MANIFEST_VERSION_HISTORY,
                storage_prefix,
            )?;
        }
        writes.push((
            progress_key,
            serde_json::to_value(&progress).map_err(|e| {
                SchemaError::InvalidData(format!("serialize atom history upgrade: {e}"))
            })?,
        ));
        self.main_store.batch_put_items(writes).await.map_err(|e| {
            SchemaError::InvalidData(format!("write atom history upgrade page: {e}"))
        })?;
        Ok(progress)
    }

    /// Verify one small fixture molecule, then mark its replay manifest complete.
    ///
    /// Terminal proofs use this only on a throwaway copy after they prove the
    /// schema did not exist before declaration and disable external writers.
    /// The global reindex is intentionally unsuitable there: a proof schema
    /// has a few exact slots, while the copied home can contain millions of
    /// unrelated slots. The caller supplies a strict slot bound so this helper
    /// cannot become an unbounded storage-plane walk.
    #[cfg(feature = "cloud-sync")]
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn verify_and_complete_atom_ref_molecule_for_isolated_copy_proof(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        max_slots: usize,
    ) -> Result<u64, SchemaError> {
        let max_slots = max_slots.max(1);
        let prefix = build_storage_key(storage_prefix, &format!("mk:{molecule_uuid}:"));
        let end = FilterUtils::create_prefix_end(&prefix);
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(prefix.as_bytes(), end.as_bytes(), max_slots + 1)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "verify proof atom refs for molecule {molecule_uuid}: {e}"
                ))
            })?;
        if rows.is_empty() {
            return Err(SchemaError::InvalidData(format!(
                "proof molecule {molecule_uuid} has no durable slots"
            )));
        }
        if rows.len() > max_slots {
            return Err(SchemaError::InvalidData(format!(
                "proof molecule {molecule_uuid} exceeds the {max_slots}-slot verification bound"
            )));
        }

        let mut expected = Vec::new();
        for (key, value) in &rows {
            let full_key = String::from_utf8_lossy(key).into_owned();
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                return Err(SchemaError::InvalidData(format!(
                    "proof molecule row has the wrong storage prefix: {full_key}"
                )));
            };
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode proof molecule row {full_key}"
                )));
            };
            let record: PerKeyRecord = serde_json::from_slice(value).map_err(|e| {
                SchemaError::InvalidData(format!("decode proof molecule row {full_key}: {e}"))
            })?;
            expected.extend(
                self.reference_edges_for_record(
                    molecule_uuid,
                    &disk_hash,
                    &disk_range,
                    &record,
                    storage_prefix,
                )
                .await?
                .into_iter()
                .map(|edge| edge.storage_key(storage_prefix)),
            );
        }

        let history_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::history_molecule_prefix(molecule_uuid),
        );
        let history_end = FilterUtils::create_prefix_end(&history_prefix);
        let history_rows = self
            .main_store
            .inner()
            .scan_range_paged(
                history_prefix.as_bytes(),
                history_end.as_bytes(),
                max_slots + 1,
            )
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "verify proof history refs for molecule {molecule_uuid}: {e}"
                ))
            })?;
        if history_rows.len() > max_slots {
            return Err(SchemaError::InvalidData(format!(
                "proof molecule {molecule_uuid} exceeds the {max_slots}-history-row verification bound"
            )));
        }
        for (key, value) in history_rows {
            let event_key = String::from_utf8_lossy(&key).into_owned();
            let event: MutationEvent = serde_json::from_slice(&value).map_err(|e| {
                SchemaError::InvalidData(format!("decode proof history {event_key}: {e}"))
            })?;
            expected.extend(
                mutation_history_edges(&event_key, &event)
                    .into_iter()
                    .map(|edge| edge.storage_key(storage_prefix)),
            );
        }

        for chunk in expected.chunks(64) {
            let hits = self.main_store.exists_items(chunk).await.map_err(|e| {
                SchemaError::InvalidData(format!(
                    "verify proof reverse edges for molecule {molecule_uuid}: {e}"
                ))
            })?;
            if hits.iter().any(|exists| !exists) {
                return Err(SchemaError::InvalidData(format!(
                    "proof molecule {molecule_uuid} has a missing reverse edge"
                )));
            }
        }
        let mut writes = Vec::with_capacity(1);
        push_manifest_item_with_version(
            &mut writes,
            molecule_uuid,
            unix_nanos(),
            true,
            ATOM_REF_MANIFEST_VERSION_HISTORY,
            storage_prefix,
        )?;
        self.main_store.batch_put_items(writes).await.map_err(|e| {
            SchemaError::InvalidData(format!(
                "complete proof atom-ref manifest for molecule {molecule_uuid}: {e}"
            ))
        })?;
        Ok(rows.len() as u64)
    }
}
