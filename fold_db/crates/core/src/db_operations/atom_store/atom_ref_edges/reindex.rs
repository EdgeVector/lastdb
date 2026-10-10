//! v1 edge reindex entry points and manifest/backfill-status reads; the upgrade and v2 families live in the sibling `reindex_*` modules.

use super::*;

impl AtomStore {
    /// Point-read one molecule's cutover manifest.
    pub async fn atom_ref_molecule_manifest(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomRefMoleculeManifest>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_molecule_manifest_key(molecule_uuid),
        );
        self.main_store
            .get_item(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("load atom reverse-edge manifest: {e}")))
    }

    /// Seed a replay-complete v1 manifest after the prefix-wide rebuild ends.
    ///
    /// The prefix rebuild creates manifests for molecules that own `mk:` rows.
    /// A declared but empty molecule has no row to discover, so it needs this
    /// exact catalog-keyed completion step. New writes already dual-maintain
    /// reverse edges, which makes the prefix watermark safe for later writes.
    pub async fn ensure_atom_ref_molecule_manifest_after_reindex(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomRefMoleculeManifest>, SchemaError> {
        if let Some(manifest) = self
            .atom_ref_molecule_manifest(molecule_uuid, storage_prefix)
            .await?
        {
            return Ok(Some(manifest));
        }
        let status = self.atom_ref_backfill_status(storage_prefix).await?;
        if !status.completed || status.phase != AtomRefBackfillPhase::Complete {
            return Ok(None);
        }
        let manifest = AtomRefMoleculeManifest {
            version: ATOM_REF_MANIFEST_VERSION_TIPS,
            molecule_uuid: molecule_uuid.to_string(),
            mutation_watermark_nanos: status.mutation_watermark_nanos,
            replay_complete: true,
        };
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_molecule_manifest_key(molecule_uuid),
        );
        self.main_store
            .batch_put_items(vec![(
                key,
                serde_json::to_value(&manifest).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "serialize atom reverse-edge manifest: {error}"
                    ))
                })?,
            )])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "seed atom reverse-edge manifest after reindex: {error}"
                ))
            })?;
        Ok(Some(manifest))
    }

    /// Read durable rebuild state without a data-plane scan.
    pub async fn atom_ref_backfill_status(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<AtomRefBackfillStatus, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            molecule_key_codec::ATOM_REF_REINDEX_CHECKPOINT_KEY,
        );
        self.main_store
            .get_item(&key)
            .await
            .map(Option::unwrap_or_default)
            .map_err(|e| SchemaError::InvalidData(format!("load atom edge rebuild status: {e}")))
    }

    /// Reset the rebuild markers for an isolated-copy proof.
    ///
    /// The caller must point the database at a throwaway copy. This method is
    /// available only in test or cloud-sync builds because production code
    /// must preserve the monotonic completeness marker.
    #[cfg(feature = "cloud-sync")]
    pub async fn reset_atom_ref_reindex_for_isolated_copy_proof(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.main_store
            .batch_delete_keys(vec![
                build_storage_key(storage_prefix, molecule_key_codec::ATOM_REF_COMPLETE_KEY),
                build_storage_key(
                    storage_prefix,
                    molecule_key_codec::ATOM_REF_REINDEX_CHECKPOINT_KEY,
                ),
            ])
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "reset atom reverse-edge rebuild markers on isolated copy: {e}"
                ))
            })
    }

    /// Run one bounded page of the internal two-pass rebuild.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn reindex_atom_ref_edges(
        &self,
        storage_prefix: Option<&str>,
        slot_page: Option<usize>,
    ) -> Result<AtomRefBackfillReport, SchemaError> {
        let page = slot_page.unwrap_or(DEFAULT_REINDEX_SLOT_PAGE).max(1);
        let mut status = self.atom_ref_backfill_status(storage_prefix).await?;
        if matches!(
            status.phase,
            AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked
        ) {
            return Ok(AtomRefBackfillReport {
                slots_walked: 0,
                edges_written: 0,
                completed: status.completed,
                status,
            });
        }

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let start = status
            .after_tip_key
            .clone()
            .unwrap_or_else(|| mk_prefix.clone());
        let raw_limit = page + usize::from(status.after_tip_key.is_some());
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), mk_end.as_bytes(), raw_limit)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("reindex scan mk: {e}")))?;
        let range_exhausted = rows.len() < raw_limit;

        let phase = status.phase;
        let mut writes: Vec<(String, Value)> = Vec::new();
        let mut walked = 0u64;
        let mut last_key = status.after_tip_key.clone();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            if status.after_tip_key.as_deref() == Some(full_key.as_str()) {
                continue;
            }
            last_key = Some(full_key.clone());
            walked += 1;
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                status.skipped_rows += 1;
                continue;
            };
            let Some(rest) = base_key.strip_prefix("mk:") else {
                status.skipped_rows += 1;
                continue;
            };
            let Some((molecule_uuid, _)) = rest.split_once(':') else {
                status.skipped_rows += 1;
                continue;
            };
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                status.skipped_rows += 1;
                continue;
            };
            let Ok(record): Result<PerKeyRecord, _> = serde_json::from_slice(&value) else {
                status.skipped_rows += 1;
                continue;
            };

            if phase == AtomRefBackfillPhase::Replay
                && status.current_molecule.as_deref() != Some(molecule_uuid)
            {
                if let Some(previous) = status.current_molecule.replace(molecule_uuid.to_string()) {
                    push_manifest_item(
                        &mut writes,
                        &previous,
                        status.mutation_watermark_nanos,
                        true,
                        storage_prefix,
                    )?;
                    status.molecules_complete += 1;
                }
            } else if phase == AtomRefBackfillPhase::Backfill {
                push_manifest_item(
                    &mut writes,
                    molecule_uuid,
                    status.mutation_watermark_nanos,
                    false,
                    storage_prefix,
                )?;
            }

            for edge in self
                .reference_edges_for_record(
                    molecule_uuid,
                    &disk_hash,
                    &disk_range,
                    &record,
                    storage_prefix,
                )
                .await?
            {
                push_edge_item(&mut writes, edge, storage_prefix)?;
            }
        }

        status.after_tip_key = last_key;
        status.slots_walked += walked;
        let edge_writes = writes
            .iter()
            .filter(|(key, _)| key.contains(molecule_key_codec::ATOM_REF_EDGE_PREFIX))
            .count() as u64;
        status.edges_written += edge_writes;

        if range_exhausted {
            match phase {
                AtomRefBackfillPhase::Backfill => {
                    status.phase = AtomRefBackfillPhase::Replay;
                    status.after_tip_key = None;
                    status.current_molecule = None;
                }
                AtomRefBackfillPhase::Replay => {
                    if let Some(last_molecule) = status.current_molecule.take() {
                        push_manifest_item(
                            &mut writes,
                            &last_molecule,
                            status.mutation_watermark_nanos,
                            true,
                            storage_prefix,
                        )?;
                        status.molecules_complete += 1;
                    }
                    if status.skipped_rows == 0 {
                        status.phase = AtomRefBackfillPhase::Complete;
                        status.completed = true;
                        writes.push((
                            build_storage_key(
                                storage_prefix,
                                molecule_key_codec::ATOM_REF_COMPLETE_KEY,
                            ),
                            Value::Bool(true),
                        ));
                    } else {
                        status.phase = AtomRefBackfillPhase::Blocked;
                    }
                }
                AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked => {}
            }
        }

        writes.push((
            build_storage_key(
                storage_prefix,
                molecule_key_codec::ATOM_REF_REINDEX_CHECKPOINT_KEY,
            ),
            serde_json::to_value(&status).map_err(|e| {
                SchemaError::InvalidData(format!("serialize atom edge rebuild status: {e}"))
            })?,
        ));
        self.main_store.batch_put_items(writes).await.map_err(|e| {
            SchemaError::InvalidData(format!("write atom reverse-edge rebuild page: {e}"))
        })?;

        Ok(AtomRefBackfillReport {
            slots_walked: walked,
            edges_written: edge_writes,
            completed: status.completed,
            status,
        })
    }
}
