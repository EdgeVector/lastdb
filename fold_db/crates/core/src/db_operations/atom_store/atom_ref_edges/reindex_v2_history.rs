//! v2 molecule history upgrade page and the isolated-copy reset.

use super::*;

impl AtomStore {
    /// Advance one bounded mutation-history page for a compact molecule.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn upgrade_atom_ref_v2_molecule_history_page(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        page: usize,
    ) -> Result<AtomRefHistoryUpgrade, SchemaError> {
        let Some(manifest) = self
            .atom_ref_v2_molecule_manifest(molecule_uuid, storage_prefix)
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
            &molecule_key_codec::atom_ref_v2_history_upgrade_key(molecule_uuid),
        );
        let mut progress: AtomRefHistoryUpgrade = self
            .main_store
            .get_item(&progress_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("load compact atom history upgrade: {error}"))
            })?
            .unwrap_or_default();

        if !progress.complete {
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
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "upgrade compact atom history edges for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            let range_exhausted = rows.len() < raw_limit;
            let mut mutations = Vec::new();
            let mut walked = 0u64;
            let mut edges_written = 0u64;
            for (key, value) in rows {
                let event_key = String::from_utf8_lossy(&key).into_owned();
                if progress.after_history_key.as_deref() == Some(event_key.as_str()) {
                    continue;
                }
                let event: MutationEvent = serde_json::from_slice(&value).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "decode compact mutation history {event_key}: {error}"
                    ))
                })?;
                if event.molecule_uuid != molecule_uuid {
                    return Err(SchemaError::InvalidData(format!(
                        "history row {event_key} belongs to molecule {}, expected {molecule_uuid}",
                        event.molecule_uuid
                    )));
                }
                for edge in mutation_history_edges(&event_key, &event) {
                    mutations.push(compact_edge_put_mutation(&edge, storage_prefix)?);
                    edges_written = edges_written.saturating_add(1);
                }
                progress.after_history_key = Some(event_key);
                walked = walked.saturating_add(1);
            }
            progress.rows_walked = progress.rows_walked.saturating_add(walked);
            progress.edges_written = progress.edges_written.saturating_add(edges_written);
            if range_exhausted {
                progress.complete = true;
            }
            mutations.push(compact_json_put_mutation(
                progress_key,
                &progress,
                "serialize compact atom history upgrade",
            )?);
            if edges_written > 0 {
                let mut status = self.atom_ref_v2_backfill_status(storage_prefix).await?;
                status.edges_written = status.edges_written.saturating_add(edges_written);
                mutations.push(compact_json_put_mutation(
                    build_storage_key(
                        storage_prefix,
                        molecule_key_codec::ATOM_REF_V2_REINDEX_CHECKPOINT_KEY,
                    ),
                    &status,
                    "serialize compact atom reverse-edge rebuild status",
                )?);
            }
            self.main_store
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "write compact atom history upgrade page: {error}"
                    ))
                })?;
        }

        if progress.complete {
            let audit = self
                .audit_atom_ref_v2_molecule(molecule_uuid, storage_prefix)
                .await?;
            if audit.missing_edges != 0 || audit.invalid_live_keys != 0 {
                return Err(SchemaError::InvalidData(format!(
                    "compact atom reverse-edge audit failed for molecule {molecule_uuid}: {} missing edge(s), {} invalid key(s)",
                    audit.missing_edges, audit.invalid_live_keys
                )));
            }
            let complete = AtomRefMoleculeManifest {
                version: ATOM_REF_MANIFEST_VERSION_HISTORY,
                molecule_uuid: molecule_uuid.to_string(),
                mutation_watermark_nanos: manifest.mutation_watermark_nanos,
                replay_complete: true,
            };
            self.main_store
                .inner()
                .batch_mutate(vec![compact_manifest_mutation(&complete, storage_prefix)?])
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "complete compact atom reverse-edge manifest: {error}"
                    ))
                })?;
        }
        Ok(progress)
    }

    /// Reset compact rebuild markers only on a throwaway proof copy.
    #[cfg(feature = "cloud-sync")]
    pub async fn reset_atom_ref_v2_reindex_for_isolated_copy_proof(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.main_store
            .batch_delete_keys(vec![
                build_storage_key(storage_prefix, molecule_key_codec::ATOM_REF_V2_COMPLETE_KEY),
                build_storage_key(
                    storage_prefix,
                    molecule_key_codec::ATOM_REF_V2_REINDEX_CHECKPOINT_KEY,
                ),
                build_storage_key(
                    storage_prefix,
                    molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
                ),
            ])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "reset compact atom reverse-edge rebuild markers on isolated copy: {error}"
                ))
            })
    }
}
