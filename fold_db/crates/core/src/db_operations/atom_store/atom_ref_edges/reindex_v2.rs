//! v2 backfill status, history-complete marker, molecule manifest and compact two-pass rebuild.

use super::*;

impl AtomStore {
    /// Read the compact-plane durable rebuild state without a data-plane scan.
    pub async fn atom_ref_v2_backfill_status(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<AtomRefBackfillStatus, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            molecule_key_codec::ATOM_REF_V2_REINDEX_CHECKPOINT_KEY,
        );
        let status: Option<AtomRefBackfillStatus> =
            self.main_store.get_item(&key).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load compact atom reverse-edge rebuild status: {error}"
                ))
            })?;
        Ok(status.unwrap_or_else(AtomRefBackfillStatus::compact_v2))
    }

    /// Return whether every known molecule finished compact history audit.
    pub async fn atom_ref_v2_history_complete(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
        );
        self.main_store.exists_item(&key).await.map_err(|error| {
            SchemaError::InvalidData(format!("probe compact atom history completion: {error}"))
        })
    }

    /// Mark one storage prefix complete after every catalog molecule passes.
    pub async fn mark_atom_ref_v2_history_complete(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let status = self.atom_ref_v2_backfill_status(storage_prefix).await?;
        if !status.completed || status.phase != AtomRefBackfillPhase::Complete {
            return Err(SchemaError::InvalidData(
                "compact tip replay is incomplete".to_string(),
            ));
        }
        self.main_store
            .inner()
            .batch_mutate(vec![KvMutation::put(
                build_storage_key(
                    storage_prefix,
                    molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
                )
                .into_bytes(),
                ATOM_REF_V2_COMPLETE_MARKER,
            )])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("mark compact atom history complete: {error}"))
            })
    }

    /// Point-read one molecule's compact-plane cutover manifest.
    pub async fn atom_ref_v2_molecule_manifest(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomRefMoleculeManifest>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_v2_molecule_manifest_key(molecule_uuid),
        );
        self.main_store.get_item(&key).await.map_err(|error| {
            SchemaError::InvalidData(format!("load compact atom reverse-edge manifest: {error}"))
        })
    }

    /// Seed a tip-complete compact manifest for a declared empty molecule.
    pub async fn ensure_atom_ref_v2_molecule_manifest_after_reindex(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomRefMoleculeManifest>, SchemaError> {
        if let Some(manifest) = self
            .atom_ref_v2_molecule_manifest(molecule_uuid, storage_prefix)
            .await?
        {
            return Ok(Some(manifest));
        }
        let status = self.atom_ref_v2_backfill_status(storage_prefix).await?;
        if !status.completed || status.phase != AtomRefBackfillPhase::Complete {
            return Ok(None);
        }
        let manifest = AtomRefMoleculeManifest {
            version: ATOM_REF_MANIFEST_VERSION_TIPS,
            molecule_uuid: molecule_uuid.to_string(),
            mutation_watermark_nanos: status.mutation_watermark_nanos,
            replay_complete: true,
        };
        let mutation = compact_manifest_mutation(&manifest, storage_prefix)?;
        self.main_store
            .inner()
            .batch_mutate(vec![mutation])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "seed compact atom reverse-edge manifest after reindex: {error}"
                ))
            })?;
        Ok(Some(manifest))
    }

    /// Run one bounded page of the compact two-pass rebuild.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn reindex_atom_ref_v2_edges(
        &self,
        storage_prefix: Option<&str>,
        slot_page: Option<usize>,
    ) -> Result<AtomRefBackfillReport, SchemaError> {
        let page = slot_page.unwrap_or(DEFAULT_REINDEX_SLOT_PAGE).max(1);
        let mut status = self.atom_ref_v2_backfill_status(storage_prefix).await?;
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
            .map_err(|error| {
                SchemaError::InvalidData(format!("compact atom reindex scan mk: {error}"))
            })?;
        let range_exhausted = rows.len() < raw_limit;

        let phase = status.phase;
        let mut mutations = Vec::new();
        let mut replay_edge_keys = BTreeSet::new();
        let mut walked = 0u64;
        let mut edge_writes = 0u64;
        let mut last_key = status.after_tip_key.clone();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            if status.after_tip_key.as_deref() == Some(full_key.as_str()) {
                continue;
            }
            last_key = Some(full_key.clone());
            walked = walked.saturating_add(1);
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                status.skipped_rows = status.skipped_rows.saturating_add(1);
                continue;
            };
            let Some(rest) = base_key.strip_prefix("mk:") else {
                status.skipped_rows = status.skipped_rows.saturating_add(1);
                continue;
            };
            let Some((molecule_uuid, _)) = rest.split_once(':') else {
                status.skipped_rows = status.skipped_rows.saturating_add(1);
                continue;
            };
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                status.skipped_rows = status.skipped_rows.saturating_add(1);
                continue;
            };
            let Ok(record): Result<PerKeyRecord, _> = serde_json::from_slice(&value) else {
                status.skipped_rows = status.skipped_rows.saturating_add(1);
                continue;
            };

            if status.current_molecule.as_deref() != Some(molecule_uuid) {
                match phase {
                    AtomRefBackfillPhase::Replay => {
                        if let Some(previous) =
                            status.current_molecule.replace(molecule_uuid.to_string())
                        {
                            mutations.push(compact_manifest_mutation(
                                &AtomRefMoleculeManifest {
                                    version: ATOM_REF_MANIFEST_VERSION_TIPS,
                                    molecule_uuid: previous,
                                    mutation_watermark_nanos: status.mutation_watermark_nanos,
                                    replay_complete: true,
                                },
                                storage_prefix,
                            )?);
                            status.molecules_complete = status.molecules_complete.saturating_add(1);
                        }
                    }
                    AtomRefBackfillPhase::Backfill => {
                        status.current_molecule = Some(molecule_uuid.to_string());
                        mutations.push(compact_manifest_mutation(
                            &AtomRefMoleculeManifest {
                                version: ATOM_REF_MANIFEST_VERSION_TIPS,
                                molecule_uuid: molecule_uuid.to_string(),
                                mutation_watermark_nanos: status.mutation_watermark_nanos,
                                replay_complete: false,
                            },
                            storage_prefix,
                        )?);
                    }
                    AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked => {}
                }
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
                if phase == AtomRefBackfillPhase::Replay {
                    replay_edge_keys.insert(edge.storage_key_v2(storage_prefix)?.into_bytes());
                } else {
                    mutations.push(compact_edge_put_mutation(&edge, storage_prefix)?);
                    edge_writes = edge_writes.saturating_add(1);
                }
            }
        }

        if phase == AtomRefBackfillPhase::Replay && !replay_edge_keys.is_empty() {
            let keys: Vec<Vec<u8>> = replay_edge_keys.into_iter().collect();
            let values = self
                .main_store
                .inner()
                .get_many(keys.clone())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "probe compact atom reverse edges during replay: {error}"
                    ))
                })?;
            for (key, value) in keys.into_iter().zip(values) {
                if value.as_deref() != Some(ATOM_REF_V2_ACTIVE_MARKER) {
                    mutations.push(KvMutation::put(key, ATOM_REF_V2_ACTIVE_MARKER));
                    edge_writes = edge_writes.saturating_add(1);
                }
            }
        }

        status.after_tip_key = last_key;
        status.slots_walked = status.slots_walked.saturating_add(walked);
        if phase == AtomRefBackfillPhase::Backfill {
            status.edges_written = status.edges_written.saturating_add(edge_writes);
        }

        if range_exhausted {
            match phase {
                AtomRefBackfillPhase::Backfill => {
                    status.phase = AtomRefBackfillPhase::Replay;
                    status.after_tip_key = None;
                    status.current_molecule = None;
                }
                AtomRefBackfillPhase::Replay => {
                    if let Some(last_molecule) = status.current_molecule.take() {
                        mutations.push(compact_manifest_mutation(
                            &AtomRefMoleculeManifest {
                                version: ATOM_REF_MANIFEST_VERSION_TIPS,
                                molecule_uuid: last_molecule,
                                mutation_watermark_nanos: status.mutation_watermark_nanos,
                                replay_complete: true,
                            },
                            storage_prefix,
                        )?);
                        status.molecules_complete = status.molecules_complete.saturating_add(1);
                    }
                    if status.skipped_rows == 0 {
                        status.phase = AtomRefBackfillPhase::Complete;
                        status.completed = true;
                        mutations.push(KvMutation::put(
                            build_storage_key(
                                storage_prefix,
                                molecule_key_codec::ATOM_REF_V2_COMPLETE_KEY,
                            )
                            .into_bytes(),
                            ATOM_REF_V2_COMPLETE_MARKER,
                        ));
                    } else {
                        status.phase = AtomRefBackfillPhase::Blocked;
                    }
                }
                AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked => {}
            }
        }

        mutations.push(compact_json_put_mutation(
            build_storage_key(
                storage_prefix,
                molecule_key_codec::ATOM_REF_V2_REINDEX_CHECKPOINT_KEY,
            ),
            &status,
            "serialize compact atom reverse-edge rebuild status",
        )?);
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "write compact atom reverse-edge rebuild page: {error}"
                ))
            })?;

        Ok(AtomRefBackfillReport {
            slots_walked: walked,
            edges_written: edge_writes,
            completed: status.completed,
            status,
        })
    }
}
