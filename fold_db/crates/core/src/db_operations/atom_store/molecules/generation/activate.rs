use super::*;

/// Generation body rows to write, and the per-molecule state for activation.
type StagedGenerations = (Vec<(String, Value)>, Vec<ReadyMoleculeGenerationActivation>);

impl AtomStore {
    /// Capture the complete logical row set at the generation cut.
    pub(crate) async fn prepare_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<PreparedMoleculeGeneration, SchemaError> {
        let generation = uuid::Uuid::new_v4().simple().to_string();
        let live_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule_uuid),
        );
        let observed_live: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_with_prefix(&live_prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "capture live rows for molecule generation {molecule_uuid}: {error}"
                ))
            })?;
        let observed_live = Self::strip_per_key_storage_prefix(storage_prefix, observed_live);
        let (observed_live, _) = self
            .merge_generation_rows_with_page_rows(
                molecule_uuid,
                storage_prefix,
                observed_live,
                MATERIALIZE_PAGE_ROWS,
            )
            .await?;
        let mut observed: HashMap<(String, String), PerKeyRecord> = HashMap::new();
        for (key, record) in observed_live {
            if let Some(slot) = molecule_key_codec::decode_hash_range(&key, molecule_uuid) {
                observed.insert(slot, record);
            }
        }
        let current = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?;
        let previous_generation = current.as_ref().map(|value| value.generation.clone());
        let reclaim_generation = current.and_then(|value| value.previous_generation);
        Ok(PreparedMoleculeGeneration {
            generation,
            observed,
            previous_generation,
            reclaim_generation,
        })
    }

    /// Build immutable bases, then select every field generation in one batch.
    /// Ordinary writers continue to use `mk:` rows while each base is built.
    ///
    /// The activation copies tips into `storage_prefix`. An atom that has no
    /// counter in that scope gets its body from `bodies`, or from the personal
    /// scope, in the same batch as the tip that names it.
    pub(crate) async fn activate_prepared_molecule_generations(
        &self,
        activations: Vec<PreparedMoleculeGenerationActivation>,
        bodies: &[SealedAtom],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        if activations.is_empty() {
            return Ok(Vec::new());
        }

        let (generation_items, mut ready) =
            Self::stage_generation_bodies(activations, storage_prefix)?;
        self.write_generation_bodies(generation_items).await?;

        // Only the selector batch takes exact-slot locks. One native batch
        // activates every field or restores every prior selector.
        let tip_keys = Self::activation_tip_keys(&ready, storage_prefix);
        let tip_guards = self.lock_tip_commits(&tip_keys).await;
        let publication_guards = self.lock_tip_publications(&tip_keys).await;

        let activation_items: Vec<(String, Value)> = ready
            .iter_mut()
            .flat_map(|item| std::mem::take(&mut item.activation_items))
            .collect();
        let winners = self.retain_durable_tip_winners(activation_items).await?;
        let mut tips = winners.tips;
        let mut rows = winners.rows;
        for item in &ready {
            self.tombstone_stale_direct_rows(item, storage_prefix, &mut tips)
                .await?;
            rows.push(Self::generation_pointer_item(item, storage_prefix)?);
        }

        let accounted = super::super::store::accounted_items(&tips, &rows)?;
        let commit = self
            .commit_tip_changes(TipChangeSet {
                scope: storage_prefix,
                tips,
                bodies,
                body_source: Some(None),
                records: Vec::new(),
                rows,
                deletes: Vec::new(),
            })
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("activate molecule generation batch: {error}"))
            })?;
        self.account_stored_bodies(bodies, &commit);
        self.account_keep_small_items(&accounted, &commit);
        self.keep_small
            .settle_pending_atoms(bodies.iter().map(|(atom, _)| atom.uuid()));
        // Debounced persist only — never `flush_keep_small` on a write path.
        // The 2026-09-21 primary restart loop came from a per-write flush on
        // activation: every one re-appended the whole per-molecule counter map
        // (~7 MB) into one `metadata` hash group, ~16 GB/hour.
        let _ = self.persist_keep_small().await;
        drop(publication_guards);
        drop(tip_guards);

        let generations = ready.iter().map(|item| item.generation.clone()).collect();
        self.reclaim_expired_generations(ready, storage_prefix)
            .await?;
        Ok(generations)
    }

    /// Build the immutable generation body rows for every activation, and the
    /// per-molecule state the selector batch needs afterwards.
    fn stage_generation_bodies(
        activations: Vec<PreparedMoleculeGenerationActivation>,
        storage_prefix: Option<&str>,
    ) -> Result<StagedGenerations, SchemaError> {
        let mut generation_items = Vec::new();
        let mut ready = Vec::with_capacity(activations.len());
        for activation in activations {
            let PreparedMoleculeGenerationActivation {
                molecule_uuid,
                mut prepared,
                base_records,
                activation_items,
            } = activation;
            let generation = prepared.generation.clone();
            for (base_key, record) in base_records {
                let Some((hash, range)) =
                    molecule_key_codec::decode_hash_range(&base_key, &molecule_uuid)
                else {
                    return Err(SchemaError::InvalidData(format!(
                        "generation base contains a malformed molecule key: {base_key}"
                    )));
                };
                prepared.observed.remove(&(hash.clone(), range.clone()));
                let key = build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_record_key(
                        &molecule_uuid,
                        &generation,
                        &hash,
                        &range,
                    ),
                );
                generation_items.push((
                    key,
                    serde_json::to_value(MoleculeGenerationSlot {
                        record: Some(record),
                        shadow: None,
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize molecule generation slot: {error}"
                        ))
                    })?,
                ));
            }
            let stale_direct: Vec<_> = prepared.observed.drain().collect();
            for ((hash, range), old) in &stale_direct {
                let key = build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_record_key(
                        &molecule_uuid,
                        &generation,
                        hash,
                        range,
                    ),
                );
                generation_items.push((
                    key,
                    serde_json::to_value(MoleculeGenerationSlot {
                        record: None,
                        shadow: Some(old.entry.clone()),
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize molecule generation tombstone: {error}"
                        ))
                    })?,
                ));
            }
            ready.push(ReadyMoleculeGenerationActivation {
                molecule_uuid,
                prepared,
                generation,
                stale_direct,
                activation_items,
            });
        }
        Ok((generation_items, ready))
    }

    /// Write and flush the generation bodies before any selector is switched.
    async fn write_generation_bodies(
        &self,
        generation_items: Vec<(String, Value)>,
    ) -> Result<(), SchemaError> {
        if generation_items.is_empty() {
            return Ok(());
        }
        self.main_store
            .batch_put_items(generation_items)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("write molecule generation batch: {error}"))
            })?;
        self.main_store.inner().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!(
                "flush molecule generation bodies before activation: {error}"
            ))
        })
    }

    /// Sorted, deduplicated tip keys the selector batch must lock.
    fn activation_tip_keys(
        ready: &[ReadyMoleculeGenerationActivation],
        storage_prefix: Option<&str>,
    ) -> Vec<String> {
        let mut tip_keys = Vec::new();
        for item in ready {
            tip_keys.extend(
                item.activation_items
                    .iter()
                    .filter(|(key, _)| key.contains("mk:"))
                    .map(|(key, _)| key.clone()),
            );
            tip_keys.extend(item.stale_direct.iter().map(|((hash, range), _)| {
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::hash_range_record_key(&item.molecule_uuid, hash, range),
                )
            }));
        }
        tip_keys.sort_unstable();
        tip_keys.dedup();
        tip_keys
    }

    /// Tombstone the direct rows that the new generation shadows, unless a
    /// concurrent write already replaced them. The tombstoned row names the
    /// same atom, so the planner sees no change of references.
    async fn tombstone_stale_direct_rows(
        &self,
        item: &ReadyMoleculeGenerationActivation,
        storage_prefix: Option<&str>,
        tips: &mut Vec<TipChange>,
    ) -> Result<(), SchemaError> {
        let stale_keys: Vec<String> = item
            .stale_direct
            .iter()
            .map(|((hash, range), _)| {
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::hash_range_record_key(&item.molecule_uuid, hash, range),
                )
            })
            .collect();
        let current_stale = self
            .main_store
            .get_items::<PerKeyRecord>(&stale_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read stale direct rows for molecule {}: {error}",
                    item.molecule_uuid
                ))
            })?;
        for (((_, old), key), current) in
            item.stale_direct.iter().zip(stale_keys).zip(current_stale)
        {
            let Some(mut current) = current else {
                continue;
            };
            if current.entry.lww_key() != old.entry.lww_key() {
                continue;
            }
            current.meta.get_or_insert_with(Default::default).tombstoned = true;
            tips.push(TipChange {
                key,
                new: Some(current),
            });
        }
        Ok(())
    }

    /// The selector row that switches one molecule to its new generation.
    fn generation_pointer_item(
        item: &ReadyMoleculeGenerationActivation,
        storage_prefix: Option<&str>,
    ) -> Result<(String, Value), SchemaError> {
        let activated_at_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                SchemaError::InvalidData(format!("system clock before epoch: {error}"))
            })?
            .as_nanos() as u64;
        let pointer = MoleculeGenerationPointer {
            generation: item.generation.clone(),
            previous_generation: item.prepared.previous_generation.clone(),
            activated_at_unix_nanos,
        };
        Ok((
            build_storage_key(
                storage_prefix,
                &molecule_key_codec::molecule_generation_pointer_key(&item.molecule_uuid),
            ),
            serde_json::to_value(pointer).map_err(|error| {
                SchemaError::InvalidData(format!("serialize molecule generation pointer: {error}"))
            })?,
        ))
    }

    /// Delete the generation that the new one displaced two activations ago.
    async fn reclaim_expired_generations(
        &self,
        ready: Vec<ReadyMoleculeGenerationActivation>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for item in ready {
            let Some(reclaim_generation) = item.prepared.reclaim_generation else {
                continue;
            };
            let reclaim_prefix = build_storage_key(
                storage_prefix,
                &molecule_key_codec::molecule_generation_record_prefix(
                    &item.molecule_uuid,
                    &reclaim_generation,
                ),
            );
            let keys: Vec<String> = self
                .main_store
                .scan_items_with_prefix::<MoleculeGenerationSlot>(&reclaim_prefix)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "list expired molecule generation {reclaim_generation}: {error}"
                    ))
                })?
                .into_iter()
                .map(|(key, _)| key)
                .collect();
            if !keys.is_empty() {
                self.main_store
                    .batch_delete_keys(keys)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "reclaim expired molecule generation {reclaim_generation}: {error}"
                        ))
                    })?;
            }
        }
        Ok(())
    }
}
