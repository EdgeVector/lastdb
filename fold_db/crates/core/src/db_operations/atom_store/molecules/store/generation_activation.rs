use super::*;

impl AtomStore {
    #[cfg(feature = "sharing")]
    pub(in super::super) async fn prepare_molecule_generation_activation(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
        generation_cut: super::super::PreparedMoleculeGeneration,
    ) -> Result<super::super::PreparedMoleculeGenerationActivation, SchemaError> {
        // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
        let records = self.per_key_storage_records(molecule_uuid, data, domain)?;
        // A full rewrite requires the molecule's complete key set. A tail
        // molecule holds only the keys this write touched.
        if data.order_is_tail() {
            return Err(SchemaError::InvalidData(format!(
                "refusing full rewrite of molecule {molecule_uuid} from a write-only \
                 (partial) molecule: it holds {} touched key(s) and an order tail, \
                 not the molecule's complete state",
                data.per_key_records().len()
            )));
        }
        let header = MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        };

        // Page index (`mhr:`) is retired — see HASH_RANGE_PAGE_INDEX_ENABLED.
        let include_hash_range_page_index =
            super::super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED;
        let legacy_page_index_complete_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::legacy_hash_range_page_index_complete_key(molecule_uuid),
        );
        let legacy_page_index_complete = if include_hash_range_page_index {
            self.main_store
                .exists_item(&legacy_page_index_complete_key)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "probe legacy HashRange page index marker {molecule_uuid}: {e}"
                    ))
                })?
        } else {
            false
        };
        if legacy_page_index_complete {
            self.main_store
                .delete_item(&legacy_page_index_complete_key)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "delete legacy HashRange page index marker {molecule_uuid}: {e}"
                    ))
                })?;
        }
        let mut items: Vec<(String, Value)> = Vec::with_capacity(
            records.len()
                + if include_hash_range_page_index {
                    records.len() * 2 + 3
                } else {
                    2
                },
        );
        // The complete tips land in an immutable generation. Ordinary writers
        // keep appending live `mk:` changes while that body is built; the final
        // pointer and the structural rows below share one small activation
        // batch.
        // Archived tip versions for the tip-version chain (`tv:{id}`).
        for (version_id, entry) in data.pending_tip_versions() {
            let value = serde_json::to_value(entry).map_err(|e| {
                SchemaError::InvalidData(format!("serialize tip version {version_id}: {e}"))
            })?;
            items.push((
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::tip_version_key(version_id),
                ),
                value,
            ));
        }
        // Derived atom -> archived-tip access pattern. These rows land in the
        // same durable batch as their authoritative `tv:` nodes, including
        // replay/full-rewrite paths.
        items.extend(self.pending_tip_version_backref_items(
            molecule_uuid,
            data,
            &records,
            storage_prefix,
        )?);
        // Shadow atom-reference edges share this durable batch with the `mk:`
        // tips and optional `tv:` history rows they describe.
        items.extend(self.pending_atom_ref_edge_items(
            molecule_uuid,
            data,
            &records,
            storage_prefix,
        )?);
        if include_hash_range_page_index {
            items.extend(records.iter().filter_map(|(base_key, _)| {
                hash_range_page_index_item_for_record_key(molecule_uuid, base_key, storage_prefix)
            }));
            if !legacy_page_index_complete {
                // Stamp the marker with the header this full rewrite is writing:
                // the index emitted just above is derived from exactly these
                // records, so a later rebuild at an unchanged header is provably
                // a no-op and can be skipped.
                items.push(hash_range_page_index_complete_item(
                    molecule_uuid,
                    header.version,
                    header.updated_at,
                    false,
                    storage_prefix,
                ));
            }
        }
        // HashKey lookup markers (`mhk:`) — retired co-write; see
        // HASH_RANGE_HASH_KEY_LOOKUP_ENABLED.
        if super::super::super::helpers::HASH_RANGE_HASH_KEY_LOOKUP_ENABLED {
            items.extend(Self::hash_key_lookup_items_for_records(
                molecule_uuid,
                &records,
                storage_prefix,
            )?);
        }
        let header_value = serde_json::to_value(&header)
            .map_err(|e| SchemaError::InvalidData(format!("serialize molecule header: {e}")))?;
        items.push((
            build_storage_key(
                storage_prefix,
                &molecule_key_codec::header_key(molecule_uuid),
            ),
            header_value,
        ));

        let tip_items: Vec<(String, Value)> = records
            .iter()
            .map(|(base_key, record)| {
                serde_json::to_value(record)
                    .map(|value| (build_storage_key(storage_prefix, base_key), value))
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize generation tip record: {error}"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        let automatic_gc_uuids = self.automatic_gc_tip_reference_uuids(&tip_items, storage_prefix);
        items
            .extend(self.automatic_gc_reference_marker_items(&automatic_gc_uuids, storage_prefix)?);
        // Transition mirror for existing bounded maintenance readers.
        // LEGACY-SUNSET: class=residue remove after every `mk:` maintenance
        // reader resolves the selected generation and one release dwell shows
        // zero direct-base fallbacks.
        items.extend(tip_items.clone());

        refuse_legacy_ref_blob_store_items(&items)?;
        self.account_keep_small_items(&tip_items);
        self.account_keep_small_items(&items);
        // Debounced persist only — never `flush_keep_small` on a write path.
        // The 2026-09-21 primary restart loop came from a per-write flush
        // here: every activation re-appended the whole per-molecule counter
        // map (~7 MB) into one `metadata` hash group, ~16 GB/hour.
        let _ = self.persist_keep_small().await;
        Ok(super::super::PreparedMoleculeGenerationActivation {
            molecule_uuid: molecule_uuid.to_string(),
            prepared: generation_cut,
            base_records: records,
            activation_items: items,
        })
    }

    /// Build every immutable molecule body, then select all bodies in one batch.
    #[cfg(feature = "sharing")]
    pub(crate) async fn store_molecule_generations_batch(
        &self,
        molecules: Vec<(
            String,
            MoleculeData,
            super::super::PreparedMoleculeGeneration,
        )>,
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
    ) -> Result<(), SchemaError> {
        let mut activations = Vec::with_capacity(molecules.len());
        for (molecule_uuid, data, generation_cut) in molecules {
            activations.push(
                self.prepare_molecule_generation_activation(
                    &molecule_uuid,
                    &data,
                    storage_prefix,
                    domain,
                    generation_cut,
                )
                .await?,
            );
        }
        self.activate_prepared_molecule_generations(activations, storage_prefix)
            .await
            .map(|_| ())
    }
}
