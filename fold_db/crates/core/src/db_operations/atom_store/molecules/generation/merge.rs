use super::*;

/// The stored bound for one generation row merge.
#[derive(Clone, Copy)]
enum GenerationRowBound<'a> {
    Prefix {
        base_prefix: &'a str,
    },
    Range {
        base_start: &'a str,
        base_end: &'a str,
    },
}

impl AtomStore {
    pub(in crate::db_operations::atom_store::molecules) async fn merge_generation_rows_with_page_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
        page_rows: usize,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok((live_rows, MaterializeReadCost::default()));
        };
        let prefix = molecule_key_codec::molecule_generation_record_prefix(
            molecule_uuid,
            &pointer.generation,
        );
        let storage_prefix_key = build_storage_key(storage_prefix, &prefix);
        let generation_rows = self
            .scan_exact_prefix_pages_with_limit::<MoleculeGenerationSlot>(
                &storage_prefix_key,
                &format!("molecule generation {molecule_uuid}"),
                page_rows,
            )
            .await?;
        let mut cost = generation_rows.cost;
        let merged = self.merge_generation_rows_from_selected(
            molecule_uuid,
            storage_prefix,
            &pointer.generation,
            live_rows,
            generation_rows.rows,
        )?;
        let (merged, delete_cost) = self
            .filter_generation_deletes_with_page_rows(
                molecule_uuid,
                &molecule_key_codec::molecule_generation_delete_prefix(molecule_uuid),
                storage_prefix,
                merged,
                page_rows,
            )
            .await?;
        cost.merge(delete_cost);
        Ok((merged, cost))
    }

    /// Merge rows for one bounded logical prefix without scanning any other
    /// hash partition in the generation.
    pub(crate) async fn merge_generation_prefix_rows(
        &self,
        molecule_uuid: &str,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        self.merge_generation_bounded_rows(
            molecule_uuid,
            storage_prefix,
            live_rows,
            GenerationRowBound::Prefix { base_prefix },
        )
        .await
    }

    /// Merge rows for one bounded logical key range.
    pub(crate) async fn merge_generation_range_rows(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        self.merge_generation_bounded_rows(
            molecule_uuid,
            storage_prefix,
            live_rows,
            GenerationRowBound::Range {
                base_start,
                base_end,
            },
        )
        .await
    }

    /// Load one active generation, merge the selected rows, then apply that caller's deletes.
    /// An absent generation or an absent bound returns the live rows.
    async fn merge_generation_bounded_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<(String, PerKeyRecord)>,
        bound: GenerationRowBound<'_>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(live_rows);
        };
        let rows: Vec<(String, MoleculeGenerationSlot)> = match bound {
            GenerationRowBound::Prefix { base_prefix } => {
                let Some(generation_prefix) =
                    molecule_key_codec::molecule_generation_bound_for_record_bound(
                        molecule_uuid,
                        &pointer.generation,
                        base_prefix,
                    )
                else {
                    return Ok(live_rows);
                };
                let storage_generation_prefix =
                    build_storage_key(storage_prefix, &generation_prefix);
                self.main_store
                    .scan_items_with_prefix(&storage_generation_prefix)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "scan generation prefix {storage_generation_prefix}: {error}"
                        ))
                    })?
            }
            GenerationRowBound::Range {
                base_start,
                base_end,
            } => {
                let (Some(start), Some(end)) = (
                    molecule_key_codec::molecule_generation_bound_for_record_bound(
                        molecule_uuid,
                        &pointer.generation,
                        base_start,
                    ),
                    molecule_key_codec::molecule_generation_bound_for_record_bound(
                        molecule_uuid,
                        &pointer.generation,
                        base_end,
                    ),
                ) else {
                    return Ok(live_rows);
                };
                let start = build_storage_key(storage_prefix, &start);
                let end = build_storage_key(storage_prefix, &end);
                self.main_store
                    .scan_items_in_range(&start, &end)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "scan generation range {start}..{end}: {error}"
                        ))
                    })?
            }
        };
        let merged = self.merge_generation_rows_from_selected(
            molecule_uuid,
            storage_prefix,
            &pointer.generation,
            live_rows,
            rows,
        )?;
        match bound {
            GenerationRowBound::Prefix { base_prefix } => {
                let delete_prefix =
                    molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                        molecule_uuid,
                        base_prefix,
                    )
                    .unwrap_or_else(|| {
                        molecule_key_codec::molecule_generation_delete_prefix(molecule_uuid)
                    });
                self.filter_generation_deletes(
                    molecule_uuid,
                    &delete_prefix,
                    storage_prefix,
                    merged,
                )
                .await
            }
            GenerationRowBound::Range {
                base_start,
                base_end,
            } => {
                self.filter_generation_deletes_range(
                    molecule_uuid,
                    base_start,
                    base_end,
                    storage_prefix,
                    merged,
                )
                .await
            }
        }
    }

    pub(super) fn merge_generation_rows_from_selected(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        generation: &str,
        live_rows: Vec<(String, PerKeyRecord)>,
        generation_rows: Vec<(String, MoleculeGenerationSlot)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let strip = build_storage_key(storage_prefix, "");
        let mut merged: BTreeMap<String, (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>)> =
            BTreeMap::new();
        for (key, record) in live_rows {
            merged.entry(key).or_default().0 = Some(record);
        }
        for (key, slot) in generation_rows {
            let bare = key.strip_prefix(&strip).unwrap_or(&key);
            let Some(record_key) = molecule_key_codec::molecule_record_key_from_generation_key(
                molecule_uuid,
                generation,
                bare,
            ) else {
                continue;
            };
            merged.entry(record_key).or_default().1 = Some(slot);
        }
        Ok(merged
            .into_iter()
            .filter_map(|(key, (live, base))| {
                Self::merge_generation_slot(live, base).map(|record| (key, record))
            })
            .collect())
    }
}
