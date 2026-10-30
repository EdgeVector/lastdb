use super::*;

impl AtomStore {
    pub(super) async fn filter_generation_deletes(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        Ok(self
            .filter_generation_deletes_with_cost(molecule_uuid, delete_prefix, storage_prefix, rows)
            .await?
            .0)
    }

    pub(super) async fn filter_generation_deletes_with_cost(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        self.filter_generation_deletes_with_page_rows(
            molecule_uuid,
            delete_prefix,
            storage_prefix,
            rows,
            MATERIALIZE_PAGE_ROWS,
        )
        .await
    }

    pub(super) async fn filter_generation_deletes_with_page_rows(
        &self,
        molecule_uuid: &str,
        delete_prefix: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
        page_rows: usize,
    ) -> Result<(Vec<(String, PerKeyRecord)>, MaterializeReadCost), SchemaError> {
        let storage_delete_prefix = build_storage_key(storage_prefix, delete_prefix);
        let deletes = self
            .scan_exact_prefix_pages_with_limit::<MoleculeGenerationDelete>(
                &storage_delete_prefix,
                &format!("generation deletes for molecule {molecule_uuid}"),
                page_rows,
            )
            .await?;
        let cost = deletes.cost;
        Ok((
            Self::rows_after_generation_deletes(molecule_uuid, storage_prefix, rows, deletes.rows),
            cost,
        ))
    }

    pub(super) async fn filter_generation_deletes_range(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let (Some(start), Some(end)) = (
            molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                base_start,
            ),
            molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                base_end,
            ),
        ) else {
            return Ok(rows);
        };
        let start = build_storage_key(storage_prefix, &start);
        let end = build_storage_key(storage_prefix, &end);
        let deletes: Vec<(String, MoleculeGenerationDelete)> = self
            .main_store
            .scan_items_in_range(&start, &end)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan generation delete range {start}..{end}: {error}"
                ))
            })?;
        Ok(Self::rows_after_generation_deletes(
            molecule_uuid,
            storage_prefix,
            rows,
            deletes,
        ))
    }

    /// Keep the scanned row order. A later delete for one key replaces an earlier one.
    fn rows_after_generation_deletes(
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
        scanned: Vec<(String, MoleculeGenerationDelete)>,
    ) -> Vec<(String, PerKeyRecord)> {
        let strip = build_storage_key(storage_prefix, "");
        let deletes: HashMap<String, MoleculeGenerationDelete> = scanned
            .into_iter()
            .filter_map(|(key, deletion)| {
                let bare = key.strip_prefix(&strip).unwrap_or(&key);
                molecule_key_codec::molecule_record_key_from_generation_delete_key(
                    molecule_uuid,
                    bare,
                )
                .map(|key| (key, deletion))
            })
            .collect();
        rows.into_iter()
            .filter_map(|(key, record)| {
                Self::apply_generation_delete(Some(record), deletes.get(&key).cloned())
                    .map(|record| (key, record))
            })
            .collect()
    }
}
