use super::*;

impl AtomStore {
    /// Check one exact Delete barrier for each returned molecule row. A range
    /// read never walks the barriers; it checks only the keys it already found.
    pub(crate) async fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<(String, PerKeyRecord)>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let final_keys: Vec<String> = rows
            .iter()
            .map(|(base, _)| build_storage_key(storage_prefix, base))
            .collect();
        let barriers = self.winning_delete_barriers(&final_keys).await?;
        Ok(rows
            .into_iter()
            .filter_map(|(base, record)| {
                let key = build_storage_key(storage_prefix, &base);
                barriers
                    .get(&key)
                    .is_none_or(|barrier| !barrier.blocks_tip(&record.entry))
                    .then_some((base, record))
            })
            .collect())
    }
}
