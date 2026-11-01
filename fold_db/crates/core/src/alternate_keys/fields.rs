//! Read the alternate source fields of primary records in one batch read.

use super::*;
use crate::db_operations::record_store::RecordStore;
use std::collections::BTreeSet;

impl RecordStore {
    pub(crate) async fn schema_fields_at_keys(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        scope: Option<&str>,
        names: &BTreeSet<&str>,
    ) -> Result<Vec<RecordFields>, SchemaError> {
        if names.len().saturating_mul(keys.len()) > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate field dependencies exceed 4096 records"));
        }
        // One record holds every field of its key.
        let storage = keys
            .iter()
            .map(|key| self.record_key(schema, key, scope))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self
            .read_records(&storage)
            .await?
            .into_iter()
            .map(|record| {
                record.map_or_else(RecordFields::new, |record| {
                    record
                        .fields
                        .iter()
                        .filter(|(name, _)| names.contains(name.as_str()))
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect()
                })
            })
            .collect())
    }
}
