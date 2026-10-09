use std::collections::HashMap;

use crate::atom::ImportedFieldProvenance;
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;

use super::FoldDB;

impl FoldDB {
    /// Extracts every field's molecule signature for the record at `key` in
    /// transplantable form (see [`crate::atom::ImportedFieldProvenance`]).
    /// Sender side of cross-node provenance: an outbound `data_share` calls
    /// this so the receiver can verify and store the original author's
    /// signatures instead of re-signing the records locally. Fields whose
    /// entry at `key` is absent or unsigned are simply omitted — the caller
    /// degrades those fields to attribution-only sharing.
    pub async fn field_provenance_for_key(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<HashMap<String, ImportedFieldProvenance>, SchemaError> {
        let mut by_key = self
            .field_provenance_for_keys(schema_name, std::slice::from_ref(key))
            .await?;
        Ok(by_key.remove(key).unwrap_or_default())
    }

    /// Batch variant of [`Self::field_provenance_for_key`]: extract signed
    /// provenance for MANY record keys of one schema while loading the schema
    /// and refreshing each field's molecule exactly once, instead of once per
    /// key. A delivery snapshot materializes thousands of records; the per-key
    /// form made that O(records × fields) molecule reads (N+1 sweep
    /// 2026-07-02). Returns one (possibly empty) map per requested key.
    pub async fn field_provenance_for_keys(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<HashMap<KeyValue, HashMap<String, ImportedFieldProvenance>>, SchemaError> {
        let mut out = HashMap::with_capacity(keys.len());
        if keys.is_empty() {
            return Ok(out);
        }
        let Some(mut schema) = self
            .schema_manager
            .get_schema_following_supersession(schema_name)
            .await?
        else {
            return Ok(out);
        };
        for field in schema.runtime_fields.values_mut() {
            field.refresh_from_db(&self.db_ops).await?;
        }
        for key in keys {
            let mut per_field = HashMap::new();
            for (field_name, field) in &schema.runtime_fields {
                if let Some(provenance) = field.signed_entry_provenance(key) {
                    per_field.insert(field_name.clone(), provenance);
                }
            }
            out.insert(key.clone(), per_field);
        }
        Ok(out)
    }
}
