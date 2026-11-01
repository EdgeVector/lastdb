//! The keys of a TTL sweep batch that are already gone.

use super::*;
use lastdb_model::record_value::StoredRecord;

impl MutationManager {
    /// The keys of `keys` that no live record of `schema_name` holds.
    ///
    /// A purge refuses a batch that names a key already gone. The sweep calls
    /// this after such a refusal, drops the absent keys, and sends the rest.
    /// A key is gone when its record holds no field value. One batch read by
    /// key.
    pub async fn absent_erase_targets(
        &self,
        schema_name: &str,
        keys: &[crate::schema::types::KeyValue],
    ) -> Result<Vec<crate::schema::types::KeyValue>, SchemaError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let scope = crate::db_operations::atom_store::record_scope(&schema);
        let atoms = self.db_ops.atoms();
        let record_keys = keys
            .iter()
            .map(|key| atoms.record_key(&schema, key, scope.as_deref()))
            .collect::<Result<Vec<_>, _>>()?;
        let records = atoms.read_records(&record_keys).await?;
        Ok(keys
            .iter()
            .zip(records)
            .filter(|(_, record)| !record.as_deref().is_some_and(StoredRecord::is_live))
            .map(|(key, _)| key.clone())
            .collect())
    }
}
