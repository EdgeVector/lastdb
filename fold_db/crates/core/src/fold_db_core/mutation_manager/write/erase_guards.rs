//! The keys of a TTL sweep batch that are already gone.

use super::*;

impl MutationManager {
    /// The keys of `keys` that no field of `schema_name` has a record for.
    ///
    /// A purge refuses a batch that names a key already gone. The sweep calls
    /// this after such a refusal, drops the absent keys, and sends the rest.
    pub async fn absent_erase_targets(
        &self,
        schema_name: &str,
        keys: &[crate::schema::types::KeyValue],
    ) -> Result<Vec<crate::schema::types::KeyValue>, SchemaError> {
        // A format 7 home has no tips: a key is gone when its record holds no
        // field value. One batch read by key.
        if self.db_ops.atoms().record_home() {
            let schema = self.schema_manager.require_schema_metadata(schema_name)?;
            let scope = crate::db_operations::atom_store::record_scope(&schema);
            let atoms = self.db_ops.atoms();
            let record_keys = keys
                .iter()
                .map(|key| atoms.record_key(&schema, key, scope.as_deref()))
                .collect::<Result<Vec<_>, _>>()?;
            let records = atoms.read_records(&record_keys).await?;
            return Ok(keys
                .iter()
                .zip(records)
                .filter(|(_, record)| !record.as_ref().is_some_and(|record| record.is_live()))
                .map(|(key, _)| key.clone())
                .collect());
        }
        super::super::super::purge::absent_purge_targets(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            keys,
            None,
        )
        .await
    }
}
