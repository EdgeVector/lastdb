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
