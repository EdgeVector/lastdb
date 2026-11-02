//! Conflict reads of a home: reads by key of the markers and rows
//! that a peer import writes beside each record.

use std::collections::BTreeSet;

use crate::db_operations::record_store::{record_scope, RecordFieldConflicts};
use crate::schema::types::KeyValue;

use super::FoldDB;

impl FoldDB {
    /// The conflicted fields of the record at each key of one schema, in
    /// order: one read by key of the markers. `None` when the schema or a key
    /// does not resolve, or the read fails; that is unknown, not clean.
    pub async fn record_conflict_flags(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Option<Vec<BTreeSet<String>>> {
        let records = self.record_storage_keys(schema_name, keys)?;
        self.db_ops
            .records()
            .record_conflict_fields(&records)
            .await
            .ok()
    }

    /// The kept losing values of the selected fields of each selected record:
    /// `(schema name, key, fields)`. The read takes the serving fence with a
    /// finite deadline, as the field key metadata read does, and writes
    /// nothing. `None` is unknown.
    pub async fn selected_record_conflicts(
        &self,
        selections: &[(String, KeyValue, Vec<String>)],
    ) -> Option<Vec<Vec<RecordFieldConflicts>>> {
        let mut pairs = Vec::with_capacity(selections.len());
        for (schema_name, key, fields) in selections {
            let record = self
                .record_storage_keys(schema_name, std::slice::from_ref(key))?
                .pop()?;
            pairs.push((record, fields.clone()));
        }
        let router = self.mutation_manager.capture_router()?;
        let read = async {
            let _fence = router.fence_mutations().await;
            self.db_ops
                .records()
                .selected_record_conflicts(&pairs)
                .await
                .ok()
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), read)
            .await
            .ok()
            .flatten()
    }

    fn record_storage_keys(&self, schema_name: &str, keys: &[KeyValue]) -> Option<Vec<String>> {
        let schema = self
            .schema_manager
            .get_schema_metadata(schema_name)
            .ok()??;
        let scope = record_scope(&schema);
        let records = self.db_ops.records();
        let field_keys = self.db_ops.field_keys();
        keys.iter()
            .map(|key| {
                let key = field_keys.canonical_key(&schema, key.clone()).ok()?;
                records.record_key(&schema, &key, scope.as_deref()).ok()
            })
            .collect()
    }
}
