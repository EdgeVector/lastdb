//! Conflict reads of a format 7 home: reads by key of the markers and rows
//! that a peer import writes beside each record.

use std::collections::BTreeSet;

use crate::db_operations::atom_store::{record_scope, RecordFieldConflicts};
use crate::schema::types::KeyValue;

use super::FoldDB;

impl FoldDB {
    /// True when this home stores one record at each key (format 7).
    pub fn record_home(&self) -> bool {
        self.db_ops.atoms().record_home()
    }

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
            .atoms()
            .record_conflict_fields(&records)
            .await
            .ok()
    }

    /// The kept losing values of the selected fields of each selected record:
    /// `(schema name, key, fields)`. The read takes the serving fence with a
    /// finite deadline, as the molecule metadata read does, and writes
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
                .atoms()
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
        if !self.record_home() {
            return None;
        }
        let schema = self
            .schema_manager
            .get_schema_metadata(schema_name)
            .ok()??;
        let scope = record_scope(&schema);
        let atoms = self.db_ops.atoms();
        let molecule_keys = self.db_ops.molecule_keys();
        keys.iter()
            .map(|key| {
                let key = molecule_keys.canonical_key(&schema, key.clone()).ok()?;
                atoms.record_key(&schema, &key, scope.as_deref()).ok()
            })
            .collect()
    }
}
