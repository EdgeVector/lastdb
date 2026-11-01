//! Stage derived entry changes in the request's existing native transaction.

use super::*;
use crate::db_operations::atom_store::AtomStore;
use crate::storage::KvMutation;

impl AtomStore {
    /// The entry changes between the old and the new field values at each
    /// primary key. A format 7 record write already holds both values, so it
    /// puts these changes in its own batch instead of reading the tips again.
    pub(crate) fn alternate_entry_changes(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        before: &[RecordFields],
        after: &[RecordFields],
        scope: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        if schema.alternate_keys.is_empty() || keys.is_empty() {
            return Ok(Vec::new());
        }
        if before.len() != keys.len() || after.len() != keys.len() {
            return Err(invalid("alternate change batch is incomplete"));
        }
        let mut changes = Vec::new();
        for ((primary, before), after) in keys.iter().zip(before).zip(after) {
            let old = derived(schema, before)?;
            let new = derived(schema, after)?;
            for name in schema.alternate_keys.keys() {
                if old.get(name) == new.get(name) {
                    continue;
                }
                if let Some(key) = old.get(name) {
                    changes.push(KvMutation::delete(
                        build_storage_key(scope, &storage_key(self, schema, name, key)?).as_bytes(),
                    ));
                }
                if let Some(key) = new.get(name) {
                    let entry = AlternateEntry {
                        primary: primary.clone(),
                        matched: key.clone(),
                    };
                    changes.push(KvMutation::put(
                        build_storage_key(scope, &storage_key(self, schema, name, key)?).as_bytes(),
                        serde_json::to_vec(&entry)
                            .map_err(|_| invalid("encode alternate entry"))?,
                    ));
                }
            }
        }
        if changes.len() > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate change batch exceeds 4096 entries"));
        }
        Ok(changes)
    }
}
