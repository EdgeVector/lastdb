//! Stage derived entry changes in the request's existing native transaction.
//!
//! An entry belongs to the record that it names. Two records can hold one
//! alternate key for a moment: a batch that moves a value from one record to
//! another, or two requests that each write the value. So the changes resolve
//! per storage key: a put in the change set wins over a delete of the same
//! entry, and a record that leaves a key deletes the entry only when the
//! stored entry still names that record. The read of the stored entries runs
//! in the request, so it sees the entries that an earlier group of the request
//! staged.

use super::*;
use crate::db_operations::record_store::RecordStore;
use crate::storage::KvMutation;

/// One entry that a record leaves: the storage key and the record's primary key.
type Leave<'a> = (String, &'a KeyValue);

/// The entries that the records put (storage key to encoded entry) and leave.
type EntryMoves<'a> = (BTreeMap<String, Vec<u8>>, Vec<Leave<'a>>);

impl RecordStore {
    /// The entry changes between the old and the new field values at each
    /// primary key. A format 7 record write already holds both values, so it
    /// puts these changes in its own batch instead of reading the tips again.
    pub(crate) async fn alternate_entry_changes(
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
        let (puts, mut leaves) = self.entry_moves(schema, keys, before, after, scope)?;
        if puts.len() + leaves.len() > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate change batch exceeds 4096 entries"));
        }
        leaves.retain(|(key, _)| !puts.contains_key(key));
        let mut changes: Vec<KvMutation> = self
            .owned_entries(schema, leaves)
            .await?
            .into_iter()
            .map(KvMutation::delete)
            .collect();
        changes.extend(
            puts.into_iter()
                .map(|(key, value)| KvMutation::put(key, value)),
        );
        Ok(changes)
    }

    /// The entries that the records put, by storage key, and the entries that
    /// they leave. A later put of one storage key replaces an earlier one.
    fn entry_moves<'a>(
        &self,
        schema: &Schema,
        keys: &'a [KeyValue],
        before: &[RecordFields],
        after: &[RecordFields],
        scope: Option<&str>,
    ) -> Result<EntryMoves<'a>, SchemaError> {
        let mut puts = BTreeMap::new();
        let mut leaves = Vec::new();
        for ((primary, before), after) in keys.iter().zip(before).zip(after) {
            let old = derived(schema, before)?;
            let new = derived(schema, after)?;
            for name in schema.alternate_keys.keys() {
                if old.get(name) == new.get(name) {
                    continue;
                }
                if let Some(key) = old.get(name) {
                    let storage = build_storage_key(scope, &storage_key(self, schema, name, key)?);
                    leaves.push((storage, primary));
                }
                if let Some(key) = new.get(name) {
                    let entry = AlternateEntry {
                        primary: primary.clone(),
                        matched: key.clone(),
                    };
                    puts.insert(
                        build_storage_key(scope, &storage_key(self, schema, name, key)?),
                        serde_json::to_vec(&entry)
                            .map_err(|_| invalid("encode alternate entry"))?,
                    );
                }
            }
        }
        Ok((puts, leaves))
    }

    /// The storage keys of the left entries that still name their record.
    /// One read by key; an absent entry needs no delete.
    ///
    /// Two primary keys name one record when they encode to one record key.
    /// The text of a key is not its identity: the record key blinds the hash
    /// and types the range, so two texts can name one record (an Integer range
    /// written as `7` and as `07`, or one DateTime instant in two UTC offsets).
    /// A text compare kept such an entry after its record left the key. A
    /// stored primary that no longer encodes names no record that this write
    /// can own, so it stays.
    async fn owned_entries(
        &self,
        schema: &Schema,
        leaves: Vec<Leave<'_>>,
    ) -> Result<Vec<String>, SchemaError> {
        if leaves.is_empty() {
            return Ok(Vec::new());
        }
        let stored = self
            .raw()
            .inner()
            .get_many(
                leaves
                    .iter()
                    .map(|(key, _)| key.clone().into_bytes())
                    .collect(),
            )
            .await?;
        if stored.len() != leaves.len() {
            return Err(invalid("alternate entry read batch is incomplete"));
        }
        let mut owned = Vec::new();
        for ((key, primary), bytes) in leaves.into_iter().zip(stored) {
            let Some(bytes) = bytes else { continue };
            let entry: AlternateEntry =
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid alternate entry"))?;
            let leaving = self.record_key(schema, primary, None)?;
            let named = self.record_key(schema, &entry.primary, None).ok();
            if named.as_deref() == Some(leaving.as_str()) {
                owned.push(key);
            }
        }
        Ok(owned)
    }
}
