//! The side rows of the merged records of one schema, as a local write
//! stages them (`write/record_effects.rs`): the alternate entries, the field
//! history copies of the overwritten and deleted values, and the retention
//! rows. A peer delete writes no `dellog:` row: that audit names the caller
//! of an erase on this device.

use super::merge::PeerRecord;
use super::*;
use crate::db_operations::atom_store::record_scope;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::KvMutation;

impl MutationManager {
    /// Stage the merged records of one schema with their alternate entries,
    /// their field history rows and `entries`, then their retention rows.
    pub(super) async fn stage_peer_schema_records(
        &self,
        schema: &Schema,
        records: &[&PeerRecord],
        mut entries: Vec<KvMutation>,
    ) -> Result<(), SchemaError> {
        let atoms = self.db_ops.atoms();
        let scope = record_scope(schema);
        let keys: Vec<KeyValue> = records.iter().map(|record| record.key.clone()).collect();
        let before: Vec<_> = records
            .iter()
            .map(|record| {
                record
                    .before
                    .as_ref()
                    .map(|stored| stored.fields.clone())
                    .unwrap_or_default()
            })
            .collect();
        let after: Vec<_> = records
            .iter()
            .map(|record| record.after.fields.clone())
            .collect();
        entries.extend(atoms.alternate_entry_changes(
            schema,
            &keys,
            &before,
            &after,
            scope.as_deref(),
        )?);
        let changes: Vec<_> = records.iter().copied().map(PeerRecord::change).collect();
        let purges = vec![false; changes.len()];
        let history = atoms
            .record_history_rows(schema, scope.as_deref(), &changes, &purges)
            .await?;
        entries.extend(history.rows);
        let ages: Vec<Option<u64>> = changes
            .iter()
            .map(|change| change.after.written_at().filter(|_| change.after.is_live()))
            .collect();
        atoms.stage_records(changes, entries, history.blobs).await?;
        self.stage_peer_retention_rows(schema, &keys, &ages).await
    }

    /// The retention rows of the merged records: the hash partitions of a
    /// HashRange schema, and for a Hash or Single schema with a policy the
    /// write age of each live record and the removal of each deleted one.
    async fn stage_peer_retention_rows(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        written_at: &[Option<u64>],
    ) -> Result<(), SchemaError> {
        let schemas = self.db_ops.schemas();
        if schema.schema_type == DeclarativeSchemaType::HashRange {
            return schemas
                .record_schema_retention_hash_partitions(&schema.name, keys)
                .await;
        }
        if !matches!(
            schema.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
        ) || schemas
            .get_schema_retention_policy(&schema.name)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let mut ages = Vec::new();
        let mut removed = Vec::new();
        for (key, nanos) in keys.iter().zip(written_at) {
            match nanos {
                Some(nanos) => ages.push((key.clone(), nanos / 1_000_000_000)),
                None => removed.push(key.clone()),
            }
        }
        schemas
            .record_schema_retention_write_ages(&schema.name, &ages)
            .await?;
        schemas
            .remove_schema_retention_keys(&schema.name, &removed)
            .await
    }
}
