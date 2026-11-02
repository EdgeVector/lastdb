//! The native write: one record value at each primary key.
//!
//! Each mutation reads its record in the request's write scope and merges its
//! fields by stamp (decision D1). The records, their alternate entry changes,
//! the idempotency entries, the file blob counter changes, the field history
//! entries, the delete audit rows and the retention age rows join the
//! request's one batch (`record_effects.rs`). Writers that a record cannot
//! hold are refused here, before any byte is staged.

use super::*;
use crate::db_operations::record_store::{record_scope, RecordChange};
use crate::fold_db_core::purge::describe_key;
use crate::schema::types::cas::CasExpectation;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::{transaction, PackRuntime};
use lastdb_model::record_value::{FieldMerge, FieldStamp, StoredRecord};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::super::cas_rules::{check_expectation, CurrentHead};
use super::super::key_values::validate_field_value;
use super::record_effects::ChangedRecord;

impl MutationManager {
    /// Stage one schema group as records. A key appears once in a group, so a
    /// repeated key reads the record that the earlier group staged. The keys
    /// in `repeated` are written by another group of the request too: their
    /// cap waits for the final record, and the returned storage keys name the
    /// records whose check waits.
    pub(super) async fn stage_record_group(
        &self,
        schema_name: &str,
        mut mutations: Vec<Mutation>,
        idempotency_entries: Vec<(String, String)>,
        storage_prefix: Option<&str>,
        repeated: Option<&HashSet<KeyValue>>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<Vec<String>, SchemaError> {
        self.schema_manager.require_queryable_schema(schema_name)?;
        let schema = self
            .load_schema_for_delta(schema_name, storage_prefix)
            .await?;
        Self::require_record_write(&mutations)?;
        let scope = record_scope(&schema);
        let records = self.db_ops.records();
        let mut keys = Vec::with_capacity(mutations.len());
        let mut record_keys = Vec::with_capacity(mutations.len());
        for mutation in &mut mutations {
            let key = self.resolve_mutation_key_value(schema_name, &schema, mutation)?;
            self.materialize_key_fields(schema_name, &schema, mutation, &key)?;
            record_keys.push(records.record_key(&schema, &key, scope.as_deref())?);
            keys.push(key);
        }
        let stored = records.read_records(&record_keys).await?;
        let stored = record_keys.into_iter().zip(stored).collect();
        let group = merge_group(schema_name, &schema, &mutations, &keys, stored, repeated)?;
        let entries = records
            .alternate_entry_changes(
                &schema,
                &group.keys,
                &group.before,
                &group.after,
                scope.as_deref(),
            )
            .await?;
        let deferred = group.deferred();
        self.stage_records_with_effects(
            &schema,
            (storage_prefix, scope.as_deref()),
            &group.changed,
            group.changes,
            entries,
        )
        .await?;
        runtime.record_read_stage("record-stores-done", keys.len());

        if !idempotency_entries.is_empty() {
            self.db_ops
                .metadata()
                .batch_put_idempotency(idempotency_entries)
                .await?;
        }
        // The retention hash partitions are the keys a later TTL pass reads
        // by hash.
        if storage_prefix.is_none() && schema.schema_type == DeclarativeSchemaType::HashRange {
            self.db_ops
                .schemas()
                .record_schema_retention_hash_partitions(schema_name, &keys)
                .await?;
        }
        if let Some(batch) = Self::build_index_change_batch(schema_name, &schema, &mutations, &keys)
        {
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                Self::deliver_search_outbox(inbox, batch).await
            })?;
        }
        Ok(deferred)
    }

    /// Refuse each writer that a record cannot hold.
    ///
    /// A signed import would keep the sender's key and signature with each
    /// value. A record names no writer (decision D7), so it cannot hold them.
    /// An import's `imported_version` signs only a Single field, which no home
    /// has. Aggregate writes were dropped (Tom, 2026-11-01).
    fn require_record_write(mutations: &[Mutation]) -> Result<(), SchemaError> {
        for mutation in mutations {
            if mutation.provenance.is_some() {
                return Err(unsupported("a signed imported write"));
            }
            if mutation.aggregate_set.is_some() || mutation.aggregate_derived_internal {
                return Err(unsupported("an aggregate write"));
            }
        }
        Ok(())
    }

    /// Check each CAS expectation, each `must_exist` update and each loud
    /// erasure against the stored record, before any group stages. One schema
    /// load for each schema and one batch read for the records.
    pub(super) async fn check_record_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let guarded: Vec<&Mutation> = mutations
            .iter()
            .filter(|mutation| {
                mutation.expected.is_some()
                    || Self::is_must_exist_update(mutation)
                    || loud_erasure(mutation).is_some()
            })
            .collect();
        if guarded.is_empty() {
            return Ok(());
        }
        let mut schemas: HashMap<&str, Schema> = HashMap::new();
        for &mutation in &guarded {
            let name = mutation.schema_name.as_str();
            if let std::collections::hash_map::Entry::Vacant(slot) = schemas.entry(name) {
                slot.insert(self.load_schema_for_delta(name, storage_prefix).await?);
            }
        }
        let record_store = self.db_ops.records();
        let mut keys = Vec::with_capacity(guarded.len());
        for &mutation in &guarded {
            let schema = &schemas[mutation.schema_name.as_str()];
            let key = self.resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            keys.push(record_store.record_key(schema, &key, record_scope(schema).as_deref())?);
        }
        let records = record_store.read_records(&keys).await?;
        // A purge of a deleted record finds the record while its field history
        // still holds copies of old values: the compliance verb releases them.
        let deleted: Vec<&str> = guarded
            .iter()
            .zip(&keys)
            .zip(&records)
            .filter(|((mutation, _), record)| {
                mutation.mutation_type == MutationType::Purge
                    && !record.as_deref().is_some_and(StoredRecord::is_live)
            })
            .map(|((_, key), _)| key.as_str())
            .collect();
        let mut with_history = std::collections::HashSet::new();
        if !deleted.is_empty() {
            let counts = record_store.record_history_counts(&deleted).await?;
            with_history.extend(
                deleted
                    .into_iter()
                    .zip(counts)
                    .filter(|(_, n)| *n > 0)
                    .map(|(key, _)| key),
            );
        }
        for ((mutation, record), key) in guarded.into_iter().zip(records).zip(&keys) {
            let record = record.unwrap_or_default();
            if Self::is_must_exist_update(mutation) && !record.is_live() {
                return Err(SchemaError::InvalidData(format!(
                    "update target not found: schema '{}', key {} — refusing to silently create a \
                     row from a partial update",
                    mutation.schema_name,
                    mutation.key_value.to_storage_key(),
                )));
            }
            let found = record.is_live() || with_history.contains(key.as_str());
            if let Some((noun, suffix)) = loud_erasure(mutation).filter(|_| !found) {
                // The miss wording for one absent key.
                return Err(SchemaError::InvalidData(format!(
                    "{noun} target not found: schema '{}', key {} — refusing to silently no-op \
                     {suffix}",
                    mutation.schema_name,
                    describe_key(&mutation.key_value),
                )));
            }
            if let Some(expected) = &mutation.expected {
                check_expectation(mutation, expected, record_head(&record, expected))?;
            }
        }
        Ok(())
    }
}

/// The records that one group changes, in the order of the mutations.
struct GroupChanges<'a> {
    changed: Vec<ChangedRecord<'a>>,
    changes: Vec<RecordChange>,
    /// The key and the fields before and after the merge of each change.
    keys: Vec<KeyValue>,
    before: Vec<BTreeMap<String, serde_json::Value>>,
    after: Vec<BTreeMap<String, serde_json::Value>>,
}

impl GroupChanges<'_> {
    /// The storage keys of the changes whose cap waits for the final record.
    fn deferred(&self) -> Vec<String> {
        self.changes
            .iter()
            .filter(|change| change.cap_deferred)
            .map(|change| change.key.clone())
            .collect()
    }
}

/// Merge each mutation of one group into its stored record. A mutation that
/// changes nothing stays out, except a purge of a deleted record.
fn merge_group<'a>(
    schema_name: &str,
    schema: &Schema,
    mutations: &'a [Mutation],
    keys: &'a [KeyValue],
    stored: Vec<(String, Option<Arc<StoredRecord>>)>,
    repeated: Option<&HashSet<KeyValue>>,
) -> Result<GroupChanges<'a>, SchemaError> {
    let mut group = GroupChanges {
        changed: Vec::with_capacity(mutations.len()),
        changes: Vec::with_capacity(mutations.len()),
        keys: Vec::with_capacity(mutations.len()),
        before: Vec::with_capacity(mutations.len()),
        after: Vec::with_capacity(mutations.len()),
    };
    for ((mutation, key), (record_key, before)) in mutations.iter().zip(keys).zip(stored) {
        let mut after = before.as_deref().cloned().unwrap_or_default();
        let merged = merge_mutation(schema_name, schema, mutation, &mut after)?;
        if !merged && !purges_deleted_record(mutation, before.as_deref()) {
            continue;
        }
        group.changed.push(ChangedRecord { mutation, key });
        group.keys.push(key.clone());
        group.before.push(
            before
                .as_ref()
                .map(|record| record.fields.clone())
                .unwrap_or_default(),
        );
        group.after.push(after.fields.clone());
        group.changes.push(RecordChange {
            key: record_key,
            before,
            after,
            cap_deferred: repeated.is_some_and(|repeated| repeated.contains(key)),
        });
    }
    Ok(group)
}

/// The miss wording of a delete that must find its record: a purge, or a
/// delete with `must_exist`. `None` for every other mutation.
fn loud_erasure(mutation: &Mutation) -> Option<(&'static str, &'static str)> {
    match mutation.mutation_type {
        MutationType::Purge => Some(("Purge", "(compliance verb)")),
        MutationType::Delete if mutation.must_exist == Some(true) => {
            Some(("Delete", "(must_exist)"))
        }
        _ => None,
    }
}

/// True for a compliance purge of a deleted record. The merge changes nothing,
/// but the purge still releases the field history of the record. The
/// precondition check refused a deleted record with no history. A TTL sweep
/// purge carries its cutoff and never takes this path.
fn purges_deleted_record(mutation: &Mutation, before: Option<&StoredRecord>) -> bool {
    mutation.mutation_type == MutationType::Purge
        && mutation.erase_written_before_nanos.is_none()
        && before.is_some_and(|record| !record.is_live())
}

/// Merge the fields and the metadata of one mutation into its record. True
/// when something changed; an equal or older stamp changes nothing. The
/// metadata belongs to the record (D7): a write without metadata keeps it, and
/// a write with metadata replaces only the keys that it sends.
///
/// A delete or a purge removes every value at or below its stamp and keeps
/// the stamp as the delete floor: the record becomes a tombstone, and its
/// alternate entries and file blob references go in the same batch. A delete
/// of an absent record writes nothing.
fn merge_mutation(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    record: &mut StoredRecord,
) -> Result<bool, SchemaError> {
    let stamp = field_stamp(mutation)?;
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        // The TTL sweep of a Hash or Single schema ages a record by its write
        // time. A record written at or after the cutoff is not expired. An
        // expired record is deleted with a floor that is a local write of this
        // device (its device ID and logical counter) at the cutoff, so a write
        // with a later stamp from any device stays, and a peer that receives
        // the floor in a capture event applies it as a delete.
        if let Some(cutoff) = mutation.erase_written_before_nanos {
            if record.written_at().is_some_and(|at| at >= cutoff) {
                return Ok(false);
            }
            return Ok(record.delete(FieldStamp {
                written_at: cutoff,
                ..stamp
            }));
        }
        return Ok(record.delete(stamp));
    }
    let mut changed = false;
    let metadata = mutation.metadata.as_ref().filter(|map| !map.is_empty());
    if mutation.source_file_name.is_some() || metadata.is_some() {
        changed |= record.write_meta(
            mutation.source_file_name.clone(),
            metadata
                .into_iter()
                .flatten()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            stamp.clone(),
        );
    }
    for (field, value) in &mutation.fields_and_values {
        if !validate_field_value(schema_name, schema, field, value)? {
            continue;
        }
        changed |= matches!(
            record.merge_field(field, Some(value.clone()), stamp.clone()),
            FieldMerge::Written { .. }
        );
    }
    Ok(changed)
}

/// The stamp of every field that one mutation writes. The author clock set the
/// write time and the logical counter before the native request; the device is
/// the author-clock writer.
fn field_stamp(mutation: &Mutation) -> Result<FieldStamp, SchemaError> {
    let written_at = mutation.imported_written_at.ok_or_else(|| {
        SchemaError::InvalidData("a record write lacks the author-clock write time".into())
    })?;
    let device_id = if mutation.author_clock_writer_id.is_empty() {
        mutation.pub_key.clone()
    } else {
        mutation.author_clock_writer_id.clone()
    };
    Ok(FieldStamp {
        written_at,
        logical_counter: mutation.logical_counter,
        device_id,
        mutation_id: mutation.uuid.clone(),
    })
}

/// The CAS head of one field in a record. A removed field is absent.
fn record_head(record: &StoredRecord, expected: &CasExpectation) -> CurrentHead {
    let field = expected.field();
    let value = record.fields.get(field).cloned();
    let mutation_uuid = value
        .as_ref()
        .and(record.stamps.get(field))
        .map(|stamp| stamp.mutation_id.clone());
    CurrentHead {
        mutation_uuid,
        value,
    }
}

fn unsupported(what: &str) -> SchemaError {
    SchemaError::InvalidData(format!("LastDB does not store {what}"))
}
