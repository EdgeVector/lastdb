//! The native write of a format 7 home: one record value at each primary key.
//!
//! Each mutation reads its record in the request's write scope and merges its
//! fields by stamp (decision D1). The records, their alternate entry changes,
//! the idempotency entries and the file blob counter changes join the
//! request's one batch. A format 7 home writes no atom, tip or molecule
//! counter. Writers that the record model does not cover yet are refused
//! here, before any byte is staged.

use super::*;
use crate::db_operations::atom_store::{record_scope, RecordChange};
use crate::schema::types::cas::CasExpectation;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use crate::storage::disk_pack::{transaction, PackRuntime};
use lastdb_model::record_value::{FieldMerge, FieldStamp, StoredRecord};
use std::sync::Arc;

use super::super::cas_rules::{check_expectation, CurrentHead};
use super::super::molecules::field_rules::validate_field_value;

impl MutationManager {
    /// Stage one schema group as records. A key appears once in a group, so a
    /// repeated key reads the record that the earlier group staged.
    pub(super) async fn stage_record_group(
        &self,
        schema_name: &str,
        mut mutations: Vec<Mutation>,
        idempotency_entries: Vec<(String, String)>,
        storage_prefix: Option<&str>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        self.schema_manager.require_queryable_schema(schema_name)?;
        let schema = self
            .load_schema_for_delta(schema_name, storage_prefix)
            .await?;
        self.require_record_write(&schema, &mutations, storage_prefix, runtime)
            .await?;
        let scope = record_scope(&schema);
        let atoms = self.db_ops.atoms();
        let mut keys = Vec::with_capacity(mutations.len());
        let mut record_keys = Vec::with_capacity(mutations.len());
        for mutation in &mut mutations {
            let key = self.resolve_mutation_key_value(schema_name, &schema, mutation)?;
            self.materialize_key_fields(schema_name, &schema, mutation, &key)?;
            record_keys.push(atoms.record_key(&schema, &key, scope.as_deref())?);
            keys.push(key);
        }
        let stored = atoms.read_records(&record_keys).await?;

        let alternates = !schema.alternate_keys.is_empty();
        let mut changes = Vec::with_capacity(mutations.len());
        let (mut changed_keys, mut before_fields, mut after_fields) =
            (Vec::new(), Vec::new(), Vec::new());
        for (((mutation, key), record_key), before) in
            mutations.iter().zip(&keys).zip(record_keys).zip(stored)
        {
            let mut after = before.clone().unwrap_or_default();
            if !merge_mutation(schema_name, &schema, mutation, &mut after)? {
                continue;
            }
            if alternates {
                changed_keys.push(key.clone());
                before_fields.push(if let Some(record) = &before {
                    record.fields.clone()
                } else {
                    std::collections::BTreeMap::new()
                });
                after_fields.push(after.fields.clone());
            }
            changes.push(RecordChange {
                key: record_key,
                before,
                after,
            });
        }
        let entries = atoms.alternate_entry_changes(
            &schema,
            &changed_keys,
            &before_fields,
            &after_fields,
            scope.as_deref(),
        )?;
        atoms.stage_records(changes, entries).await?;
        runtime.record_read_stage("record-stores-done", keys.len());

        if !idempotency_entries.is_empty() {
            self.db_ops
                .metadata()
                .batch_put_idempotency(idempotency_entries)
                .await?;
        }
        // The retention hash partitions are the keys a later TTL pass reads
        // by hash; record them as the molecule path does.
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
        Ok(())
    }

    /// Refuse each writer that a record cannot hold yet. The molecule path
    /// keeps serving these in format 4 to 6 homes.
    async fn require_record_write(
        &self,
        schema: &Schema,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        // Capture marks the request ready even with no candidate, so an
        // uncaptured record write would pass the capture gate.
        if runtime.logical_capture_enabled() {
            return Err(unsupported("a write in a home with logical capture"));
        }
        if !self
            .active_share_prefixes(&schema.name, storage_prefix)
            .await?
            .is_empty()
        {
            return Err(unsupported("a write to a schema with an active share rule"));
        }
        if self
            .db_ops
            .schemas()
            .get_schema_retention_policy(&schema.name)
            .await?
            .is_some()
        {
            return Err(unsupported("a write to a schema with a retention policy"));
        }
        for mutation in mutations {
            if !matches!(
                mutation.mutation_type,
                MutationType::Create | MutationType::Update
            ) {
                return Err(unsupported("a delete"));
            }
            if mutation.provenance.is_some() || mutation.imported_version.is_some() {
                return Err(unsupported("an imported write"));
            }
            if mutation.aggregate_set.is_some() || mutation.aggregate_derived_internal {
                return Err(unsupported("an aggregate write"));
            }
        }
        Ok(())
    }

    /// Check each CAS expectation and each `must_exist` update against the
    /// stored record, before any group stages. One schema load for each
    /// schema and one batch read for the records.
    pub(super) async fn check_record_preconditions(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let guarded: Vec<&Mutation> = mutations
            .iter()
            .filter(|mutation| mutation.expected.is_some() || Self::is_must_exist_update(mutation))
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
        let atoms = self.db_ops.atoms();
        let mut keys = Vec::with_capacity(guarded.len());
        for &mutation in &guarded {
            let schema = &schemas[mutation.schema_name.as_str()];
            let key = self.resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            keys.push(atoms.record_key(schema, &key, record_scope(schema).as_deref())?);
        }
        let records = atoms.read_records(&keys).await?;
        for (mutation, record) in guarded.into_iter().zip(records) {
            let record = record.unwrap_or_default();
            if Self::is_must_exist_update(mutation) && !record.is_live() {
                return Err(SchemaError::InvalidData(format!(
                    "update target not found: schema '{}', key {} — refusing to silently create a \
                     row from a partial update",
                    mutation.schema_name,
                    mutation.key_value.to_storage_key(),
                )));
            }
            if let Some(expected) = &mutation.expected {
                check_expectation(mutation, expected, record_head(&record, expected)?)?;
            }
        }
        Ok(())
    }
}

/// Merge the fields and the metadata of one mutation into its record. True
/// when something changed; an equal or older stamp changes nothing. The
/// metadata belongs to the record (D7): a write without metadata keeps it.
fn merge_mutation(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    record: &mut StoredRecord,
) -> Result<bool, SchemaError> {
    let stamp = field_stamp(mutation)?;
    let mut changed = false;
    let metadata = mutation.metadata.as_ref().filter(|map| !map.is_empty());
    if mutation.source_file_name.is_some() || metadata.is_some() {
        changed |= record.merge_meta(
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
/// the author-clock writer, as for a tip.
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

/// The CAS head of one field in a record. A removed field is absent. A record
/// has no atom, so a content hash expectation has nothing to compare.
fn record_head(
    record: &StoredRecord,
    expected: &CasExpectation,
) -> Result<CurrentHead, SchemaError> {
    if matches!(expected, CasExpectation::ContentHash { .. }) {
        return Err(unsupported("a content hash precondition"));
    }
    let field = expected.field();
    let value = record.fields.get(field).cloned();
    let mutation_uuid = value
        .as_ref()
        .and(record.stamps.get(field))
        .map(|stamp| stamp.mutation_id.clone());
    Ok(CurrentHead {
        mutation_uuid,
        atom_uuid: None,
        value,
    })
}

fn unsupported(what: &str) -> SchemaError {
    SchemaError::InvalidData(format!("a format 7 home does not store {what} yet"))
}
