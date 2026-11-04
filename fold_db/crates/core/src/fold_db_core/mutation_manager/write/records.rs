//! Complete record replacements and their nested effects share one native commit.
use super::super::cas_rules::check_expectation;
use super::super::key_values::validate_field_value;
use super::record_effects::ChangedRecord;
use super::record_rules::take_file_writes;
use super::*;
use crate::db_operations::record_store::RecordChange;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, MutationType, Schema};
use crate::storage::disk_pack::{transaction, PackRuntime};
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement, RetryReceipt};
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

impl MutationManager {
    /// One batch read supplies both preconditions and the old nested addresses.
    /// Each key appears once in the request; staged effects publish only if all succeed.
    pub(super) async fn stage_record_group(
        &self,
        schema_name: &str,
        mut mutations: Vec<Mutation>,
        idempotency_entries: Vec<(String, RetryReceipt)>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        self.schema_manager.require_queryable_schema(schema_name)?;
        let schema = self.load_schema_for_delta(schema_name).await?;
        let records = self.db_ops.records();
        let mut keys = Vec::with_capacity(mutations.len());
        let mut addresses = Vec::with_capacity(mutations.len());
        for mutation in &mut mutations {
            let key = self.resolve_mutation_key_value(schema_name, &schema, mutation)?;
            mutation.key_value = key.clone();
            for (name, key) in &mut mutation.alternates {
                if let Some(range) = key.as_mut().and_then(|key| key.range.as_mut()) {
                    let profile = schema.require_record_keys()?.alternates[name]
                        .range_profile()
                        .map_err(SchemaError::InvalidData)?;
                    *range = crate::schema::types::key_comparison::normalize::canonical_range(
                        profile, range,
                    )?;
                }
            }
            addresses.push(records.record_address(&schema, &key)?);
            keys.push(key);
        }
        let stored = records.read_records(&addresses).await?;
        let file_writes = take_file_writes(&mut mutations);
        let mut changes = Vec::with_capacity(mutations.len());
        let mut changed = Vec::with_capacity(mutations.len());
        let mut before_addresses = Vec::with_capacity(mutations.len());
        let mut after_addresses = Vec::with_capacity(mutations.len());
        for (((mutation, key), (address, before)), file_write) in mutations
            .iter()
            .zip(&keys)
            .zip(addresses.into_iter().zip(stored))
            .zip(file_writes)
        {
            check_expectation(mutation, before.as_deref())?;
            let after = replace_record(&schema, mutation, before.as_deref())?;
            before_addresses.push(
                before
                    .as_ref()
                    .filter(|record| record.is_live())
                    .map(|record| record.alternates.clone())
                    .unwrap_or_default(),
            );
            after_addresses.push(if after.is_live() {
                after.alternates.clone()
            } else {
                Default::default()
            });
            changed.push(ChangedRecord { mutation, key });
            changes.push(RecordChange {
                key: address,
                before,
                after,
                file_write,
            });
        }
        let entries =
            records.alternate_entry_changes(&schema, &keys, &before_addresses, &after_addresses)?;
        self.stage_records_with_effects(&schema, &changed, changes, entries)
            .await?;
        runtime.record_read_stage("record-stores-done", keys.len());
        if !idempotency_entries.is_empty() {
            self.db_ops
                .metadata()
                .batch_put_idempotency(idempotency_entries)
                .await?;
        }
        if schema.schema_type == DeclarativeSchemaType::HashRange {
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
}

fn replace_record(
    schema: &Schema,
    mutation: &Mutation,
    before: Option<&StoredRecord>,
) -> Result<StoredRecord, SchemaError> {
    let stamp = mutation.record_stamp().map_err(SchemaError::InvalidData)?;
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        let mut after = before
            .cloned()
            .ok_or_else(|| SchemaError::InvalidData("deletion record is absent".into()))?;
        after.delete(stamp.clone());
        // A purge of a tombstone still gets a new revision for its history release.
        after.stamp = stamp;
        return Ok(after);
    }
    for (field, value) in &mutation.record_fields {
        validate_field_value(&schema.name, schema, field, value)?;
    }
    let replacement = RecordReplacement {
        target: mutation.target(),
        expected: mutation.expected.clone(),
        fields: mutation.record_fields.clone().into_iter().collect(),
        metadata: if mutation.source_file_name.is_some() || mutation.metadata.is_some() {
            Some(RecordMetadata {
                source_file_name: mutation.source_file_name.clone(),
                metadata: mutation
                    .metadata
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            })
        } else {
            None
        },
        file: mutation.file.clone(),
    };
    replacement
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    Ok(StoredRecord::replacement(replacement, stamp))
}
