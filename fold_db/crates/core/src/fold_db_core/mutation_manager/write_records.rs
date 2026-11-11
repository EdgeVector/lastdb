//! Complete record replacements and their nested effects share one native commit.

use std::collections::BTreeMap;
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement};
use lastdb_model::record_value::{InlineWrite, StoredRecord};

use super::validation::{check_expectation, require_contribution_author, validate_field_value};
use super::write_effects::ChangedRecord;
use super::{AuthoredMutation, MutationManager};
use crate::db_operations::record_store::{RecordChange, RecordStore};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FileWrite, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::records::RecordAddress;

impl MutationManager {
    /// One batch read supplies both preconditions and the old nested addresses.
    /// Each key appears once in the request; staged effects publish only if all succeed.
    pub(super) async fn stage_record_group(
        &self,
        schema_name: &str,
        mut mutations: Vec<AuthoredMutation>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        self.schema_manager.require_queryable_schema(schema_name)?;
        let schema = self.load_schema_for_delta(schema_name).await?;
        if let Some(admission) = mutations
            .first()
            .and_then(|item| item.mutation.contribution.as_ref())
        {
            require_contribution_definition(&schema, &admission.definition_id)?;
        }
        let records = self.db_ops.records();
        let mut keys = Vec::with_capacity(mutations.len());
        let mut addresses = Vec::with_capacity(mutations.len());
        for AuthoredMutation { mutation, .. } in &mut mutations {
            let key = self.resolve_mutation_key_value(schema_name, &schema, mutation)?;
            mutation.key_value = key.clone();
            for (name, key) in &mut mutation.alternates {
                if let Some(range) = key.as_mut().and_then(|key| key.range.as_mut()) {
                    let profile = schema.require_record_keys()?.alternates[name]
                        .range_profile()
                        .map_err(SchemaError::InvalidData)?;
                    *range = crate::schema::types::key_comparison::canonical_range(profile, range)?;
                }
            }
            addresses.push(records.record_address(&schema, &key)?);
            keys.push(key);
        }
        let stored = read_write_preimages(records, &addresses, &mutations).await?;
        let mut changes = Vec::with_capacity(mutations.len());
        let mut changed = Vec::with_capacity(mutations.len());
        for (authored, (key, (address, before))) in mutations
            .iter_mut()
            .zip(keys.iter().zip(addresses.into_iter().zip(stored)))
        {
            changes.push(prepare_record(&schema, authored, address, before)?);
            changed.push(ChangedRecord {
                mutation: &authored.mutation,
                key,
            });
        }
        self.stage_records_with_effects(&schema, &changed, changes)
            .await?;
        runtime.record_read_stage("record-stores-done", keys.len());
        if schema.schema_type == DeclarativeSchemaType::HashRange {
            self.db_ops
                .schemas()
                .record_schema_retention_hash_partitions(schema_name, &keys)
                .await?;
        }
        if let Some(batch) = Self::build_index_change_batch(
            schema_name,
            &schema,
            mutations.iter().map(|authored| &authored.mutation),
            &keys,
        ) {
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                Self::deliver_search_outbox(inbox, &batch)
            })?;
        }
        Ok(())
    }
}

async fn read_write_preimages(
    records: &RecordStore,
    addresses: &[RecordAddress],
    mutations: &[AuthoredMutation],
) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
    let mut stored = records.read_records(addresses).await?;
    let purge_misses: Vec<_> = mutations
        .iter()
        .zip(&stored)
        .enumerate()
        .filter(|(_, (mutation, record))| {
            mutation.mutation.mutation_type == MutationType::Purge && record.is_none()
        })
        .map(|(index, _)| index)
        .collect();
    if !purge_misses.is_empty() {
        let addresses: Vec<_> = purge_misses
            .iter()
            .map(|&index| &addresses[index])
            .collect();
        let deletes = records.read_applied_deletes(&addresses).await?;
        for (index, stamp) in purge_misses.into_iter().zip(deletes) {
            stored[index] = stamp.map(|stamp| Arc::new(StoredRecord::erased(stamp)));
        }
    }
    Ok(stored)
}

fn require_contribution_definition(schema: &Schema, expected: &str) -> Result<(), SchemaError> {
    let wire = schema.to_wire();
    let id = wire.identity_hash.clone();
    let definition_id = id
        .ok_or_else(|| "schema has no catalog identity".to_owned())
        .and_then(|id| schema_types::PortableSchemaId::from_catalog(&id, wire));
    if definition_id.is_ok_and(|id| id.as_str() == expected) {
        return Ok(());
    }
    Err(SchemaError::InvalidField(
        "share contribution requires the exact installed schema definition".into(),
    ))
}

fn prepare_record(
    schema: &Schema,
    authored: &mut AuthoredMutation,
    address: RecordAddress,
    before: Option<Arc<StoredRecord>>,
) -> Result<RecordChange, SchemaError> {
    let AuthoredMutation { mutation, stamp } = authored;
    check_expectation(mutation, before.as_deref())?;
    require_contribution_author(mutation, before.as_deref())?;
    let after = replace_record(schema, mutation, stamp.clone(), before.as_deref())?;
    Ok(RecordChange {
        key: address,
        before,
        after,
        file_write: mutation.file_write.take(),
        inline_write: mutation.inline_write.take(),
    })
}

fn replace_record(
    schema: &Schema,
    mutation: &Mutation,
    stamp: Stamp,
    before: Option<&StoredRecord>,
) -> Result<StoredRecord, SchemaError> {
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        return Ok(StoredRecord::erased(stamp));
    }
    let fields: BTreeMap<String, serde_json::Value> = mutation
        .record_fields
        .iter()
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect();
    for (field, value) in &fields {
        validate_field_value(&schema.name, schema, field, value)?;
    }
    let file = mutation.file.clone().or_else(|| {
        if matches!(&mutation.file_write, Some(FileWrite::Clear)) {
            None
        } else {
            before.and_then(|record| record.file.clone())
        }
    });
    let inline_file = match &mutation.inline_write {
        Some(InlineWrite::Put(file)) => Some(file.marker.clone()),
        Some(InlineWrite::Clear) => None,
        None => before.and_then(|record| record.inline_file.clone()),
    };
    let replacement = RecordReplacement {
        target: mutation.target(),
        expected: mutation.expected.clone(),
        fields,
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
        file,
        inline_file,
    };
    replacement
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    Ok(StoredRecord::replacement(replacement, stamp, before))
}
