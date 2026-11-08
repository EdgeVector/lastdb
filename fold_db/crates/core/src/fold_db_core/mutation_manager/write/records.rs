//! Complete record replacements and their nested effects share one native commit.
use super::super::cas_rules::check_expectation;
use super::super::key_values::validate_field_value;
use super::super::portable_journal::schema_fingerprint;
use super::record_effects::ChangedRecord;
use super::*;
use crate::db_operations::record_store::{RecordChange, RecordStore};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FileWrite, MutationType, Schema};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::metadata::DefinitionSet;
use crate::storage::format::records::{PortableEffect, RecordAddress};
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement};
use lastdb_model::record_value::{InlineWrite, StoredRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
        require_current_binding(&schema, &mutations)?;
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
                    *range = crate::schema::types::key_comparison::normalize::canonical_range(
                        profile, range,
                    )?;
                }
            }
            addresses.push(records.record_address(&schema, &key)?);
            keys.push(key);
        }
        let declared = self.declared_fields_for(schema_name, &mutations).await?;
        let stored = read_write_preimages(records, &addresses, &mutations).await?;
        let mut changes = Vec::with_capacity(mutations.len());
        let mut changed = Vec::with_capacity(mutations.len());
        for ((authored, declared_fields), (key, (address, before))) in mutations
            .iter_mut()
            .zip(&declared)
            .zip(keys.iter().zip(addresses.into_iter().zip(stored)))
        {
            changes.push(prepare_record(
                &schema,
                authored,
                address,
                before,
                declared_fields.as_ref(),
            )?);
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
                Self::deliver_search_outbox(inbox, batch).await
            })?;
        }
        Ok(())
    }

    /// Field names of each mutation's named definition. `None` when the write
    /// names no definition, or the schema is local. A shared write that names
    /// an unknown or incompatible definition is refused before the record changes.
    async fn declared_fields_for(
        &self,
        schema_name: &str,
        mutations: &[AuthoredMutation],
    ) -> Result<Vec<Option<BTreeSet<String>>>, SchemaError> {
        let named = mutations
            .iter()
            .any(|item| item.shared_schema.is_some() && item.mutation.definition_id.is_some());
        if !named {
            return Ok(vec![None; mutations.len()]);
        }
        let sets = self.db_ops.metadata().portable_definition_sets().await?;
        let set = sets.get(schema_name);
        mutations
            .iter()
            .map(
                |item| match (&item.shared_schema, item.mutation.definition_id.as_deref()) {
                    (Some(_), Some(id)) => Ok(Some(declared_fields(schema_name, set, id)?)),
                    _ => Ok(None),
                },
            )
            .collect()
    }
}

fn declared_fields(
    schema_name: &str,
    set: Option<&DefinitionSet>,
    raw_id: &str,
) -> Result<BTreeSet<String>, SchemaError> {
    let unknown = || {
        SchemaError::InvalidData(format!(
            "schema '{schema_name}' names an unknown definition '{raw_id}'"
        ))
    };
    let id = raw_id
        .parse::<schema_types::PortableSchemaId>()
        .map_err(|_| unknown())?;
    let Some(set) = set else {
        return Err(unknown());
    };
    let Some(named) = set.accepted.get(&id) else {
        return Err(unknown());
    };
    let current = set.current.verify(set.current.id()).map_err(|error| {
        SchemaError::InvalidData(format!(
            "schema '{schema_name}' definition '{raw_id}' cannot be verified: {error}"
        ))
    })?;
    let named_definition = named.verify(&id).map_err(|error| {
        SchemaError::InvalidData(format!(
            "schema '{schema_name}' definition '{raw_id}' cannot be verified: {error}"
        ))
    })?;
    match current.extends(named_definition) {
        Ok(true) => Ok(named_definition.fields.iter().flatten().cloned().collect()),
        Ok(false) => Err(SchemaError::InvalidData(format!(
            "schema '{schema_name}' names an incompatible definition '{raw_id}'"
        ))),
        Err(error) => Err(SchemaError::InvalidData(format!(
            "schema '{schema_name}' names an incompatible definition '{raw_id}': {error}"
        ))),
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
            (mutation.shared_schema.is_some()
                || mutation.mutation.mutation_type == MutationType::Purge)
                && record.is_none()
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

fn require_current_binding(
    schema: &Schema,
    mutations: &[AuthoredMutation],
) -> Result<(), SchemaError> {
    if let Some(binding) = mutations
        .first()
        .and_then(|item| item.shared_schema.as_ref())
    {
        if schema_fingerprint(schema)? != binding.local_sha256 {
            return Err(SchemaError::InvalidData(
                "shared schema differs from its admitted revision".into(),
            ));
        }
    }
    Ok(())
}

fn prepare_record(
    schema: &Schema,
    authored: &mut AuthoredMutation,
    address: RecordAddress,
    before: Option<Arc<StoredRecord>>,
    declared_fields: Option<&BTreeSet<String>>,
) -> Result<RecordChange, SchemaError> {
    let AuthoredMutation {
        mutation,
        stamp,
        shared_schema,
    } = authored;
    let portable = if let Some(binding) = shared_schema {
        if mutation
            .erase_caller
            .is_some_and(|caller| caller.starts_with("ttl-sweep-"))
        {
            return Err(SchemaError::InvalidData(
                "shared expiry policy is deferred".into(),
            ));
        }
        if before.as_ref().is_some_and(|record| record.stamp >= *stamp) {
            return Err(SchemaError::InvalidData(
                "local write would lose to a newer shared stamp".into(),
            ));
        }
        PortableEffect::SharedAuthor(binding.id.clone())
    } else {
        PortableEffect::Local
    };
    check_expectation(mutation, before.as_deref())?;
    let mut after = replace_record(
        schema,
        mutation,
        stamp.clone(),
        before.as_deref(),
        declared_fields,
    )?;
    // A live shared record keeps the definition its log entry names.
    if let Some(binding) = shared_schema.as_ref().filter(|_| after.is_live()) {
        after.definition_id = Some(
            binding
                .id
                .parse::<schema_types::PortableSchemaId>()
                .map_err(SchemaError::InvalidData)?,
        );
    }
    Ok(RecordChange {
        key: address,
        before,
        after,
        file_write: mutation.file_write.take(),
        inline_write: mutation.inline_write.take(),
        portable,
    })
}

fn replace_record(
    schema: &Schema,
    mutation: &Mutation,
    stamp: Stamp,
    before: Option<&StoredRecord>,
    declared_fields: Option<&BTreeSet<String>>,
) -> Result<StoredRecord, SchemaError> {
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        return Ok(StoredRecord::erased(stamp));
    }
    let mut fields: BTreeMap<String, serde_json::Value> = mutation
        .record_fields
        .iter()
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect();
    // A shared replace that names its definition keeps stored fields that
    // definition does not declare. A field it does declare, and the write
    // omits, is still cleared. No named definition means today's replace.
    if mutation.mutation_type == MutationType::Replace {
        if let (Some(declared), Some(before)) =
            (declared_fields, before.filter(|record| record.is_live()))
        {
            for (field, value) in &before.fields {
                if !declared.contains(field) {
                    fields.insert(field.clone(), value.clone());
                }
            }
        }
    }
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
    // Like the blob marker, a write keeps the current inline file unless it
    // carries an inline file operation. The record keeps only the marker.
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
