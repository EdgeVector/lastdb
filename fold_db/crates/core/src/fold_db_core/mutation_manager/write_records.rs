//! Preconditions, field validation, and complete record replacements for native commits.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement};
use lastdb_model::record_value::{InlineWrite, StoredRecord};

use super::{AuthoredMutation, ChangedRecord, MutationManager, ResidentCommitStages};
use crate::db_operations::record_store::{RecordChange, RecordStore};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{
    cas::CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Schema,
};
use crate::schema::{SchemaError, SchemaState};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::records::RecordAddress;

impl MutationManager {
    pub(super) async fn stage_record_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
        stages: &mut ResidentCommitStages,
        runtime: &Arc<PackRuntime>,
    ) -> Result<usize, SchemaError> {
        let mut grouped: Vec<(String, Vec<AuthoredMutation>)> = Vec::new();
        let mut seen: HashMap<String, (usize, Schema, HashSet<KeyValue>)> = HashMap::new();
        for authored in mutations {
            let name = &authored.mutation.schema_name;
            if !seen.contains_key(name) {
                let schema = self.schema_manager.require_schema_metadata(name)?;
                seen.insert(name.clone(), (grouped.len(), schema, HashSet::new()));
                grouped.push((name.clone(), Vec::new()));
            }
            let (idx, schema, keys) = seen.get_mut(name).expect("inserted schema group");
            if !keys.insert(Self::resolve_mutation_key_value(
                schema,
                &authored.mutation,
            )?) {
                return Err(SchemaError::InvalidData(
                    "a batch cannot replace the same record twice".into(),
                ));
            }
            grouped[*idx].1.push(authored);
        }
        let group_count = grouped.len();
        let mut elapsed = std::time::Duration::ZERO;
        for (schema_name, group) in grouped {
            let started = std::time::Instant::now();
            self.stage_record_group(&schema_name, group, runtime)
                .await?;
            elapsed += started.elapsed();
        }
        stages.prepare_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        Ok(group_count)
    }

    fn resolve_mutation_key_value(
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        let record_keys = schema.require_record_keys()?;
        mutation
            .target()
            .validate(record_keys)
            .map_err(SchemaError::InvalidData)?;
        let profile = record_keys
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let mut key = mutation.key_value.clone();
        crate::schema::types::key_comparison::canonical_key(profile, &mut key)?;
        Ok(key)
    }

    pub(super) fn validate_mutation_batch(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let (mut identities, mut schemas) = (HashSet::new(), BTreeMap::new());
        for mutation in mutations {
            if !identities.insert(&mutation.uuid) {
                return Err(SchemaError::InvalidData(format!(
                    "mutation batch repeats identity '{}'",
                    mutation.uuid
                )));
            }
            for field in mutation.record_fields.keys() {
                schema_types::DeclarativeSchemaDefinition::validate_field_name(field)
                    .map_err(SchemaError::InvalidData)?;
            }
            let name = mutation.schema_name.as_str();
            if !schemas.contains_key(name) {
                let schema = self.schema_manager.require_schema_metadata(name)?;
                crate::schema::require_logical_layout(&schema.schema_type)?;
                schemas.insert(name, schema);
            }
            Self::resolve_mutation_key_value(&schemas[name], mutation)?;
        }
        Ok(())
    }

    async fn load_schema_for_delta(&self, schema_name: &str) -> Result<Schema, SchemaError> {
        let (mut schema, mut state) = self.schema_manager.get_schema_and_state(schema_name)?;
        if schema.is_none() && state != SchemaState::Blocked {
            schema = self.db_ops.schemas().get_schema(schema_name).await?;
            if schema.is_some() {
                state = self
                    .db_ops
                    .schemas()
                    .get_schema_state(schema_name)
                    .await?
                    .unwrap_or_default();
            }
        }
        if state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
            )));
        }
        let schema = schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        Ok(schema)
    }

    async fn stage_record_group(
        &self,
        schema_name: &str,
        mut mutations: Vec<AuthoredMutation>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        self.schema_manager.require_queryable_schema(schema_name)?;
        let schema = self.load_schema_for_delta(schema_name).await?;
        if let Some(adm) = mutations
            .first()
            .and_then(|m| m.mutation.contribution.as_ref())
        {
            require_contribution_definition(&schema, &adm.definition_id)?;
        }
        let records = self.db_ops.records();
        let (mut keys, mut addresses) = (
            Vec::with_capacity(mutations.len()),
            Vec::with_capacity(mutations.len()),
        );
        for AuthoredMutation { mutation, .. } in &mut mutations {
            let key = Self::resolve_mutation_key_value(&schema, mutation)?;
            mutation.key_value = key.clone();
            for (name, alt_key) in &mut mutation.alternates {
                if let Some(range) = alt_key.as_mut().and_then(|k| k.range.as_mut()) {
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
        let (mut changes, mut changed) = (
            Vec::with_capacity(mutations.len()),
            Vec::with_capacity(mutations.len()),
        );
        for (AuthoredMutation { mutation, stamp }, (key, (address, before))) in mutations
            .iter_mut()
            .zip(keys.iter().zip(addresses.into_iter().zip(stored)))
        {
            check_expectation(mutation, before.as_deref())?;
            let after = replace_record(&schema, mutation, stamp.clone(), before.as_deref())?;
            changes.push(RecordChange {
                key: address,
                before,
                after,
                file_write: mutation.file_write.take(),
                inline_write: mutation.inline_write.take(),
            });
            changed.push(ChangedRecord { mutation, key });
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
        if let Some(batch) = crate::db_operations::search_index::build_index_change_batch(
            schema_name,
            &schema,
            mutations.iter().map(|a| &a.mutation),
            &keys,
        ) {
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                crate::db_operations::search_index::deliver_outbox(inbox, &batch)
                    .map_err(|e| crate::storage::StorageError::BackendError(e.to_string()))
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
    let misses: Vec<_> = mutations
        .iter()
        .zip(&stored)
        .enumerate()
        .filter_map(|(i, (m, r))| {
            (m.mutation.mutation_type == MutationType::Purge && r.is_none()).then_some(i)
        })
        .collect();
    if !misses.is_empty() {
        let addrs: Vec<_> = misses.iter().map(|&i| &addresses[i]).collect();
        for (idx, stamp) in misses
            .into_iter()
            .zip(records.format.applied_deletes(&addrs).await?)
        {
            stored[idx] = stamp.map(|s| Arc::new(StoredRecord::erased(s)));
        }
    }
    Ok(stored)
}

fn require_contribution_definition(schema: &Schema, expected: &str) -> Result<(), SchemaError> {
    let wire = schema.to_wire();
    wire.identity_hash
        .clone()
        .ok_or_else(|| "schema has no catalog identity".to_owned())
        .and_then(|id| schema_types::PortableSchemaId::from_catalog(&id, wire))
        .ok()
        .filter(|id| id.as_str() == expected)
        .map(|_| ())
        .ok_or_else(|| {
            SchemaError::InvalidField(
                "share contribution requires the exact installed schema definition".into(),
            )
        })
}

fn check_expectation(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    let current = record.filter(|r| r.is_live() || mutation.mutation_type == MutationType::Purge);
    let actual = current.map(|r| r.stamp.revision());
    let matches = match &mutation.expected {
        CasExpectation::Absent => !record.is_some_and(StoredRecord::is_live),
        CasExpectation::Revision { revision } => actual.as_ref() == Some(revision),
    };
    if !matches {
        return Err(SchemaError::CasConflict {
            schema: mutation.schema_name.clone(),
            key: mutation.key_value.to_storage_key(),
            expected: match &mutation.expected {
                CasExpectation::Absent => "<absent>".into(),
                CasExpectation::Revision { revision } => revision.clone(),
            },
            actual,
        });
    }
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) && current.is_some_and(|r| r.is_live() && r.alternates != mutation.alternates)
    {
        let msg = "deletion must supply the record's complete current alternate keys".into();
        return Err(if mutation.contribution.is_some() {
            SchemaError::InvalidField(msg)
        } else {
            SchemaError::InvalidData(msg)
        });
    }
    if mutation.erase_written_before_nanos.is_some_and(|cutoff| {
        record
            .and_then(StoredRecord::written_at)
            .is_some_and(|w| w >= cutoff)
    }) {
        return Err(SchemaError::InvalidData(
            "record is newer than the erasure cutoff".into(),
        ));
    }
    if matches!(
        mutation.mutation_type,
        MutationType::Replace | MutationType::Delete
    ) {
        if let (Some(adm), Some(live)) = (&mutation.contribution, record.filter(|r| r.is_live())) {
            let author = live
                .meta
                .as_ref()
                .and_then(|m| m.metadata.get("share_author"))
                .map(String::as_str);
            if author != Some(adm.writer.as_str()) {
                return Err(SchemaError::InvalidField(
                    "share contribution can change only a record its writer created".into(),
                ));
            }
        }
    }
    Ok(())
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
    let mut fields = BTreeMap::new();
    for (field, value) in &mutation.record_fields {
        if !schema.has_field(field) {
            return Err(SchemaError::InvalidData(format!(
                "field '{field}' is not declared in schema '{}'",
                schema.name
            )));
        }
        let field_type = schema.get_field_type(field);
        if let Err(type_err) = field_type.validate(value) {
            return Err(SchemaError::InvalidData(format!(
                "Type error in field '{field}' of schema '{}': {type_err}. Expected {field_type}, got {}",
                schema.name,
                serde_json::to_string(value).unwrap_or_else(|_| "?".into())
            )));
        }
        fields.insert(field.clone(), value.clone());
    }
    let file = mutation.file.clone().or_else(|| {
        (!matches!(&mutation.file_write, Some(FileWrite::Clear)))
            .then(|| before.and_then(|r| r.file.clone()))
            .flatten()
    });
    let inline_file = match &mutation.inline_write {
        Some(InlineWrite::Put(f)) => Some(f.marker.clone()),
        Some(InlineWrite::Clear) => None,
        None => before.and_then(|r| r.inline_file.clone()),
    };
    let metadata =
        (mutation.source_file_name.is_some() || mutation.metadata.is_some()).then(|| {
            RecordMetadata {
                source_file_name: mutation.source_file_name.clone(),
                metadata: mutation
                    .metadata
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            }
        });
    let replacement = RecordReplacement {
        target: mutation.target(),
        expected: mutation.expected.clone(),
        fields,
        metadata,
        file,
        inline_file,
    };
    replacement
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    Ok(StoredRecord::replacement(replacement, stamp, before))
}
