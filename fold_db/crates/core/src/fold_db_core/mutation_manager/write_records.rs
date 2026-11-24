//! Batch resolution, pre-signing canonicalization/validation, and per-schema record staging.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lastdb_model::portable_mutation::Stamp;
use lastdb_model::record_replacement::RecordReplacement;
use lastdb_model::record_value::StoredRecord;

use super::replace::{
    build_replacement, check_expectation, finish_record, read_write_preimages,
    require_contribution_definition,
};
use super::{ChangedRecord, MutationManager, ReplayStamp};
use crate::db_operations::record_store::RecordChange;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType, Schema};
use crate::schema::{SchemaError, SchemaState};
use crate::storage::format::records::RecordAddress;

pub(super) struct PreparedRecord {
    pub(super) index: usize,
    pub(super) address: RecordAddress,
    pub(super) before: Option<Arc<StoredRecord>>,
    pub(super) replacement: Option<(RecordReplacement, bool)>,
}

pub(super) struct PreparedGroup {
    pub(super) schema: Schema,
    pub(super) records: Vec<PreparedRecord>,
}

impl MutationManager {
    pub(super) fn prepare_mutation_batch(
        &self,
        mutations: &[Mutation],
    ) -> Result<Vec<KeyValue>, SchemaError> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        let mut identities = HashSet::with_capacity(mutations.len());
        let mut schemas: HashMap<&str, (Schema, SchemaState)> = HashMap::new();
        let mut keys = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            if !identities.insert(mutation.uuid.as_str()) {
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
                let Some((schema, state)) = self.schema_manager.schema_and_state(name)? else {
                    if self.schema_manager.is_schema_blocked(name)? {
                        return Err(blocked_mutation_err(name));
                    }
                    return Err(SchemaError::InvalidData(format!(
                        "Schema '{name}' not found"
                    )));
                };
                crate::schema::require_logical_layout(&schema.schema_type)?;
                schemas.insert(name, (schema, state));
            }
            let (schema, _) = &schemas[name];
            keys.push(Self::resolve_mutation_key_value(schema, mutation)?);
        }
        for mutation in mutations {
            if schemas[mutation.schema_name.as_str()].1 == SchemaState::Blocked {
                return Err(blocked_mutation_err(&mutation.schema_name));
            }
            mutation.validate_record_intent()?;
        }
        Ok(keys)
    }

    pub(super) fn canonical_primary_key(
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<KeyValue, SchemaError> {
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let mut key = key.clone();
        crate::schema::types::key_comparison::canonical_key(profile, &mut key)?;
        Ok(key)
    }

    pub(super) fn canonicalize_alternates(
        schema: &Schema,
        alternates: &mut BTreeMap<String, Option<KeyValue>>,
    ) -> Result<(), SchemaError> {
        let record_keys = schema.require_record_keys()?;
        for (name, alt_key) in alternates {
            let def = record_keys.alternates.get(name).ok_or_else(|| {
                SchemaError::InvalidData(format!("undeclared alternate key '{name}'"))
            })?;
            if let Some(range) = alt_key.as_mut().and_then(|k| k.range.as_mut()) {
                let profile = def.range_profile().map_err(SchemaError::InvalidData)?;
                *range = crate::schema::types::key_comparison::canonical_range(profile, range)?;
            }
        }
        Ok(())
    }

    fn resolve_mutation_key_value(
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        let record_keys = schema.require_record_keys()?;
        mutation
            .target()
            .validate(record_keys)
            .map_err(SchemaError::InvalidData)?;
        Self::canonical_primary_key(schema, &mutation.key_value)
    }

    pub(super) async fn prepare_write_groups(
        &self,
        mutations: &mut [Mutation],
        keys: &[KeyValue],
    ) -> Result<(Vec<PreparedGroup>, Duration), SchemaError> {
        let mut grouped: Vec<(String, Vec<usize>)> = Vec::new();
        let mut seen: HashMap<String, (usize, HashSet<KeyValue>)> = HashMap::new();
        for (idx, (mutation, key)) in mutations.iter().zip(keys).enumerate() {
            let name = &mutation.schema_name;
            if !seen.contains_key(name) {
                seen.insert(name.clone(), (grouped.len(), HashSet::new()));
                grouped.push((name.clone(), Vec::new()));
            }
            let (group_idx, seen_keys) = seen.get_mut(name).expect("inserted schema group");
            if !seen_keys.insert(key.clone()) {
                return Err(SchemaError::InvalidData(
                    "a batch cannot replace the same record twice".into(),
                ));
            }
            grouped[*group_idx].1.push(idx);
        }
        let mut out = Vec::with_capacity(grouped.len());
        let mut elapsed = Duration::ZERO;
        for (schema_name, indices) in grouped {
            let started = Instant::now();
            out.push(
                self.prepare_write_group(&schema_name, &indices, mutations, keys)
                    .await?,
            );
            elapsed += started.elapsed();
        }
        Ok((out, elapsed))
    }

    async fn prepare_write_group(
        &self,
        schema_name: &str,
        indices: &[usize],
        mutations: &mut [Mutation],
        keys: &[KeyValue],
    ) -> Result<PreparedGroup, SchemaError> {
        let schema = self.load_schema_for_delta(schema_name).await?;
        if let Some(adm) = indices
            .first()
            .and_then(|&i| mutations[i].contribution.as_ref())
        {
            require_contribution_definition(&schema, &adm.definition_id)?;
        }
        let records = self.db_ops.records();
        let mut addrs = Vec::with_capacity(indices.len());
        let mut purges = Vec::with_capacity(indices.len());
        for &idx in indices {
            let mutation = &mut mutations[idx];
            mutation.key_value = keys[idx].clone();
            Self::canonicalize_alternates(&schema, &mut mutation.alternates)?;
            addrs.push(records.record_address(&schema, &mutation.key_value)?);
            purges.push(mutation.mutation_type == MutationType::Purge);
        }
        let stored = read_write_preimages(records, &addrs, &purges).await?;
        let mut prepared = Vec::with_capacity(indices.len());
        for (&index, (address, before)) in indices.iter().zip(addrs.into_iter().zip(stored)) {
            check_expectation(&mutations[index], before.as_deref())?;
            let replacement = build_replacement(&schema, &mutations[index], before.as_deref())?;
            prepared.push(PreparedRecord {
                index,
                address,
                before,
                replacement,
            });
        }
        Ok(PreparedGroup {
            schema,
            records: prepared,
        })
    }

    pub(super) async fn stage_prepared_groups(
        &self,
        groups: Vec<PreparedGroup>,
        mutations: Vec<Mutation>,
        stamps: Vec<Stamp>,
        replay: Vec<Option<ReplayStamp>>,
    ) -> Result<Duration, SchemaError> {
        let mut items: Vec<Option<(Mutation, Stamp, Option<ReplayStamp>)>> = mutations
            .into_iter()
            .zip(stamps.into_iter().zip(replay))
            .map(|(m, (s, r))| Some((m, s, r)))
            .collect();
        let mut elapsed = Duration::ZERO;
        for group in groups {
            let started = Instant::now();
            self.stage_prepared_group(group, &mut items).await?;
            elapsed += started.elapsed();
        }
        Ok(elapsed)
    }

    async fn stage_prepared_group(
        &self,
        group: PreparedGroup,
        items: &mut [Option<(Mutation, Stamp, Option<ReplayStamp>)>],
    ) -> Result<(), SchemaError> {
        let count = group.records.len();
        let mut addrs = Vec::with_capacity(count);
        let mut local_replays = Vec::with_capacity(count);
        let mut group_mutations = Vec::with_capacity(count);
        let mut changes = Vec::with_capacity(count);
        let mut keys = Vec::with_capacity(count);
        for rec in group.records {
            let (mut mutation, stamp, replay) =
                items[rec.index].take().expect("unique prepared index");
            let after = finish_record(rec.replacement, &mutation, stamp, rec.before.as_deref());
            let file_write = mutation.file_write.take();
            let inline_write = mutation.inline_write.take();
            // Preserve slot flags before taking file_write/inline_write out of mutation.
            let slots = super::replay::slots_from(
                mutation.mutation_type,
                file_write.is_some(),
                inline_write.is_some(),
            );
            addrs.push(rec.address.clone());
            local_replays.push((replay, slots));
            keys.push(mutation.key_value.clone());
            changes.push(RecordChange {
                key: rec.address,
                before: rec.before,
                after,
                file_write,
                inline_write,
            });
            group_mutations.push(mutation);
        }
        self.stamp_local_records(&group.schema, &addrs, &local_replays)
            .await?;
        let changed: Vec<ChangedRecord<'_>> = group_mutations
            .iter()
            .zip(&keys)
            .map(|(mutation, key)| ChangedRecord { mutation, key })
            .collect();
        self.stage_records_with_effects(&group.schema, &changed, changes)
            .await?;
        if group.schema.schema_type == DeclarativeSchemaType::HashRange {
            self.db_ops
                .schemas()
                .record_schema_retention_hash_partitions(&group.schema.name, &keys)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
    ) -> Result<Schema, SchemaError> {
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
            return Err(blocked_mutation_err(schema_name));
        }
        let schema = schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        Ok(schema)
    }
}

fn blocked_mutation_err(schema_name: &str) -> SchemaError {
    SchemaError::Blocked(format!(
        "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
    ))
}
