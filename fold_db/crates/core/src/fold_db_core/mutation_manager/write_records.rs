//! Batch resolution, per-schema record staging, and shared record revisions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lastdb_model::STANDARD_COMMENT_SCHEMA;

use super::replace::{
    check_expectation, read_write_preimages, replace_record, require_contribution_definition,
};
use super::{AuthoredMutation, ChangedRecord, MutationManager, ResidentCommitStages};
use crate::db_operations::record_store::RecordChange;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType, Schema};
use crate::schema::{SchemaError, SchemaState};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::metadata::SharedCommentWrite;

/// The same record version expressed in local and publisher terms.
pub struct SharedRecordRevision {
    /// Conditional-write token in this store.
    pub local_revision: String,
    /// Mutation ID carried in the publisher's accepted record.
    pub mutation_id: String,
    /// Verified original receiver that created a standard shared comment.
    pub comment_creator: Option<String>,
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

    pub(super) async fn stage_record_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
        stages: &mut ResidentCommitStages,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        let mut grouped: Vec<(String, Vec<AuthoredMutation>)> = Vec::new();
        let mut seen: HashMap<String, (usize, HashSet<KeyValue>)> = HashMap::new();
        for authored in mutations {
            let name = &authored.mutation.schema_name;
            if !seen.contains_key(name) {
                seen.insert(name.clone(), (grouped.len(), HashSet::new()));
                grouped.push((name.clone(), Vec::new()));
            }
            let (idx, keys) = seen.get_mut(name).expect("inserted schema group");
            if !keys.insert(authored.key.clone()) {
                return Err(SchemaError::InvalidData(
                    "a batch cannot replace the same record twice".into(),
                ));
            }
            grouped[*idx].1.push(authored);
        }
        let mut elapsed = std::time::Duration::ZERO;
        for (schema_name, group) in grouped {
            let started = std::time::Instant::now();
            self.stage_record_group(&schema_name, group, runtime)
                .await?;
            elapsed += started.elapsed();
        }
        stages.prepare_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
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
            return Err(blocked_mutation_err(schema_name));
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
        let schema = self.load_schema_for_delta(schema_name).await?;
        if let Some(adm) = mutations
            .first()
            .and_then(|m| m.mutation.contribution.as_ref())
        {
            require_contribution_definition(&schema, &adm.definition_id)?;
        }
        let records = self.db_ops.records();
        let record_keys = schema.require_record_keys()?;
        let (mut keys, mut addrs) = (
            Vec::with_capacity(mutations.len()),
            Vec::with_capacity(mutations.len()),
        );
        for AuthoredMutation { mutation, key, .. } in &mut mutations {
            mutation.key_value = key.clone();
            for (name, alt_key) in &mut mutation.alternates {
                if let Some(range) = alt_key.as_mut().and_then(|k| k.range.as_mut()) {
                    let profile = record_keys.alternates[name]
                        .range_profile()
                        .map_err(SchemaError::InvalidData)?;
                    *range = crate::schema::types::key_comparison::canonical_range(profile, range)?;
                }
            }
            addrs.push(records.record_address(&schema, key)?);
            keys.push(key.clone());
        }
        let stored = read_write_preimages(records, &addrs, &mutations).await?;
        self.stage_local_replay_records(runtime, schema_name, &addrs, &mutations, &stored)
            .await?;
        let (mut changes, mut changed) = (
            Vec::with_capacity(mutations.len()),
            Vec::with_capacity(mutations.len()),
        );
        for (
            AuthoredMutation {
                mutation,
                key,
                stamp,
            },
            (addr, before),
        ) in mutations.iter_mut().zip(addrs.into_iter().zip(stored))
        {
            check_expectation(mutation, before.as_deref())?;
            let after = replace_record(&schema, mutation, stamp.clone(), before.as_deref())?;
            changes.push(RecordChange {
                key: addr,
                before,
                after,
                file_write: mutation.file_write.take(),
                inline_write: mutation.inline_write.take(),
            });
            changed.push(ChangedRecord { mutation, key });
        }
        if self.track_shared_record_revisions {
            self.stage_shared_revisions(&schema.name, &changes, &changed)
                .await?;
        }
        self.stage_records_with_effects(&schema, &changed, changes)
            .await?;
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

    /// Look up the durable receipt before re-evaluating a proposal on inbox retry.
    pub async fn shared_contribution_committed(&self, id: &[u8; 32]) -> Result<bool, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().contribution_committed(id))
            .await
    }

    /// Read the native record and its publisher mutation ID from one committed view.
    pub async fn shared_record_revision(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<SharedRecordRevision>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let preimage = self.replay_preimage(address.clone()).await?;
                let Some(record) = preimage.filter(|row| row.is_live()) else {
                    return Ok(None);
                };
                let local_revision = record.stamp.revision();
                let meta = self.db_ops.metadata();
                let Some(mutation_id) = meta
                    .shared_record_mutation(&address, &local_revision)
                    .await?
                else {
                    return Ok(None);
                };
                let comment_creator = if schema == STANDARD_COMMENT_SCHEMA {
                    meta.shared_comment_creator(&address, &local_revision)
                        .await?
                } else {
                    None
                };
                Ok(Some(SharedRecordRevision {
                    local_revision,
                    mutation_id,
                    comment_creator,
                }))
            })
            .await
    }

    async fn stage_shared_revisions(
        &self,
        schema: &str,
        records: &[RecordChange],
        changes: &[ChangedRecord<'_>],
    ) -> Result<(), SchemaError> {
        let meta = self.db_ops.metadata();
        meta.stage_shared_record_mutations(records.iter().zip(changes).map(|(row, change)| {
            (
                row.key.as_str(),
                row.after
                    .is_live()
                    .then_some((&row.after.stamp, change.mutation.uuid.as_str())),
            )
        }))
        .await?;
        if schema == STANDARD_COMMENT_SCHEMA {
            meta.stage_shared_comment_creators(records.iter().zip(changes).map(|(row, change)| {
                let mutation = &change.mutation;
                let creator = mutation.contribution.as_ref().filter(|admission| {
                    mutation.mutation_type == MutationType::Create
                        && admission.schema_scope
                        && admission.creates_record
                });
                SharedCommentWrite {
                    address: row.key.as_str(),
                    before: row
                        .before
                        .as_ref()
                        .filter(|r| r.is_live())
                        .map(|r| &r.stamp),
                    after: row.after.is_live().then_some(&row.after.stamp),
                    creator: creator.map(|admission| admission.writer.as_str()),
                }
            }))
            .await?;
        }
        Ok(())
    }
}

fn blocked_mutation_err(schema_name: &str) -> SchemaError {
    SchemaError::Blocked(format!(
        "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
    ))
}
