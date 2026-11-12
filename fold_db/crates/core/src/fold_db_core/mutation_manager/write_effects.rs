//! Per-schema group staging, delete audit, retention age rows, and erase/replay guards.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use lastdb_model::record_value::StoredRecord;

use super::{AuthoredMutation, MutationManager, ResidentCommitStages};
use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE};
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;

/// One record that a mutation of the group changed.
pub(super) struct ChangedRecord<'a> {
    pub(super) mutation: &'a Mutation,
    pub(super) key: &'a KeyValue,
}

/// The facts of one changed record after its replacement or erasure.
struct RecordOutcome {
    live: bool,
    written_at: Option<u64>,
    had_file: bool,
}

impl MutationManager {
    /// Stage every group's conditional replacements before publication.
    pub(super) async fn stage_record_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
        stages: &mut ResidentCommitStages,
        runtime: &Arc<PackRuntime>,
    ) -> Result<usize, SchemaError> {
        let grouped_mutations = self.independent_key_groups(mutations)?;
        let group_count = grouped_mutations.len();
        let mut prepare_elapsed = std::time::Duration::ZERO;
        for (schema_name, schema_mutations) in grouped_mutations {
            let prepare_started = std::time::Instant::now();
            self.stage_record_group(&schema_name, schema_mutations, runtime)
                .await?;
            prepare_elapsed += prepare_started.elapsed();
        }
        stages.prepare_us = u64::try_from(prepare_elapsed.as_micros()).unwrap_or(u64::MAX);
        Ok(group_count)
    }

    /// Group mutations by schema in first-seen order and reject duplicate primary keys.
    fn independent_key_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
    ) -> Result<Vec<(String, Vec<AuthoredMutation>)>, SchemaError> {
        let mut out: Vec<(String, Vec<AuthoredMutation>)> = Vec::new();
        let mut seen: HashMap<String, (usize, Schema, HashSet<KeyValue>)> = HashMap::new();
        for authored in mutations {
            let name = &authored.mutation.schema_name;
            if !seen.contains_key(name) {
                let schema = self.schema_manager.require_schema_metadata(name)?;
                seen.insert(name.clone(), (out.len(), schema, HashSet::new()));
                out.push((name.clone(), Vec::new()));
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
            out[*idx].1.push(authored);
        }
        Ok(out)
    }

    /// The keys of `keys` that no live record of `schema_name` holds.
    pub async fn absent_erase_targets(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<Vec<KeyValue>, SchemaError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let record_store = self.db_ops.records();
        let record_keys = keys
            .iter()
            .map(|key| record_store.record_key(&schema, key))
            .collect::<Result<Vec<_>, _>>()?;
        let records = record_store.read_records(&record_keys).await?;
        Ok(keys
            .iter()
            .zip(records)
            .filter(|(_, record)| !record.as_deref().is_some_and(StoredRecord::is_live))
            .map(|(key, _)| key.clone())
            .collect())
    }

    /// Every stamped write time is checked against the home-wide durable replay floor.
    pub(crate) async fn require_replay_cutoff(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if !mutations.iter().any(|m| m.imported_written_at.is_some()) {
            return Ok(());
        }
        let cutoff_nanos = self.db_ops.records().minimum_replay_time().await?;
        if cutoff_nanos > 0
            && mutations
                .iter()
                .any(|m| m.imported_written_at.is_some_and(|t| t <= cutoff_nanos))
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }

    /// Stage the records of one group, then their delete audit and retention age rows.
    pub(super) async fn stage_records_with_effects(
        &self,
        schema: &Schema,
        changed: &[ChangedRecord<'_>],
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        let outcomes: Vec<_> = changes
            .iter()
            .map(|change| RecordOutcome {
                live: change.after.is_live(),
                written_at: change.after.written_at(),
                had_file: change.before.as_ref().is_some_and(|b| b.file.is_some()),
            })
            .collect();
        self.db_ops
            .records()
            .format
            .stage_records(schema, changes)
            .await?;
        let live = changed
            .iter()
            .zip(&outcomes)
            .filter_map(|(r, o)| o.live.then_some(r.key));
        self.db_ops.schemas().note_live_writes(schema, live)?;
        self.stage_record_delete_audit(&schema.name, changed, &outcomes)
            .await?;
        self.stage_record_retention(schema, changed.iter().map(|r| r.key), &outcomes)
            .await
    }

    async fn stage_record_retention<'a>(
        &self,
        schema: &Schema,
        keys: impl Iterator<Item = &'a KeyValue>,
        outcomes: &[RecordOutcome],
    ) -> Result<(), SchemaError> {
        if !matches!(
            schema.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
        ) {
            return Ok(());
        }
        let schemas = self.db_ops.schemas();
        if schemas
            .get_schema_retention_policy(&schema.name)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let mut ages = Vec::new();
        let mut removed = Vec::new();
        for (key, outcome) in keys.zip(outcomes) {
            match outcome.written_at.filter(|_| outcome.live) {
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

    async fn stage_record_delete_audit(
        &self,
        schema_name: &str,
        changed: &[ChangedRecord<'_>],
        outcomes: &[RecordOutcome],
    ) -> Result<(), SchemaError> {
        let mut groups: BTreeMap<(&str, &str), Vec<(&KeyValue, &RecordOutcome)>> = BTreeMap::new();
        for (record, outcome) in changed.iter().zip(outcomes) {
            let verb = match record.mutation.mutation_type {
                MutationType::Purge => LEDGER_VERB_PURGE,
                MutationType::Delete => LEDGER_VERB_DELETE,
                MutationType::Create | MutationType::Replace => continue,
            };
            let caller = record.mutation.erase_caller.unwrap_or("mutation-pipeline");
            groups
                .entry((verb, caller))
                .or_default()
                .push((record.key, outcome));
        }
        let record_store = self.db_ops.records();
        for ((verb, caller), records) in groups {
            let descriptor = match records.as_slice() {
                [(kv, _)] => match (&kv.hash, &kv.range) {
                    (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
                    (Some(h), None) => format!("hash='{h}'"),
                    (None, Some(r)) => format!("range='{r}'"),
                    (None, None) => "<single>".into(),
                },
                _ => format!("<bulk:{} keys>", records.len()),
            };
            let mut entry =
                RecordDeleteLedgerEntry::erasure(verb, caller, schema_name, &descriptor);
            entry.records_erased = records.len() as u64;
            entry.file_records_erased = records.iter().filter(|(_, o)| o.had_file).count() as u64;
            record_store.format.record_delete_audit(entry).await?;
        }
        Ok(())
    }
}
