//! Per-schema group staging, delete audit, retention age rows, and erase/replay guards.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use lastdb_model::record_value::StoredRecord;

use super::{AuthoredMutation, MutationManager, ResidentCommitStages};
use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE};
use crate::fold_db_core::purge::describe_key;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;

type KeyGroups = Vec<(String, Vec<AuthoredMutation>)>;

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

/// The outcome of each change, in the order of the changes.
fn record_outcomes(changes: &[RecordChange]) -> Vec<RecordOutcome> {
    changes
        .iter()
        .map(|change| RecordOutcome {
            live: change.after.is_live(),
            written_at: change.after.written_at(),
            had_file: change
                .before
                .as_ref()
                .is_some_and(|before| before.file.is_some()),
        })
        .collect()
}

fn is_erasure(mutation: &Mutation) -> bool {
    matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    )
}

impl MutationManager {
    /// Stage every group's conditional replacements before publication.
    pub(super) async fn stage_record_groups(
        &self,
        mutations: Vec<AuthoredMutation>,
        stages: &mut ResidentCommitStages,
        runtime: &Arc<PackRuntime>,
    ) -> Result<usize, SchemaError> {
        let grouped_mutations =
            self.independent_key_groups(Self::group_mutations_by_schema(mutations))?;

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

    /// Independent records prepare together. A repeated primary key is rejected:
    /// its replacement would require the revision of an unacknowledged write.
    fn independent_key_groups(&self, groups: KeyGroups) -> Result<KeyGroups, SchemaError> {
        let mut result = Vec::new();
        for (name, mutations) in groups {
            let schema = self.schema_manager.require_schema_metadata(&name)?;
            let mut current_keys = HashSet::new();
            let mut current = Vec::new();
            for mutation in mutations {
                let key = self.resolve_mutation_key_value(&name, &schema, &mutation.mutation)?;
                if current_keys.contains(&key) {
                    return Err(SchemaError::InvalidData(
                        "a batch cannot replace the same record twice".into(),
                    ));
                }
                current_keys.insert(key);
                current.push(mutation);
            }
            if !current.is_empty() {
                result.push((name, current));
            }
        }
        Ok(result)
    }

    /// Groups mutations by schema name in first-seen schema order.
    fn group_mutations_by_schema(mutations: Vec<AuthoredMutation>) -> KeyGroups {
        let mut order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, Vec<AuthoredMutation>> = HashMap::new();

        for mutation in mutations {
            let schema = &mutation.mutation.schema_name;
            if !grouped.contains_key(schema) {
                order.push(schema.clone());
            }
            grouped.entry(schema.clone()).or_default().push(mutation);
        }

        order
            .into_iter()
            .map(|name| {
                let mutations = grouped
                    .remove(&name)
                    .expect("schema order came from grouped keys");
                (name, mutations)
            })
            .collect()
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
            && mutations.iter().any(|mutation| {
                mutation
                    .imported_written_at
                    .is_some_and(|time| time <= cutoff_nanos)
            })
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }

    /// Stage the records of one group, then their delete audit and retention
    /// age rows, using the outcomes captured before staging.
    pub(super) async fn stage_records_with_effects(
        &self,
        schema: &Schema,
        changed: &[ChangedRecord<'_>],
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        let outcomes = record_outcomes(&changes);
        self.db_ops.records().stage_records(schema, changes).await?;
        // A live record below a floor of the TTL sweep removes the floor.
        let live = changed
            .iter()
            .zip(&outcomes)
            .filter(|(_, outcome)| outcome.live);
        self.db_ops
            .schemas()
            .note_live_writes(schema, live.map(|(record, _)| record.key))?;
        self.stage_record_delete_audit(&schema.name, changed, &outcomes)
            .await?;
        self.stage_record_retention(schema, changed.iter().map(|record| record.key), &outcomes)
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

    /// One audit row for each verb and caller of the deletes in the group. A
    /// delete that removed nothing changed no record and is not here.
    async fn stage_record_delete_audit(
        &self,
        schema_name: &str,
        changed: &[ChangedRecord<'_>],
        outcomes: &[RecordOutcome],
    ) -> Result<(), SchemaError> {
        let mut groups: BTreeMap<(&str, &str), Vec<(&KeyValue, &RecordOutcome)>> = BTreeMap::new();
        for (record, outcome) in changed.iter().zip(outcomes) {
            if !is_erasure(record.mutation) {
                continue;
            }
            let verb = if record.mutation.mutation_type == MutationType::Purge {
                LEDGER_VERB_PURGE
            } else {
                LEDGER_VERB_DELETE
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
                [(key, _)] => describe_key(key),
                _ => format!("<bulk:{} keys>", records.len()),
            };
            let mut entry =
                RecordDeleteLedgerEntry::erasure(verb, caller, schema_name, &descriptor);
            entry.records_erased = records.len() as u64;
            entry.file_records_erased = records
                .iter()
                .filter(|(_, outcome)| outcome.had_file)
                .count() as u64;
            record_store.record_record_delete_audit(entry).await?;
        }
        Ok(())
    }
}
