//! Mutation Manager — public write entry points, commit receipts, and native transaction commits.

mod replace;
mod replay;
mod write_records;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;

use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{DbOperations, RecordDeleteLedgerEntry};
use crate::platform::clock;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::{SchemaCore, SchemaError};
use crate::storage::disk_pack::transaction;

pub use crate::storage::format::metadata::ReplayStamp;
pub use write_records::SharedRecordRevision;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitOperations {
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
}

impl ResidentCommitOperations {
    #[must_use]
    pub fn total(&self) -> u64 {
        self.created
            .saturating_add(self.updated)
            .saturating_add(self.deleted)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitStages {
    pub prepare_us: u64,
    pub ack_us: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    pub mutation_ids: Vec<String>,
    pub record_revisions: BTreeMap<String, String>,
    pub revision: Option<u64>,
    pub operations: ResidentCommitOperations,
    pub stages: ResidentCommitStages,
}

pub(super) struct AuthoredMutation {
    pub(super) mutation: Mutation,
    pub(super) key: KeyValue,
    pub(super) stamp: Stamp,
}

pub(super) struct ChangedRecord<'a> {
    pub(super) mutation: &'a Mutation,
    pub(super) key: &'a KeyValue,
}

pub struct MutationManager {
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) schema_manager: Arc<SchemaCore>,
    pub(super) writer_id: String,
    track_shared_record_revisions: bool,
}

impl MutationManager {
    pub(crate) fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        writer_id: String,
        track_shared_record_revisions: bool,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            writer_id,
            track_shared_record_revisions,
        }
    }

    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, SchemaError>> + Send + '_>> {
        Box::pin(async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) })
    }

    /// Commit an authenticated replay and its receipt in the same native write scope.
    pub async fn write_replay_once<T>(
        &self,
        receipt_id: &[u8; 32],
        payload_digest: &[u8; 32],
        replay: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<Option<T>, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(transaction::scope(Arc::clone(&runtime), async {
                if self
                    .db_ops
                    .metadata()
                    .stage_replay_receipt(receipt_id, payload_digest)
                    .await?
                {
                    return Ok(None);
                }
                replay.await.map(Some)
            }))
            .await
    }

    pub async fn write_mutations_receipt(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        Box::pin(async move {
            let keys = self.prepare_mutation_batch(&mutations)?;
            if mutations.is_empty() {
                return Ok(ResidentCommitReceipt::default());
            }
            self.commit_native_batch(mutations, keys, std::time::Instant::now())
                .await
        })
        .await
    }

    async fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
    ) -> Result<u64, SchemaError> {
        let mut state = self
            .db_ops
            .metadata()
            .author_clock(&self.writer_id)
            .await?
            .unwrap_or_default();
        let (mut times, counter) = state
            .advance_local_batch(clock::unix_nanos(), mutations.len())
            .ok_or_else(|| {
                SchemaError::InvalidData("mutation author-clock range exhausted".into())
            })?;
        for m in mutations {
            m.imported_written_at = Some(times.next().expect("reserved time"));
        }
        self.db_ops
            .metadata()
            .stage_author_clock(&self.writer_id, &state)
            .await?;
        Ok(counter)
    }

    async fn commit_native_batch(
        &self,
        mut mutations: Vec<Mutation>,
        keys: Vec<KeyValue>,
        start: std::time::Instant,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        let mut stages = ResidentCommitStages::default();
        let (mutation_ids, record_revisions, operations, revision) =
            transaction::scope(Arc::clone(&runtime), async {
                if let Some(adm) = mutations.iter().find_map(|m| m.contribution.as_ref()) {
                    if mutations.len() != 1 {
                        return Err(SchemaError::InvalidData(
                            "a contribution requires one native mutation".into(),
                        ));
                    }
                    if self
                        .db_ops
                        .metadata()
                        .stage_contribution_receipt(&adm.receipt_id, &adm.intent_digest)
                        .await?
                    {
                        return Ok((
                            vec![mutations[0].uuid.clone()],
                            BTreeMap::new(),
                            ResidentCommitOperations::default(),
                            None,
                        ));
                    }
                }
                let revision = self.prepare_mutation_author_clocks(&mut mutations).await?;
                let ops = count_commit_ops(&mutations);
                let listed: Vec<_> = mutations
                    .iter()
                    .zip(&keys)
                    .map(|(m, key)| {
                        let deleted =
                            matches!(m.mutation_type, MutationType::Delete | MutationType::Purge);
                        (m.schema_name.as_str(), key, deleted)
                    })
                    .collect();
                let stamps = self.db_ops.metadata().stage_local_stamps(&listed).await?;
                let revs = mutations
                    .iter()
                    .zip(&stamps)
                    .map(|(m, s)| (m.uuid.clone(), s.revision()))
                    .collect();
                self.db_ops
                    .records()
                    .stage_accepted_mutations(&mutations)
                    .await?;
                let ids = mutations.iter().map(|m| m.uuid.clone()).collect();
                let authored = mutations
                    .into_iter()
                    .zip(keys.into_iter().zip(stamps))
                    .map(|(mutation, (key, stamp))| AuthoredMutation {
                        mutation,
                        key,
                        stamp,
                    })
                    .collect();
                self.stage_record_groups(authored, &mut stages, &runtime)
                    .await?;
                Ok::<_, SchemaError>((ids, revs, ops, Some(revision)))
            })
            .await?;
        stages.ack_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        Ok(ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision,
            operations,
            stages,
        })
    }

    pub(super) async fn stage_records_with_effects(
        &self,
        schema: &Schema,
        changed: &[ChangedRecord<'_>],
        changes: Vec<RecordChange>,
    ) -> Result<(), SchemaError> {
        let outcomes: Vec<_> = changes
            .iter()
            .map(|c| {
                (
                    c.after.is_live(),
                    c.after.written_at(),
                    c.before.as_ref().is_some_and(|b| b.file.is_some()),
                )
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
            .filter_map(|(r, &(live, _, _))| live.then_some(r.key));
        self.db_ops.schemas().note_live_writes(schema, live)?;
        for entry in collect_erasure_audits(&schema.name, changed, &outcomes) {
            self.db_ops
                .records()
                .format
                .record_delete_audit(entry)
                .await?;
        }
        let schemas = self.db_ops.schemas();
        if schema.schema_type == DeclarativeSchemaType::Hash
            && schemas
                .get_schema_retention_policy(&schema.name)
                .await?
                .is_some()
        {
            let (mut ages, mut removed) = (Vec::new(), Vec::new());
            for (rec, &(live, written_at, _)) in changed.iter().zip(&outcomes) {
                if let Some(nanos) = written_at.filter(|_| live) {
                    ages.push((rec.key.clone(), nanos / 1_000_000_000));
                } else {
                    removed.push(rec.key.clone());
                }
            }
            schemas
                .record_schema_retention_write_ages(&schema.name, &ages)
                .await?;
            schemas
                .remove_schema_retention_keys(&schema.name, &removed)
                .await?;
        }
        Ok(())
    }
}

fn count_commit_ops(mutations: &[Mutation]) -> ResidentCommitOperations {
    let mut ops = ResidentCommitOperations::default();
    for m in mutations {
        match m.mutation_type {
            MutationType::Create => ops.created += 1,
            MutationType::Replace => ops.updated += 1,
            MutationType::Delete | MutationType::Purge => ops.deleted += 1,
        }
    }
    ops
}

fn collect_erasure_audits(
    schema_name: &str,
    changed: &[ChangedRecord<'_>],
    outcomes: &[(bool, Option<u64>, bool)],
) -> Vec<RecordDeleteLedgerEntry> {
    let mut audits: BTreeMap<(&str, &str), Vec<(&KeyValue, bool)>> = BTreeMap::new();
    for (rec, &(_, _, had_file)) in changed.iter().zip(outcomes) {
        let verb = match rec.mutation.mutation_type {
            MutationType::Purge => "purge",
            MutationType::Delete => "delete",
            MutationType::Create | MutationType::Replace => continue,
        };
        let caller = rec.mutation.erase_caller.unwrap_or("mutation-pipeline");
        audits
            .entry((verb, caller))
            .or_default()
            .push((rec.key, had_file));
    }
    audits
        .into_iter()
        .map(|((verb, caller), recs)| {
            let desc = if let [(kv, _)] = recs.as_slice() {
                match (&kv.hash, &kv.range) {
                    (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
                    (Some(h), None) => format!("hash='{h}'"),
                    (None, Some(r)) => format!("range='{r}'"),
                    (None, None) => "<single>".into(),
                }
            } else {
                format!("<bulk:{} keys>", recs.len())
            };
            let mut entry = RecordDeleteLedgerEntry::erasure(verb, caller, schema_name, &desc);
            entry.records_erased = recs.len() as u64;
            entry.file_records_erased = recs.iter().filter(|(_, f)| *f).count() as u64;
            entry
        })
        .collect()
}
