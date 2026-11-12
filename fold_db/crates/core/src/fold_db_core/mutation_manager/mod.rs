//! Mutation Manager — public write entry points and native transaction commits.

mod write_records;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use futures::{future::BoxFuture, FutureExt};
use lastdb_model::portable_mutation::Stamp;

use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{DbOperations, RecordDeleteLedgerEntry};
use crate::schema::mutation_author::sign_mutation_author_clock;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::{SchemaCore, SchemaError, SchemaState};
use crate::storage::disk_pack::transaction;

pub(super) struct AuthoredMutation {
    pub(super) mutation: Mutation,
    pub(super) stamp: Stamp,
}

pub(super) struct ChangedRecord<'a> {
    pub(super) mutation: &'a Mutation,
    pub(super) key: &'a KeyValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidentDurability {
    Queued,
    Durable,
}

impl ResidentDurability {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Durable => "durable",
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    pub mutation_ids: Vec<String>,
    pub record_revisions: BTreeMap<String, String>,
    pub revision: Option<u64>,
    pub operations: ResidentCommitOperations,
    pub durability: ResidentDurability,
    pub stages: ResidentCommitStages,
}

pub struct MutationManager {
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) schema_manager: Arc<SchemaCore>,
    signer: Arc<crate::security::Ed25519KeyPair>,
    pub(super) search_outbox_inbox: Option<PathBuf>,
    author_clock_state: Mutex<crate::schema::types::MutationAuthorClockState>,
}

impl MutationManager {
    pub fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        search_outbox_inbox: Option<PathBuf>,
        author_clock_state: crate::schema::types::MutationAuthorClockState,
    ) -> Self {
        let author_clock_state = Mutex::new(author_clock_state);
        Self {
            db_ops,
            schema_manager,
            signer,
            search_outbox_inbox,
            author_clock_state,
        }
    }

    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> BoxFuture<'_, Result<Vec<String>, SchemaError>> {
        async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) }.boxed()
    }

    pub async fn write_mutations_receipt(
        &self,
        mut mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        Box::pin(async move {
            let now = crate::clock::unix_nanos();
            self.validate_mutation_batch(&mutations)?;
            if mutations.is_empty() {
                return Ok(ResidentCommitReceipt {
                    mutation_ids: Vec::new(),
                    record_revisions: BTreeMap::new(),
                    revision: None,
                    operations: ResidentCommitOperations::default(),
                    durability: ResidentDurability::Queued,
                    stages: ResidentCommitStages::default(),
                });
            }
            self.prepare_mutation_author_clocks(&mut mutations, now)?;
            let revision = mutations.first().map(|m| m.logical_counter);
            let states = self.schema_manager.get_schema_states()?;
            for m in &mutations {
                if states.get(&m.schema_name).copied() == Some(SchemaState::Blocked) {
                    return Err(SchemaError::Blocked(format!(
                        "Schema '{}' is blocked and cannot be mutated (a drop is running)",
                        m.schema_name
                    )));
                }
                m.validate_record_intent()?;
            }
            let mut receipt = self
                .commit_native_batch(mutations, std::time::Instant::now())
                .await?;
            receipt.revision = revision;
            Ok(receipt)
        })
        .await
    }

    fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        now: u64,
    ) -> Result<(), SchemaError> {
        let writer_id = self.signer.public_key_base64();
        let mut guard = self
            .author_clock_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = *guard;
        let (mut times, logical_counter) = state
            .advance_local_batch(now, mutations.len())
            .ok_or_else(|| {
                SchemaError::InvalidData("mutation author-clock range exhausted".into())
            })?;
        for m in mutations {
            m.imported_written_at = Some(times.next().expect("reserved time"));
            m.logical_counter = logical_counter;
            m.author_clock_writer_id.clone_from(&writer_id);
            m.author_clock_signature_version =
                crate::schema::types::MUTATION_AUTHOR_SIGNATURE_VERSION;
            m.author_clock_signature = sign_mutation_author_clock(m, self.signer.as_ref());
        }
        *guard = state;
        Ok(())
    }

    async fn commit_native_batch(
        &self,
        mutations: Vec<Mutation>,
        start: std::time::Instant,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.iter().any(|m| m.imported_written_at.is_some()) {
            let cutoff_nanos = self.db_ops.records().minimum_replay_time().await?;
            if cutoff_nanos > 0
                && mutations
                    .iter()
                    .any(|m| m.imported_written_at.is_some_and(|t| t <= cutoff_nanos))
            {
                return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
            }
        }
        let runtime = self.db_ops.native_runtime();
        let mut stages = ResidentCommitStages::default();
        let (mutation_ids, record_revisions, operations) =
            transaction::request_scope(Arc::clone(&runtime), async {
                runtime.record_read_stage("request-start", mutations.len());
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
                        ));
                    }
                }
                let mut ops = ResidentCommitOperations::default();
                for m in &mutations {
                    match m.mutation_type {
                        MutationType::Create => ops.created += 1,
                        MutationType::Replace => ops.updated += 1,
                        MutationType::Delete | MutationType::Purge => ops.deleted += 1,
                    }
                }
                let schemas: Vec<&str> = mutations.iter().map(|m| m.schema_name.as_str()).collect();
                let stamps = self.db_ops.metadata().stage_local_stamps(&schemas).await?;
                let record_revisions = mutations
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
                    .zip(stamps)
                    .map(|(mutation, stamp)| AuthoredMutation { mutation, stamp })
                    .collect();
                let groups = self
                    .stage_record_groups(authored, &mut stages, &runtime)
                    .await?;
                runtime.record_read_stage("stored-stages-done", groups);
                self.stage_author_clock_state().await?;
                Ok::<_, SchemaError>((ids, record_revisions, ops))
            })
            .await?;
        stages.ack_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        Ok(ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision: None,
            operations,
            durability: ResidentDurability::Durable,
            stages,
        })
    }

    async fn stage_author_clock_state(&self) -> Result<(), SchemaError> {
        let writer = self.signer.public_key_base64();
        let state = *self
            .author_clock_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut durable = self
            .db_ops
            .metadata()
            .author_clock(&writer)
            .await?
            .unwrap_or_default();
        durable.observe_remote(state.physical_nanos, state.logical_counter);
        self.db_ops
            .metadata()
            .stage_author_clock(&writer, &durable)
            .await
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
        let mut audits: BTreeMap<(&str, &str), Vec<(&KeyValue, bool)>> = BTreeMap::new();
        for (rec, &(_, _, had_file)) in changed.iter().zip(&outcomes) {
            let verb = match rec.mutation.mutation_type {
                MutationType::Purge => "purge",
                MutationType::Delete => "delete",
                MutationType::Create | MutationType::Replace => continue,
            };
            audits
                .entry((
                    verb,
                    rec.mutation.erase_caller.unwrap_or("mutation-pipeline"),
                ))
                .or_default()
                .push((rec.key, had_file));
        }
        for ((verb, caller), recs) in audits {
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
            let mut entry = RecordDeleteLedgerEntry::erasure(verb, caller, &schema.name, &desc);
            entry.records_erased = recs.len() as u64;
            entry.file_records_erased = recs.iter().filter(|(_, f)| *f).count() as u64;
            self.db_ops
                .records()
                .format
                .record_delete_audit(entry)
                .await?;
        }
        let schemas = self.db_ops.schemas();
        if matches!(
            schema.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
        ) && schemas
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
