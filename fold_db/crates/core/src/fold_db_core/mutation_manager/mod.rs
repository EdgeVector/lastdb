//! Mutation Manager — public write entry points and native transaction commits.

mod replay;
mod write_records;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use futures::{future::BoxFuture, FutureExt};
use lastdb_model::portable_mutation::Stamp;

use crate::crypto::{clock, security::Ed25519KeyPair};
use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{DbOperations, RecordDeleteLedgerEntry};
use crate::schema::mutation_author::sign_mutation_author_clock;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationAuthorClockState, Schema};
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

pub use crate::resident::{
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
};
pub use crate::storage::format::metadata::ReplayStamp;

pub struct MutationManager {
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) schema_manager: Arc<SchemaCore>,
    signer: Arc<Ed25519KeyPair>,
    pub(super) search_outbox_inbox: Option<PathBuf>,
    author_clock_state: Mutex<MutationAuthorClockState>,
}

impl MutationManager {
    pub fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<Ed25519KeyPair>,
        search_outbox_inbox: Option<PathBuf>,
        author_clock_state: MutationAuthorClockState,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            signer,
            search_outbox_inbox,
            author_clock_state: Mutex::new(author_clock_state),
        }
    }

    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> BoxFuture<'_, Result<Vec<String>, SchemaError>> {
        async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) }.boxed()
    }

    /// Commit an authenticated replay and its receipt in the same native write scope.
    ///
    /// The caller binds the receipt ID to the entry's stable identity and the digest
    /// to its contents. An identical receipt returns `None` without polling `replay`.
    /// A failed replay commits neither its staged writes nor its receipt.
    pub async fn write_replay_once<T>(
        &self,
        receipt_id: &[u8; 32],
        payload_digest: &[u8; 32],
        replay: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<Option<T>, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(transaction::request_scope(Arc::clone(&runtime), async {
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
            self.validate_mutation_batch(&mutations)?;
            if mutations.is_empty() {
                return Ok(ResidentCommitReceipt::empty_queued());
            }
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
            self.commit_native_batch(mutations, std::time::Instant::now())
                .await
        })
        .await
    }

    async fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
    ) -> Result<u64, SchemaError> {
        let writer_id = self.signer.public_key_base64();
        let mut state = self
            .db_ops
            .metadata()
            .author_clock(&writer_id)
            .await?
            .unwrap_or_default();
        let cached = *self
            .author_clock_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.observe_remote(cached.physical_nanos, cached.logical_counter);
        let (mut times, logical_counter) = state
            .advance_local_batch(clock::unix_nanos(), mutations.len())
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
        self.db_ops
            .metadata()
            .stage_author_clock(&writer_id, &state)
            .await?;
        Ok(logical_counter)
    }

    async fn commit_native_batch(
        &self,
        mut mutations: Vec<Mutation>,
        start: std::time::Instant,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        let mut stages = ResidentCommitStages::default();
        let (mutation_ids, record_revisions, operations, revision) =
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
                            None,
                        ));
                    }
                }
                let revision = self.prepare_mutation_author_clocks(&mut mutations).await?;
                self.require_after_replay_cutoff(&mutations).await?;
                let ops = count_commit_ops(&mutations);
                let schemas: Vec<&str> = mutations.iter().map(|m| m.schema_name.as_str()).collect();
                let stamps = self.db_ops.metadata().stage_local_stamps(&schemas).await?;
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
                    .zip(stamps)
                    .map(|(mutation, stamp)| AuthoredMutation { mutation, stamp })
                    .collect();
                let groups = self
                    .stage_record_groups(authored, &mut stages, &runtime)
                    .await?;
                runtime.record_read_stage("stored-stages-done", groups);
                Ok::<_, SchemaError>((ids, revs, ops, Some(revision)))
            })
            .await?;
        stages.ack_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        Ok(ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision,
            operations,
            durability: ResidentDurability::Durable,
            stages,
        })
    }

    async fn require_after_replay_cutoff(&self, mutations: &[Mutation]) -> Result<(), SchemaError> {
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
