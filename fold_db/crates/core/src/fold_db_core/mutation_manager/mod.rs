//! Mutation Manager — public write entry points, commit receipts, and native transaction commits.

mod replace;
mod replay;
mod write_records;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::db_operations::record_store::RecordChange;
use crate::db_operations::{DbOperations, RecordDeleteLedgerEntry};
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::{SchemaCore, SchemaError};
use crate::storage::disk_backup::accepted;
use crate::storage::disk_pack::transaction;
use crate::storage::disk_pack::transaction::phases::{self, Step};

pub use crate::storage::format::metadata::{ReplayStamp, ReplayStreamHead};
pub use replay::ReplayAttachmentSlots;

/// The author clock keeps `writer_id` in base64. Record authorship uses lowercase hex.
fn fill_local_writer(writer_id: &str, mutations: &mut [Mutation]) -> Result<(), SchemaError> {
    if mutations.iter().all(|mutation| mutation.writer.is_some()) {
        return Ok(());
    }
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(writer_id)
        .ok()
        .filter(|bytes| bytes.len() == 32)
        .ok_or_else(|| SchemaError::InvalidData("the node writer id is not a public key".into()))?;
    let writer = lastdb_model::hex::hex_lower(bytes);
    for mutation in mutations.iter_mut() {
        if mutation.writer.is_none() {
            mutation.writer = Some(writer.clone());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    pub mutation_ids: Vec<String>,
    pub record_revisions: BTreeMap<String, String>,
}

pub(super) struct ChangedRecord<'a> {
    pub(super) mutation: &'a Mutation,
    pub(super) key: &'a KeyValue,
}

pub struct MutationManager {
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) schema_manager: Arc<SchemaCore>,
    pub(super) writer_id: String,
}

impl MutationManager {
    pub(crate) fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        writer_id: String,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            writer_id,
        }
    }

    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, SchemaError>> + Send + '_>> {
        Box::pin(async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) })
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
            self.commit_native_batch(mutations, keys).await
        })
        .await
    }

    async fn commit_native_batch(
        &self,
        mut mutations: Vec<Mutation>,
        keys: Vec<KeyValue>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        fill_local_writer(&self.writer_id, &mut mutations)?;
        let runtime = self.db_ops.native_runtime();
        let (mutation_ids, record_revisions) = transaction::scope(Arc::clone(&runtime), async {
            let explicit_ms: Vec<Option<u64>> = mutations
                .iter()
                .map(|m| m.imported_written_at.map(|ns| ns / 1_000_000))
                .collect();
            let groups = self.prepare_write_groups(&mut mutations, &keys);
            let groups = phases::step(Step::OldRecords, groups).await?;
            let captured = accepted::capture_enabled(&runtime);
            let (previous, replay, stamps) = phases::step(Step::Stamps, async {
                let previous = if captured {
                    self.db_ops
                        .metadata()
                        .local_replay_head(&self.writer_id)
                        .await?
                } else {
                    None
                };
                self.prepare_mutation_author_clocks(&mut mutations, previous.as_ref(), captured)
                    .await?;
                let replay = self.local_replay_stamps(&mutations, previous.as_ref(), captured)?;
                let listed: Vec<_> = mutations
                    .iter()
                    .map(|m| {
                        let deleted =
                            matches!(m.mutation_type, MutationType::Delete | MutationType::Purge);
                        (m.schema_name.as_str(), &m.key_value, deleted)
                    })
                    .collect();
                let stamps = self
                    .db_ops
                    .metadata()
                    .stage_local_stamps(&listed, &explicit_ms)
                    .await?;
                Ok::<_, SchemaError>((previous, replay, stamps))
            })
            .await?;
            let revs = mutations
                .iter()
                .zip(&stamps)
                .map(|(m, s)| (m.uuid.clone(), s.revision()))
                .collect();
            // Canonical keys and validated CAS/fields are already in place before signing.
            // The spool entry goes to the cloud log: it is written for the upload of this
            // device's stream and never read back here, so this step reads no stored id.
            phases::step(Step::SpoolEntry, async {
                let signed = self
                    .db_ops
                    .records()
                    .stage_accepted_mutations(&mutations, previous)
                    .await?;
                if let Some(head) = &signed {
                    self.db_ops.metadata().stage_local_replay_head(head).await?;
                }
                Ok::<_, SchemaError>(())
            })
            .await?;
            let ids = mutations.iter().map(|m| m.uuid.clone()).collect();
            let staged = self.stage_prepared_groups(groups, mutations, stamps, replay);
            phases::step(Step::Stage, staged).await?;
            Ok::<_, SchemaError>((ids, revs))
        })
        .await?;
        Ok(ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
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
        let mutation_types: Vec<_> = changed.iter().map(|r| r.mutation.mutation_type).collect();
        self.db_ops
            .records()
            .format
            .stage_records(schema, changes, &mutation_types)
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
