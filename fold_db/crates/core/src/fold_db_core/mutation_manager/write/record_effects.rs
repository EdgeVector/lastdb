//! The side rows of one format 7 record group: field history, the delete
//! audit and the retention ages. Each joins the batch of the records.
//!
//! - Field history (D5): a copy of each overwritten or deleted field value
//!   whose field has a history policy (`field_history/records.rs`). A purge
//!   captures nothing and releases the history of its record.
//! - Delete audit: one `dellog:` row for each verb and caller of the group, as
//!   the erase lane writes for a field key home. A TTL sweep delete keeps its
//!   caller tag.
//! - Conflict rows: a purge deletes the conflict rows of its record, their
//!   queue entries and the marker (`record_conflicts/purge.rs`). A delete
//!   keeps them until the end of their window.
//! - Retention: a Hash or Single schema with a policy ages each live record by
//!   its write time (the newest stamp) and drops the age rows of a deleted
//!   record. The age rows of the field key path come from the tip clocks after
//!   the commit; a record holds its own stamps, so the rows go in this batch.
//! - Share copies: a personal write to a schema with an active share rule
//!   puts each changed record in the scope of each rule too
//!   (`share:{recipient}:{opaque}`). The copy is the whole record after the
//!   merge, delete floor included, so a delete or a purge reaches the copy in
//!   the same batch. A copy has no alternate entries, field history or audit
//!   rows: this node reads no share scope by an alternate key, and the
//!   personal record keeps the history.

use super::*;
use crate::db_operations::record_store::{RecordChange, RecordHistoryRows};
use crate::db_operations::{RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE};
use crate::fold_db_core::purge::describe_key;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::operations::MutationType;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::KvMutation;
use std::collections::BTreeMap;

/// One record that a mutation of the group changed.
pub(super) struct ChangedRecord<'a> {
    pub(super) mutation: &'a Mutation,
    pub(super) key: &'a KeyValue,
}

/// The facts of one changed record after its merge.
struct RecordOutcome {
    live: bool,
    written_at: Option<u64>,
    named_blobs: bool,
}

/// The outcome of each change, in the order of the changes.
fn record_outcomes(changes: &[RecordChange]) -> Vec<RecordOutcome> {
    changes
        .iter()
        .map(|change| RecordOutcome {
            live: change.after.is_live(),
            written_at: change.after.written_at(),
            named_blobs: change
                .before
                .as_ref()
                .is_some_and(|before| !before.blob_refs().is_empty()),
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
    /// Stage the records of one group with their field history rows, then
    /// their delete audit and retention age rows. The history and the facts
    /// of the audit and the ages come from the records before the batch takes
    /// them.
    pub(super) async fn stage_records_with_effects(
        &self,
        schema: &Schema,
        (storage_prefix, scope): (Option<&str>, Option<&str>),
        changed: &[ChangedRecord<'_>],
        changes: Vec<RecordChange>,
        mut entries: Vec<KvMutation>,
    ) -> Result<(), SchemaError> {
        let history = self
            .record_history_rows(schema, scope, changed, &changes)
            .await?;
        let outcomes = record_outcomes(&changes);
        let copies = self
            .record_share_copies(&schema.name, storage_prefix, &changes)
            .await?;
        entries.extend(history.rows);
        entries.extend(self.purged_conflict_rows(changed, &changes).await?);
        let mut changes = changes;
        changes.extend(copies);
        self.db_ops
            .records()
            .stage_records(changes, entries, history.blobs)
            .await?;
        if storage_prefix.is_none() {
            // A live record below a floor of the TTL sweep removes the floor.
            let live = changed
                .iter()
                .zip(&outcomes)
                .filter(|(_, outcome)| outcome.live);
            self.db_ops
                .schemas()
                .note_live_writes(schema, live.map(|(record, _)| record.key))?;
        }
        self.stage_record_side_rows(schema, storage_prefix, changed, &outcomes, history.released)
            .await
    }

    /// The copy of each changed personal record in the scope of each active
    /// share rule of the schema. One batch read gives the stored copies, so
    /// the file blob counters change by the net delta.
    async fn record_share_copies(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
        changes: &[RecordChange],
    ) -> Result<Vec<RecordChange>, SchemaError> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        let prefixes = self
            .active_share_prefixes(schema_name, storage_prefix)
            .await?;
        let keys: Vec<String> = prefixes
            .iter()
            .flat_map(|prefix| {
                changes
                    .iter()
                    .map(move |change| build_storage_key(Some(prefix.as_str()), &change.key))
            })
            .collect();
        let stored = self.db_ops.records().read_records(&keys).await?;
        Ok(keys
            .into_iter()
            .zip(stored)
            .zip(changes.iter().cycle())
            .map(|((key, before), change)| RecordChange {
                key,
                before,
                after: change.after.clone(),
                cap_deferred: change.cap_deferred,
            })
            .collect())
    }

    /// The field history rows of the changed records of one group.
    async fn record_history_rows(
        &self,
        schema: &Schema,
        scope: Option<&str>,
        changed: &[ChangedRecord<'_>],
        changes: &[RecordChange],
    ) -> Result<RecordHistoryRows, SchemaError> {
        let purges: Vec<bool> = changed
            .iter()
            .map(|record| record.mutation.mutation_type == MutationType::Purge)
            .collect();
        self.db_ops
            .records()
            .record_history_rows(schema, scope, changes, &purges)
            .await
    }

    /// The deletes of the conflict rows of each purged record of one group.
    async fn purged_conflict_rows(
        &self,
        changed: &[ChangedRecord<'_>],
        changes: &[RecordChange],
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let purged: Vec<&str> = changed
            .iter()
            .zip(changes)
            .filter(|(record, _)| record.mutation.mutation_type == MutationType::Purge)
            .map(|(_, change)| change.key.as_str())
            .collect();
        self.db_ops.records().purged_record_conflicts(&purged).await
    }

    /// Stage the delete audit rows and the retention age rows of one group in
    /// the request's batch, after its records.
    async fn stage_record_side_rows(
        &self,
        schema: &Schema,
        storage_prefix: Option<&str>,
        changed: &[ChangedRecord<'_>],
        outcomes: &[RecordOutcome],
        history_released: u64,
    ) -> Result<(), SchemaError> {
        self.stage_record_delete_audit(&schema.name, changed, outcomes, history_released)
            .await?;
        if storage_prefix.is_some()
            || !matches!(
                schema.schema_type,
                DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
            )
        {
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
        for (record, outcome) in changed.iter().zip(outcomes) {
            match outcome.written_at.filter(|_| outcome.live) {
                Some(nanos) => ages.push((record.key.clone(), nanos / 1_000_000_000)),
                None => removed.push(record.key.clone()),
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
        history_released: u64,
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
            entry.records_converged = records.len() as u64;
            entry.file_pointer_records_purged = records
                .iter()
                .filter(|(_, outcome)| outcome.named_blobs)
                .count() as u64;
            if verb == LEDGER_VERB_PURGE {
                entry.history_rows_deleted = history_released;
            }
            record_store.record_record_delete_audit(None, entry).await?;
        }
        Ok(())
    }
}
