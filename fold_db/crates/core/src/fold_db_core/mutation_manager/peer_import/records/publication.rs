//! App notifications describe the merged records that the batch stores.

use super::super::*;
use super::merge::PeerRecord;
use crate::db_operations::search_index::inbox_fields::{inbox_fields, searchable_fields};
use crate::db_operations::search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};
use crate::db_operations::ChangeFeedEvent;
use crate::schema::types::Schema;
use std::collections::HashMap;

impl MutationManager {
    pub(super) async fn stage_peer_record_notifications(
        &self,
        changed: &[PeerRecord],
        schemas: &BTreeMap<String, Schema>,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        if changed.is_empty() {
            return Ok(());
        }
        let committed_at_ms = crate::clock::unix_millis();
        let events = changed
            .iter()
            .map(|record| ChangeFeedEvent {
                seq: 0,
                mutation_id: record.mutation.clone().unwrap_or_default(),
                schema: record.schema.clone(),
                operation: if record.after.is_live() {
                    "update"
                } else {
                    "delete"
                }
                .to_string(),
                hash: record.key.hash.clone(),
                range: record.key.range.clone(),
                committed_at_ms,
                background_tasks_drained: false,
                convergence_pending: true,
            })
            .collect();
        // Subscribers receive these after the complete commit of the batch.
        self.db_ops.change_feed().append_many(events).await?;
        if !crate::db_operations::search_index::queue_policy::enabled() {
            return Ok(());
        }
        for schema in schemas.values() {
            let Some(batch) = search_batch(schema, changed) else {
                continue;
            };
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                Self::deliver_search_outbox(inbox, batch).await
            })?;
        }
        Ok(())
    }
}

/// An upsert of the stored fields of each live record, and a tombstone for
/// each deleted one.
fn search_batch(schema: &Schema, changed: &[PeerRecord]) -> Option<IndexChangeBatch> {
    let searchable = searchable_fields(schema);
    let changes: Vec<IndexChange> = changed
        .iter()
        .filter(|record| record.schema == schema.name)
        .map(|record| {
            let live = record.after.is_live();
            let fields: HashMap<_, _> = record
                .after
                .fields
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            IndexChange {
                mutation_id: record.mutation.clone().unwrap_or_default(),
                kind: if live {
                    IndexChangeKind::Upsert
                } else {
                    IndexChangeKind::Tombstone
                },
                key_value: record.key.clone(),
                fields_and_values: if live {
                    inbox_fields(&fields, searchable.as_ref())
                } else {
                    HashMap::default()
                },
            }
        })
        .collect();
    (!changes.is_empty()).then(|| IndexChangeBatch {
        format_version: 1,
        schema_name: schema.name.clone(),
        searchable_fields: searchable,
        changes,
    })
}
