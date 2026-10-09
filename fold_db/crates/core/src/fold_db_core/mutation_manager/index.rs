//! Search app outbox batches for committed mutations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::db_operations::search_index::{
    IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink, SearchOutboxSink,
};
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use tracing::{warn, Instrument};

use super::MutationManager;

use super::helpers::searchable_outbox_fields;

impl MutationManager {
    /// Build Search tombstones for exact storage slots removed by the legacy
    /// owner drain. The lane retains this exact batch across retries.
    /// Storage-form keys stay typed; opaque BlindV1/OpeV1 segments never enter
    /// logs or mutation ids.
    pub(in crate::fold_db_core::mutation_manager) fn build_storage_slot_tombstones(
        schema_name: &str,
        schema: &Schema,
        evidence: &[super::super::purge::StorageSlotPurgeEvidence],
    ) -> Option<IndexChangeBatch> {
        if evidence.is_empty() {
            return None;
        }
        Some(IndexChangeBatch {
            schema_name: schema_name.to_string(),
            searchable_fields: searchable_outbox_fields(schema),
            changes: evidence
                .iter()
                .map(|slot| IndexChange {
                    mutation_id: format!("legacy-tombstone-drain-{}", uuid::Uuid::new_v4()),
                    kind: IndexChangeKind::Tombstone,
                    key_value: slot.storage_key_value().clone(),
                    fields_and_values: HashMap::new(),
                })
                .collect(),
        })
    }

    /// Spawn best-effort Search-app inbox delivery for a mutation batch as a
    /// tracked background task, off the write's critical path.
    ///
    /// Mini no longer hosts an in-process native embedding index. Live writes
    /// only emit typed [`IndexChangeBatch`] JSON for the first-party Search
    /// app. The task is registered with the pending-task tracker so
    /// `wait_for_background_tasks` (shutdown drain + read-your-writes in tests)
    /// can still wait for it to settle.
    pub(super) fn spawn_index_mutations(
        &self,
        schema_name: &str,
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
    ) {
        let Some(batch) = Self::build_index_change_batch(
            schema_name,
            schema,
            schema_mutations,
            mutation_key_values,
        ) else {
            return;
        };
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let user_id = crate::user_context::get_current_user_id();
        let search_outbox_inbox = self.search_outbox_inbox.clone();

        let pending_task = pending_tasks.begin();
        tokio::spawn(
            async move {
                let _pending_task = pending_task;
                let work = async {
                    let result = if let Some(inbox) = search_outbox_inbox {
                        SearchOutboxSink::new(inbox).apply_change_batch(batch).await
                    } else if let Some(sink) = SearchOutboxSink::from_env() {
                        sink.apply_change_batch(batch).await
                    } else {
                        Ok(())
                    };
                    if let Err(error) = result {
                        warn!("Search index update failed: {error}");
                    }
                };
                match user_id {
                    Some(uid) => crate::user_context::run_with_user(&uid, work).await,
                    None => work.await,
                }
            }
            .in_current_span(),
        );
    }

    /// Build one Search change batch without durable side effects.
    pub(super) fn build_index_change_batch(
        schema_name: &str,
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
    ) -> Option<IndexChangeBatch> {
        let mut changes = Vec::with_capacity(schema_mutations.len());
        for (idx, mutation) in schema_mutations.iter().enumerate() {
            let key_value = &mutation_key_values[idx];
            match mutation.mutation_type {
                MutationType::Create | MutationType::Update => {
                    changes.push(IndexChange {
                        mutation_id: mutation.uuid.clone(),
                        kind: IndexChangeKind::Upsert,
                        key_value: key_value.clone(),
                        fields_and_values: mutation.fields_and_values.clone(),
                    });
                }
                // Purge is hard-erase: Search must drop vectors the same way
                // as Delete. Skipping Purge left stale embeddings forever once
                // delete routes through purge (Search only reacts to
                // IndexChangeKind::Tombstone).
                MutationType::Delete | MutationType::Purge => {
                    changes.push(IndexChange {
                        mutation_id: mutation.uuid.clone(),
                        kind: IndexChangeKind::Tombstone,
                        key_value: key_value.clone(),
                        fields_and_values: HashMap::default(),
                    });
                }
            }
        }
        if changes.is_empty() {
            return None;
        }
        Some(IndexChangeBatch {
            schema_name: schema_name.to_string(),
            searchable_fields: searchable_outbox_fields(schema),
            changes,
        })
    }

    /// Deliver a prepared Search batch. A configured sink error keeps the
    /// schema-lane envelope at its FIFO head.
    pub(crate) async fn deliver_index_change_batch(
        &self,
        batch: &IndexChangeBatch,
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(inbox) = &self.search_outbox_inbox {
            return SearchOutboxSink::new(inbox.clone())
                .apply_change_batch(batch.clone())
                .await;
        }
        if let Some(sink) = SearchOutboxSink::from_env() {
            return sink.apply_change_batch(batch.clone()).await;
        }
        Ok(())
    }
}
