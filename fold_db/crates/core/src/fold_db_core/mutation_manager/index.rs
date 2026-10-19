//! Search app outbox batches for committed mutations.

use std::collections::HashMap;

use crate::db_operations::search_index::{
    IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink, SearchOutboxSink,
};
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};

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
