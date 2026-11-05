//! Search app outbox batches for committed mutations.

use std::collections::HashMap;

use crate::db_operations::search_index::inbox_fields::{inbox_fields, searchable_fields};
use crate::db_operations::search_index::{
    IndexChange, IndexChangeBatch, IndexChangeKind, SearchOutboxSink,
};
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};

use super::MutationManager;

impl MutationManager {
    /// Build one Search change batch without durable side effects. An upsert
    /// carries only the searchable fields, with file keys removed.
    pub(super) fn build_index_change_batch<'a>(
        schema_name: &str,
        schema: &Schema,
        schema_mutations: impl ExactSizeIterator<Item = &'a Mutation>,
        mutation_key_values: &[KeyValue],
    ) -> Option<IndexChangeBatch> {
        if !crate::db_operations::search_index::queue_policy::enabled() {
            return None;
        }
        let searchable = searchable_fields(schema);
        let mut changes = Vec::with_capacity(schema_mutations.len());
        for (mutation, key_value) in schema_mutations.zip(mutation_key_values) {
            match mutation.mutation_type {
                MutationType::Create | MutationType::Replace => {
                    changes.push(IndexChange {
                        mutation_id: mutation.uuid.clone(),
                        kind: IndexChangeKind::Upsert,
                        key_value: key_value.clone(),
                        record_fields: inbox_fields(&mutation.record_fields, searchable.as_ref()),
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
                        record_fields: HashMap::default(),
                    });
                }
            }
        }
        if changes.is_empty() {
            return None;
        }
        Some(IndexChangeBatch {
            format_version: 1,
            schema_name: schema_name.to_string(),
            searchable_fields: searchable,
            changes,
        })
    }

    /// Send one batch after a native commit. A sink error becomes a storage error.
    pub(crate) async fn deliver_search_outbox(
        inbox: Option<std::path::PathBuf>,
        batch: IndexChangeBatch,
    ) -> Result<(), crate::storage::StorageError> {
        SearchOutboxSink::deliver(inbox, batch)
            .await
            .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))
    }
}
