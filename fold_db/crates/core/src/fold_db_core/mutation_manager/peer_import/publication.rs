//! App notifications describe actual stored winners, never rejected proposals.

use super::*;
use crate::db_operations::search_index::inbox_fields::{inbox_fields, searchable_fields};
use crate::db_operations::search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};
use crate::db_operations::ChangeFeedEvent;
use crate::schema::types::KeyValue;
use std::collections::{BTreeSet, HashMap};

struct AcceptedHeads {
    key: KeyValue,
    mutation_id: String,
    fields: Vec<(String, String)>,
}

impl MutationManager {
    pub(super) async fn stage_peer_notifications(
        &self,
        job: &DeferredPersistJob,
        outcomes: &[&AcceptedFieldOutcome],
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        let heads = accepted_heads(job, outcomes);
        if heads.is_empty() {
            return Ok(());
        }
        let committed_at_ms = crate::clock::unix_millis();
        let events = heads
            .values()
            .map(|head| ChangeFeedEvent {
                seq: 0,
                mutation_id: head.mutation_id.clone(),
                schema: job.schema_name.clone(),
                operation: "update".to_string(),
                hash: head.key.hash.clone(),
                range: head.key.range.clone(),
                committed_at_ms,
                background_tasks_drained: false,
                convergence_pending: true,
            })
            .collect();
        // Nested native scopes share the current pending batch. Subscribers
        // receive these typed notifications only after its complete commit.
        self.db_ops.change_feed().append_many(events).await?;
        if let Some(batch) = self.peer_search_batch(job, heads).await? {
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                MutationManager::deliver_search_outbox(inbox, batch).await
            })?;
        }
        Ok(())
    }

    async fn peer_search_batch(
        &self,
        job: &DeferredPersistJob,
        heads: BTreeMap<(String, String), AcceptedHeads>,
    ) -> Result<Option<IndexChangeBatch>, SchemaError> {
        if !crate::db_operations::search_index::queue_policy::enabled() {
            return Ok(None);
        }
        let ids: BTreeSet<&str> = heads
            .values()
            .flat_map(|head| &head.fields)
            .map(|(_, atom)| atom.as_str())
            .collect();
        let ids: Vec<&str> = ids.into_iter().collect();
        let fetched = self.db_ops.atoms().get_atoms(&ids, None).await?;
        let atoms: BTreeMap<String, _> = ids
            .into_iter()
            .zip(fetched)
            .map(|(id, atom)| {
                atom.map(|atom| (id.to_owned(), atom))
                    .ok_or_else(|| unsupported("a selected Search atom without a stored body"))
            })
            .collect::<Result<_, _>>()?;
        let searchable = searchable_fields(&job.schema);
        let changes = heads
            .into_values()
            .map(|head| {
                let fields: HashMap<_, _> = head
                    .fields
                    .into_iter()
                    .map(|(name, atom)| (name, atoms[atom.as_str()].content().clone()))
                    .collect();
                IndexChange {
                    mutation_id: head.mutation_id,
                    kind: IndexChangeKind::Upsert,
                    key_value: head.key,
                    fields_and_values: inbox_fields(&fields, searchable.as_ref()),
                }
            })
            .collect();
        Ok(Some(IndexChangeBatch {
            format_version: 1,
            schema_name: job.schema_name.clone(),
            searchable_fields: searchable,
            changes,
        }))
    }
}

fn accepted_heads(
    job: &DeferredPersistJob,
    outcomes: &[&AcceptedFieldOutcome],
) -> BTreeMap<(String, String), AcceptedHeads> {
    let mut heads: BTreeMap<(String, String), AcceptedHeads> = BTreeMap::new();
    for outcome in outcomes {
        let Some(actual) = job
            .schema
            .runtime_fields
            .get(&outcome.field_name)
            .and_then(|field| field.atom_entry_at_key(&outcome.key))
        else {
            continue;
        };
        if actual.lww_key() != outcome.tip.lww_key() {
            continue;
        }
        let head = heads
            .entry((outcome.key.to_storage_key(), actual.mutation_uuid.clone()))
            .or_insert_with(|| AcceptedHeads {
                key: outcome.key.clone(),
                mutation_id: actual.mutation_uuid.clone(),
                fields: Vec::new(),
            });
        head.fields
            .push((outcome.field_name.clone(), actual.atom_uuid.clone()));
    }
    heads
}
