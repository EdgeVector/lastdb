//! Public write entry points, batch pipeline orchestration, timing, finalize.

use std::collections::{HashMap, HashSet};

use crate::schema::types::Mutation;
use crate::schema::{SchemaError, SchemaState};

use super::receipt::{
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
};
use super::MutationManager;

use super::helpers::apply_storage_prefix_to_schema;

mod access_entry;
mod admission;
mod attribution;
mod batch_inner;
mod create_update_batch;
mod erase_guards;
mod erasure;
mod field_keys;
mod layout;
mod native;
mod record_effects;
mod records;
use attribution::{attribution_scopes, attribution_source_events_enabled};
mod replay_cutoff;
mod schema_delta_helpers;
mod timing;

impl MutationManager {
    /// [`Self::write_mutations_with_access_receipt`] returning only the
    /// accepted input mutation ids, in caller order.
    ///
    /// The future is boxed here. A caller that spawns a task on it would
    /// otherwise make the compiler prove `Send` through the whole write graph
    /// from the spawn site, and that proof overflows the recursion limit.
    pub fn write_mutations_with_access<'a>(
        &'a self,
        mutations: Vec<Mutation>,
        access_context: &'a crate::access::AccessContext,
    ) -> futures::future::BoxFuture<'a, Result<Vec<String>, SchemaError>> {
        use futures::FutureExt;
        async move {
            Ok(self
                .write_mutations_with_access_receipt(mutations, access_context)
                .await?
                .mutation_ids)
        }
        .boxed()
    }

    /// Write multiple mutations in a batch for improved performance (async version)
    /// Groups mutations by schema to minimize schema reloads and uses true batching
    ///
    /// This is the preferred async version that avoids deadlocks.
    /// All storage operations use direct async/await instead of run_async.
    ///
    /// `storage_prefix`: when `Some`, scopes field key/idempotency keys to that
    /// org/db hash (see design-org-db-handle-platform-gap). `None` is personal.
    pub async fn write_mutations_batch_async(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        Ok(self
            .write_mutations_batch_with_receipt(mutations, storage_prefix)
            .await?
            .mutation_ids)
    }

    // --- Helpers for write_mutations_batch_async ---

    pub(super) fn reject_blocked_mutation_targets(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let states = self.schema_manager.get_schema_states()?;
        for mutation in mutations {
            if states
                .get(&mutation.schema_name)
                .copied()
                .unwrap_or_default()
                == SchemaState::Blocked
            {
                return Err(SchemaError::Blocked(format!(
                    "Schema '{}' is blocked and cannot be mutated (superseded, or a drop is running)",
                    mutation.schema_name
                )));
            }
        }
        Ok(())
    }

    /// Groups mutations by schema name for efficient batch processing.
    /// Returns groups in first-seen schema order.
    pub(super) fn group_mutations_by_schema(
        &self,
        mutations: Vec<Mutation>,
    ) -> Vec<(String, Vec<Mutation>)> {
        let mut order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, Vec<Mutation>> = HashMap::new();

        for mutation in mutations {
            if !grouped.contains_key(&mutation.schema_name) {
                order.push(mutation.schema_name.clone());
            }
            grouped
                .entry(mutation.schema_name.clone())
                .or_default()
                .push(mutation);
        }

        order
            .into_iter()
            .map(|name| {
                let mutations = grouped
                    .remove(&name)
                    .expect("schema order came from grouped keys");
                (name, mutations)
            })
            .collect()
    }
}
