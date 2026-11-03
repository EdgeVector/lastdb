//! Public write entry points, batch pipeline orchestration, timing, finalize.

use std::collections::{HashMap, HashSet};

use crate::schema::types::Mutation;
use crate::schema::{SchemaError, SchemaState};

use super::receipt::{
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
};
use super::MutationManager;

mod access_entry;
mod batch_inner;
mod create_update_batch;
mod erase_guards;
mod field_keys;
mod layout;
mod native;
mod receipt_entry;
mod record_effects;
mod record_rules;
mod records;
mod replay_cutoff;
mod schema_delta_helpers;
mod timing;

impl MutationManager {
    /// [`Self::write_mutations_receipt`] returning only the accepted input
    /// mutation ids, in caller order.
    ///
    /// The future is boxed here. A caller that spawns a task on it would
    /// otherwise make the compiler prove `Send` through the whole write graph
    /// from the spawn site, and that proof overflows the recursion limit.
    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<String>, SchemaError>> {
        use futures::FutureExt;
        async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) }.boxed()
    }

    // --- Helpers for the batch write ---

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
                    "Schema '{}' is blocked and cannot be mutated (a drop is running)",
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
