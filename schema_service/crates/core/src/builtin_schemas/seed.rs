use super::all_phase_1_schemas;
use crate::state::SchemaServiceState;
use crate::types::SchemaAddOutcome;
use schema_types::{FoldDbError, FoldDbResult, SchemaSource};
use std::collections::HashMap;

/// Seed every built-in schema into the given `SchemaServiceState`.
///
/// Idempotent: schemas whose identity_hash is already present in the
/// service are skipped. Schemas that are missing get added via
/// `state.add_schema()`, which is the same path user-data schemas
/// take. The only semantic difference is the origin — these come
/// from the service's own Rust code, not from a client proposal.
///
/// Called from `SchemaServiceServer::new()` so every production
/// schema service instance auto-seeds on boot. Tests that spin up a
/// `SchemaServiceState` directly must call this explicitly before
/// handing the state to an HTTP layer.
///
/// Fails loudly on the first error. Partial seeding is not a valid
/// state for the service — if we can't install the built-ins, the
/// service should refuse to start.
pub async fn seed(state: &SchemaServiceState) -> FoldDbResult<()> {
    // Built-ins are authoritative — they must register exactly, never fuzzy
    // reuse-before-NEW into a semantically-near committed seed (which would
    // trip the "built-ins never expand" invariant below). Gate the fuzzy
    // path off for the duration of this seed load.
    state.set_seeding_in_progress(true);
    let result = seed_inner(state).await;
    state.set_seeding_in_progress(false);
    result
}

async fn seed_inner(state: &SchemaServiceState) -> FoldDbResult<()> {
    for schema in all_phase_1_schemas() {
        let descriptive = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        match state.add_schema(schema, HashMap::new()).await {
            Ok(
                SchemaAddOutcome::Added(added, _)
                | SchemaAddOutcome::AlreadyExists(added, _)
                | SchemaAddOutcome::Composed(added, _, _),
            ) => {
                // Both outcomes are fine. AlreadyExists fires when the
                // service was previously seeded (or a client
                // previously proposed this schema via the legacy
                // propose flow). Idempotent either way.
                //
                // Tag the resulting identity hash as a system schema
                // so downstream clients (fold_db_node UI) can group
                // infrastructure schemas separately from user data.
                if added.source == SchemaSource::SystemSeed {
                    state.mark_system_schema(added.name.clone());
                }
            }
            Ok(SchemaAddOutcome::Expanded(old, _, _)) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: '{descriptive}' unexpectedly expanded existing schema '{old}'. \
                     Built-in schemas must not collide with user-proposed schemas."
                )));
            }
            Ok(SchemaAddOutcome::DescriptiveNameConflict(conflict)) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: '{}' conflicted with existing schema '{}': {}",
                    descriptive, conflict.existing_canonical, conflict.reason
                )));
            }
            Err(e) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: failed to seed '{descriptive}': {e}"
                )));
            }
        }
    }
    Ok(())
}
