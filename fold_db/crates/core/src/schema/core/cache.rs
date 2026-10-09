use std::collections::HashMap;

use super::locks::{lock_map, lock_set, read_map, write_map};
use super::SchemaCore;
use crate::schema::types::{Schema, SchemaError};
use crate::schema::{SchemaListEntry, SchemaState, SchemaWithState};

impl SchemaCore {
    /// Reload schemas from the persistent store.
    ///
    /// Refreshes the in-memory cache from disk for both new and changed
    /// schemas — needed because sync replay writes directly to the
    /// persistent store and the cache must follow. Existing entries are
    /// overwritten only when the disk copy differs from the cached copy.
    ///
    /// Returns the number of schemas that were added or refreshed.
    pub async fn reload_from_store(&self) -> Result<usize, SchemaError> {
        // If the in-memory catalog is still empty, list readers must not see
        // empty-ok while this full scan runs — flip not-ready until we finish.
        let was_empty = read_map(&self.schemas, "schemas")?.is_empty();
        if was_empty {
            self.mark_catalog_not_ready();
        }

        let stored_schemas = self.db_ops.get_all_schemas().await?;
        let stored_states = self.db_ops.get_all_schema_states().await?;
        // Name claims live in the same node-local namespace and are written by
        // the same paths sync replay drives, so they follow the store here for
        // the same reason the state map does.
        let stored_retired_claims = self.db_ops.list_retired_name_claims().await?;

        let mut changed = 0usize;

        {
            let mut schemas = write_map(&self.schemas, "schemas")?;
            let mut states = lock_map(&self.schema_states, "schema_states")?;

            for (name, mut schema) in stored_schemas {
                // Ensure runtime_fields are populated — schemas coming from
                // sync replay won't have them (runtime_fields is #[serde(skip)]).
                if schema.runtime_fields.is_empty() {
                    schema.populate_runtime_fields()?;
                }

                let schema_changed = schemas.get(&name) != Some(&schema);
                if schema_changed {
                    schemas.insert(name.clone(), schema);
                    changed += 1;
                }

                if let Some(disk_state) = stored_states.get(&name).copied() {
                    states.insert(name.clone(), disk_state);
                } else {
                    states.entry(name.clone()).or_default();
                }
            }
        }

        {
            let mut retired = lock_set(&self.retired_name_claims, "retired_name_claims")?;
            *retired = stored_retired_claims.into_iter().collect();
        }

        // Always mark ready after a successful reload — even if the store is
        // genuinely empty (empty brain is a finished state, not hydrating).
        self.mark_catalog_ready();

        if changed > 0 {
            tracing::info!(
                "reload_from_store: refreshed {} schema(s) in cache",
                changed
            );
        }

        Ok(changed)
    }

    pub fn get_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        Ok(read_map(&self.schemas, "schemas")?.clone())
    }

    pub fn get_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let schemas = self.get_schemas()?;
        let schema_states = self.get_schema_states()?;

        let mut with_states = Vec::with_capacity(schemas.len());
        for (name, schema) in schemas {
            let state = schema_states.get(&name).copied().unwrap_or_default();
            with_states.push(SchemaWithState::new(schema, state));
        }

        Ok(with_states)
    }

    pub fn get_schema_list_entries_with_states(&self) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let schemas = read_map(&self.schemas, "schemas")?;
        let schema_states = lock_map(&self.schema_states, "schema_states")?;

        let mut entries = Vec::with_capacity(schemas.len());
        for schema in schemas.values() {
            let state = schema_states.get(&schema.name).copied().unwrap_or_default();
            entries.push(SchemaListEntry::from_schema(schema, state));
        }

        Ok(entries)
    }

    /// Returns only active (non-Blocked) schemas for UI listings.
    /// Blocked schemas have been superseded and should not appear in the Data Browser.
    pub fn get_active_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let all = self.get_schemas_with_states()?;
        Ok(all
            .into_iter()
            .filter(|s| s.state != SchemaState::Blocked)
            .collect())
    }

    /// Returns active schemas as a list projection for catalog listings.
    pub fn get_active_schema_list_entries_with_states(
        &self,
    ) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let all = self.get_schema_list_entries_with_states()?;
        Ok(all
            .into_iter()
            .filter(|s| s.state != SchemaState::Blocked)
            .collect())
    }

    /// Raw, as-named lookup — does **NOT** follow the `superseded_by` chain.
    /// Returns exactly the schema the caller named (or `None`), even if it is
    /// `Blocked` and has an active successor.
    ///
    /// Use this for *inspection* paths that report on the named schema as it
    /// stands. For any runtime/executor data path — where the executor will
    /// transparently redirect a `Blocked` name to its successor — use
    /// [`Self::get_schema_following_supersession`] so the gate and the
    /// executor agree on which schema's fields/owner are authoritative.
    pub fn get_schema_metadata(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        Ok(read_map(&self.schemas, "schemas")?
            .get(schema_name)
            .cloned())
    }

    /// TH6a — return the cached identity hash for a registered schema,
    /// computed at registration time by `Schema::compute_identity_hash`.
    /// `None` for unknown schemas. Used by the firing-snapshot capture
    /// path to stamp the `schema_versions` audit-row field without
    /// recomputing the hash on every fire (TH6a spec §7).
    pub fn get_identity_hash(&self, schema_name: &str) -> Option<String> {
        read_map(&self.schemas, "schemas")
            .ok()?
            .get(schema_name)
            .and_then(|s| s.get_identity_hash().cloned())
    }

    /// Names of cached schemas whose `identity_hash` was minted by an algorithm
    /// **newer** than this binary implements.
    ///
    /// A non-empty result means the node is older than its own data:
    /// `load_schema_internal` preserved those identities rather than
    /// downgrading them, and the node should be upgraded rather than the
    /// schemas re-registered. Surfaced on `GET /api/schemas` as
    /// `newer_identity_algo_schemas` so `lastdb status` and `brain doctor` can
    /// report it instead of the operator discovering it as a search miss.
    ///
    /// Sorted, so the report is stable across calls.
    pub fn schemas_with_newer_identity_algo(&self) -> Result<Vec<String>, SchemaError> {
        let schemas = read_map(&self.schemas, "schemas")?;
        let mut names: Vec<String> = schemas
            .iter()
            .filter(|(_, schema)| schema.identity_is_newer_than_binary())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        Ok(names)
    }
}
