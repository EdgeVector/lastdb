use std::collections::HashMap;

use super::SchemaCore;
use crate::schema::types::{Schema, SchemaError};
use crate::schema::{SchemaListEntry, SchemaState, SchemaWithState};

impl SchemaCore {
    pub fn get_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let mut schemas = self.catalog_snapshot()?.schemas.clone();
        for (name, staged) in self.staged_schemas()? {
            schemas.insert(name, staged.schema.clone());
        }
        Ok(schemas)
    }

    pub fn get_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        let mut schemas = catalog.schemas.clone();
        let mut states = catalog.states.clone();
        for (name, staged) in self.staged_schemas()? {
            schemas.insert(name.clone(), staged.schema.clone());
            states.insert(name, staged.state);
        }

        let mut with_states = Vec::with_capacity(schemas.len());
        for (name, schema) in schemas {
            let state = states.get(&name).copied().unwrap_or_default();
            with_states.push(SchemaWithState::new(schema, state));
        }

        Ok(with_states)
    }

    pub fn get_schema_list_entries_with_states(&self) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let catalog = self.catalog_snapshot()?;

        let mut entries = Vec::with_capacity(catalog.schemas.len());
        for schema in catalog.schemas.values() {
            let state = catalog
                .states
                .get(&schema.name)
                .copied()
                .unwrap_or_default();
            entries.push(SchemaListEntry::from_schema(schema, state));
        }

        Ok(entries)
    }

    /// Returns only active (non-Blocked) schemas for UI listings.
    /// A Blocked schema is being dropped and should not appear in the Data Browser.
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

    /// Raw, as-named lookup from the cache. Returns exactly the schema the
    /// caller named (or `None`), even if it is `Blocked`.
    pub fn get_schema_metadata(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok(Some(staged.schema.clone()));
        }
        Ok(self.catalog_snapshot()?.schemas.get(schema_name).cloned())
    }

    /// Read a schema and its state together from one catalog snapshot.
    pub(crate) fn get_schema_and_state(
        &self,
        schema_name: &str,
    ) -> Result<(Option<Schema>, SchemaState), SchemaError> {
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok((Some(staged.schema.clone()), staged.state));
        }
        let catalog = self.catalog_snapshot()?;
        let schema = catalog.schemas.get(schema_name).cloned();
        let state = catalog.states.get(schema_name).copied().unwrap_or_default();
        Ok((schema, state))
    }

    /// The named schema, or `Schema '{name}' not found`.
    /// This uses the raw lookup and does not refresh a stale cache from the database.
    pub(crate) fn require_schema_metadata(&self, schema_name: &str) -> Result<Schema, SchemaError> {
        self.get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))
    }

    /// Return the cached identity hash for a registered schema, computed at
    /// registration time by `Schema::compute_identity_hash`. `None` for unknown schemas.
    pub fn get_identity_hash(&self, schema_name: &str) -> Option<String> {
        if let Some(staged) = self.staged_schema(schema_name).ok()? {
            return staged.schema.get_identity_hash().cloned();
        }
        self.catalog_snapshot()
            .ok()?
            .schemas
            .get(schema_name)
            .and_then(|schema| schema.get_identity_hash().cloned())
    }

    /// Names of cached schemas whose `identity_hash` was minted by an algorithm
    /// **newer** than this binary implements. Sorted so the report is stable.
    pub fn schemas_with_newer_identity_algo(&self) -> Result<Vec<String>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        let mut names: Vec<String> = catalog
            .schemas
            .iter()
            .filter(|(_, schema)| schema.identity_is_newer_than_binary())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        Ok(names)
    }
}
