use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::{SchemaCatalog, SchemaCore};
use crate::schema::types::SchemaError;
use crate::schema::SchemaState;

impl SchemaCore {
    /// Creates a new SchemaCore with DbOperations (storage abstraction)
    pub async fn new(db_ops: Arc<crate::db_operations::DbOperations>) -> Result<Self, SchemaError> {
        let schema_core = Self {
            catalog: Arc::new(RwLock::new(Arc::new(SchemaCatalog::default()))),
            db_ops: db_ops.clone(),
            catalog_ready: AtomicBool::new(false),
        };

        let stored_schemas = db_ops.get_all_schemas().await?;
        for schema in stored_schemas.values() {
            super::load::owned::admit_stored_schema(schema)?;
        }
        let schema_states = db_ops.get_all_schema_states().await?;
        let retired_name_claims = db_ops.list_retired_name_claims().await?;

        let hydrated = Arc::new(SchemaCatalog {
            schemas: stored_schemas,
            states: schema_states,
            retired_name_claims: retired_name_claims.into_iter().collect(),
        });
        {
            let mut guard = schema_core.catalog.write().map_err(|e| {
                SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}"))
            })?;
            *guard = hydrated;
        }
        schema_core.mark_catalog_ready();

        Ok(schema_core)
    }

    /// Whether the initial schema catalog load from the store has finished.
    #[must_use]
    pub fn catalog_ready(&self) -> bool {
        self.catalog_ready.load(Ordering::Acquire)
    }

    /// Mark the in-memory catalog ready for list/query after the initial store scan.
    pub(crate) fn mark_catalog_ready(&self) {
        self.catalog_ready.store(true, Ordering::Release);
    }

    pub fn get_schema_states(
        &self,
    ) -> Result<std::collections::HashMap<String, SchemaState>, SchemaError> {
        let mut states = self.catalog_snapshot()?.states.clone();
        for (name, staged) in self.staged_schemas()? {
            states.insert(name, staged.state);
        }
        Ok(states)
    }

    pub fn get_schema_state_cached(&self, schema_name: &str) -> Result<SchemaState, SchemaError> {
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok(staged.state);
        }
        Ok(self
            .catalog_snapshot()?
            .states
            .get(schema_name)
            .copied()
            .unwrap_or_default())
    }

    /// All schema value and key reads share the resolved schema's state gate.
    pub(crate) fn require_queryable_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema_state_cached(schema_name)? == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be queried"
            )));
        }
        Ok(())
    }

    /// Retire one schema's claim on its `descriptive_name`.
    pub async fn retire_name_claim(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.set_name_claim_retired(schema_name, true).await
    }

    /// Restore a retired claim so the schema answers `descriptive_name` resolution again.
    pub async fn restore_name_claim(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.set_name_claim_retired(schema_name, false).await
    }

    /// Remove one installed schema identity from the catalog and the RAM cache.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        if self
            .db_ops
            .block_schema_for_drop(schema_name)
            .await?
            .is_some()
        {
            self.update_catalog(|catalog| {
                catalog
                    .states
                    .insert(schema_name.to_string(), SchemaState::Blocked);
            })?;
        }
        let existed = self.db_ops.drop_schema(schema_name).await?;
        self.update_catalog(|catalog| {
            catalog.schemas.remove(schema_name);
            catalog.states.remove(schema_name);
            catalog.retired_name_claims.remove(schema_name);
        })?;
        Ok(existed)
    }

    /// The cached identities whose `owner_app_id` equals `owner_app`.
    pub fn schema_names_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        Ok(catalog
            .schemas
            .iter()
            .filter(|(_, schema)| schema.owner_app_id.as_deref() == Some(owner_app))
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn set_name_claim_retired(
        &self,
        schema_name: &str,
        retired: bool,
    ) -> Result<bool, SchemaError> {
        let changed = self
            .db_ops
            .set_schema_name_claim_retired(schema_name, retired)
            .await?;
        self.update_catalog(|catalog| {
            if retired {
                catalog.retired_name_claims.insert(schema_name.to_string());
            } else {
                catalog.retired_name_claims.remove(schema_name);
            }
        })?;
        Ok(changed)
    }

    /// Every schema name whose claim on its `descriptive_name` is retired.
    pub fn retired_name_claims(&self) -> Result<std::collections::HashSet<String>, SchemaError> {
        Ok(self.catalog_snapshot()?.retired_name_claims.clone())
    }

    /// Get the database operations.
    pub fn db_ops(&self) -> &Arc<crate::db_operations::DbOperations> {
        &self.db_ops
    }
}
