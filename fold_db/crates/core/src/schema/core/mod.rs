use crate::schema::types::{Schema, SchemaError};
use crate::schema::{SchemaListEntry, SchemaState, SchemaWithState};
use crate::storage::disk_pack::transaction::publication;
use crate::storage::StorageError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

mod load;

#[derive(Clone, Default)]
pub(super) struct SchemaCatalog {
    pub(super) schemas: HashMap<String, Schema>,
    pub(super) states: HashMap<String, SchemaState>,
}

pub(super) struct CachedSchema {
    pub(super) schema: Schema,
    pub(super) state: SchemaState,
}

/// Core schema management system combining validation, copy-on-write catalog, and persistence.
pub struct SchemaCore {
    catalog: Arc<RwLock<Arc<SchemaCatalog>>>,
    pub(super) db_ops: Arc<crate::db_operations::DbOperations>,
    catalog_ready: AtomicBool,
}

impl SchemaCore {
    pub async fn new(db_ops: Arc<crate::db_operations::DbOperations>) -> Result<Self, SchemaError> {
        let schema_core = Self {
            catalog: Arc::new(RwLock::new(Arc::new(SchemaCatalog::default()))),
            db_ops: Arc::clone(&db_ops),
            catalog_ready: AtomicBool::new(false),
        };

        let stored_schemas = db_ops.get_all_schemas().await?;
        for schema in stored_schemas.values() {
            load::admit_stored_schema(schema)?;
        }
        let schema_states = db_ops.get_all_schema_states().await?;
        let hydrated = Arc::new(SchemaCatalog {
            schemas: stored_schemas,
            states: schema_states,
        });
        *schema_core.catalog.write().map_err(|e| {
            SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}"))
        })? = hydrated;
        schema_core.mark_catalog_ready();
        Ok(schema_core)
    }

    pub(super) fn catalog_snapshot(&self) -> Result<Arc<SchemaCatalog>, SchemaError> {
        self.catalog
            .read()
            .map(|guard| Arc::clone(&guard))
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))
    }

    pub(super) fn update_catalog(
        &self,
        f: impl FnOnce(&mut SchemaCatalog),
    ) -> Result<(), SchemaError> {
        let mut guard = self
            .catalog
            .write()
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))?;
        f(Arc::make_mut(&mut *guard));
        Ok(())
    }

    pub(super) fn staged_schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<CachedSchema>>, SchemaError> {
        Ok(publication::get(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
            name,
        )?)
    }

    pub(super) fn staged_schemas(&self) -> Result<Vec<(String, Arc<CachedSchema>)>, SchemaError> {
        Ok(publication::entries(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
        )?)
    }

    pub(crate) fn publish_schema_cache(
        &self,
        name: &str,
        schema: Schema,
        state: SchemaState,
    ) -> Result<(), SchemaError> {
        let catalog = Arc::clone(&self.catalog);
        let name = name.to_string();
        let staged = CachedSchema {
            schema: schema.clone(),
            state,
        };
        publication::publish(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
            name.clone(),
            staged,
            move || {
                let mut guard = catalog.write().map_err(|e| {
                    StorageError::BackendError(format!("schema catalog lock poisoned: {e}"))
                })?;
                let inner = Arc::make_mut(&mut *guard);
                inner.schemas.insert(name.clone(), schema);
                inner.states.insert(name, state);
                Ok(())
            },
        )?;
        Ok(())
    }

    #[must_use]
    pub fn catalog_ready(&self) -> bool {
        self.catalog_ready.load(Ordering::Acquire)
    }

    pub(crate) fn mark_catalog_ready(&self) {
        self.catalog_ready.store(true, Ordering::Release);
    }

    pub fn db_ops(&self) -> &Arc<crate::db_operations::DbOperations> {
        &self.db_ops
    }

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
        Ok(schemas
            .into_iter()
            .map(|(name, schema)| {
                let state = states.get(&name).copied().unwrap_or_default();
                SchemaWithState::new(schema, state)
            })
            .collect())
    }

    pub fn get_schema_list_entries_with_states(&self) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        Ok(catalog
            .schemas
            .values()
            .map(|schema| {
                let state = catalog
                    .states
                    .get(&schema.name)
                    .copied()
                    .unwrap_or_default();
                SchemaListEntry::from_schema(schema, state)
            })
            .collect())
    }

    pub fn get_active_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        Ok(self
            .get_schemas_with_states()?
            .into_iter()
            .filter(|s| s.state != SchemaState::Blocked)
            .collect())
    }

    pub fn get_active_schema_list_entries_with_states(
        &self,
    ) -> Result<Vec<SchemaListEntry>, SchemaError> {
        Ok(self
            .get_schema_list_entries_with_states()?
            .into_iter()
            .filter(|s| s.state != SchemaState::Blocked)
            .collect())
    }

    pub fn get_schema_metadata(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok(Some(staged.schema.clone()));
        }
        Ok(self.catalog_snapshot()?.schemas.get(schema_name).cloned())
    }

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

    pub(crate) fn require_schema_metadata(&self, schema_name: &str) -> Result<Schema, SchemaError> {
        self.get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))
    }

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

    pub fn get_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
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

    pub(crate) fn require_queryable_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema_state_cached(schema_name)? == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be queried"
            )));
        }
        Ok(())
    }

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
        })?;
        Ok(existed)
    }

    pub fn schema_names_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        Ok(catalog
            .schemas
            .iter()
            .filter(|(_, schema)| schema.owner_app_id.as_deref() == Some(owner_app))
            .map(|(name, _)| name.clone())
            .collect())
    }
}
