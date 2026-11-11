use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use crate::schema::types::{Schema, SchemaError};
use crate::schema::SchemaState;

mod cache;
mod load;
mod staged_cache;
mod state;

/// Copy-on-write snapshot of the in-memory schema catalog.
#[derive(Clone, Default)]
pub(super) struct SchemaCatalog {
    pub(super) schemas: HashMap<String, Schema>,
    pub(super) states: HashMap<String, SchemaState>,
}

/// Core schema management system that combines schema validation, caching, and persistence.
pub struct SchemaCore {
    /// Unified copy-on-write catalog of schemas and states.
    catalog: Arc<RwLock<Arc<SchemaCatalog>>>,
    /// Unified database operations with storage abstraction
    db_ops: Arc<crate::db_operations::DbOperations>,
    /// Set once the initial store → memory catalog load finishes.
    catalog_ready: AtomicBool,
}

impl SchemaCore {
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
}
