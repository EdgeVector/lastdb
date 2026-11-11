//! Native schema reloads keep their cache state private until disk publication.

use super::{Schema, SchemaCore, SchemaState};
use crate::schema::types::SchemaError;
use crate::storage::disk_pack::transaction::publication;
use crate::storage::StorageError;
use std::sync::Arc;

pub(super) struct CachedSchema {
    pub(super) schema: Schema,
    pub(super) state: SchemaState,
}

impl SchemaCore {
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
}
