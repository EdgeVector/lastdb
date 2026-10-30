//! Native schema reloads keep their cache state private until disk publication.

use super::locks::{lock_map, read_map, write_map};
use super::{Schema, SchemaCore, SchemaState};
use crate::schema::types::SchemaError;
use crate::storage::disk_pack::transaction::publication;
use crate::storage::error::StorageResult;
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
            self.db_ops.namespaced_store().disk_pack_runtime(),
            Arc::as_ptr(&self.schemas) as usize,
            name,
        )?)
    }

    pub(super) fn staged_schemas(&self) -> Result<Vec<(String, Arc<CachedSchema>)>, SchemaError> {
        Ok(publication::entries(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            Arc::as_ptr(&self.schemas) as usize,
        )?)
    }

    pub(crate) fn publish_schema_cache(
        &self,
        name: &str,
        schema: Schema,
        state: SchemaState,
    ) -> Result<(), SchemaError> {
        let schemas = Arc::clone(&self.schemas);
        let states = Arc::clone(&self.schema_states);
        let name = name.to_string();
        let staged = CachedSchema {
            schema: schema.clone(),
            state,
        };
        publication::publish(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            Arc::as_ptr(&self.schemas) as usize,
            name.clone(),
            staged,
            move || {
                let mut schemas =
                    write_map(&schemas, "schemas").map_err(|error| cache_error(&error))?;
                let mut states =
                    lock_map(&states, "schema_states").map_err(|error| cache_error(&error))?;
                schemas.insert(name.clone(), schema);
                states.insert(name, state);
                Ok(())
            },
        )?;
        Ok(())
    }

    pub(super) fn cached_schema_for_read(&self, name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(staged) = self.staged_schema(name)? {
            return Ok(Some(staged.schema.clone_for_read_shared()));
        }
        Ok(read_map(&self.schemas, "schemas")?
            .get(name)
            .map(Schema::clone_for_read_shared))
    }
}

fn cache_error(error: &SchemaError) -> StorageError {
    StorageError::BackendError(error.to_string())
}
