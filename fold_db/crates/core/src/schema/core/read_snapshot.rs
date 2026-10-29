//! Core schema objects shared only across compatible captured native views.

use super::{Schema, SchemaCore, SchemaState};
use crate::db_operations::schema_store::{snapshot_bytes, snapshot_sources, SchemaReadSnapshot};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{
    core_memory_get, core_memory_insert, core_record_insert, read_snapshot_active,
    read_snapshot_get, CoreMemoryValue,
};
use std::sync::Arc;

mod memory;

impl SchemaCore {
    /// Native name resolution reads only the caller's captured root and overlay.
    /// `None` selects the existing non-native name-resolution policy.
    ///
    /// # Errors
    /// Returns an error for unreadable metadata or an exhausted call budget.
    pub async fn captured_name_exists(&self, name: &str) -> Result<Option<bool>, SchemaError> {
        if !self.native_read_snapshot_active() {
            return Ok(None);
        }
        Ok(Some(
            self.native_schema_snapshot(name).await?.schema.is_some(),
        ))
    }

    /// A complete directory is shared until catalog or name-claim publication.
    ///
    /// # Errors
    /// Returns an error for an incomplete or unreadable captured directory.
    pub async fn captured_name_candidates(
        &self,
    ) -> Result<Option<Arc<CoreMemoryValue<Vec<crate::schema::SchemaWithState>>>>, SchemaError>
    {
        if !self.native_read_snapshot_active() {
            return Ok(None);
        }
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let cache = self.db_ops.schemas().snapshot_owner();
        let key = "\0native_schema_name_directory";
        if let Some(directory) = core_memory_get(runtime.clone(), cache, key)? {
            return Ok(Some(directory));
        }
        let directory = self.db_ops.schemas().read_name_directory().await?;
        let bytes = memory::directory_bytes(&directory)?;
        Ok(Some(core_memory_insert(
            runtime,
            cache,
            key.into(),
            directory,
            memory::directory_sources(),
            bytes,
        )?))
    }

    pub(super) async fn resolve_native_schema(
        &self,
        name: &str,
        depth: usize,
        for_read: bool,
    ) -> Result<Option<Schema>, SchemaError> {
        let snapshot = self.native_schema_snapshot(name).await?;
        if let Some(successor) = &snapshot.successor {
            return self
                .get_schema_resolved(successor, depth + 1, for_read)
                .await;
        }
        Ok(snapshot.schema.as_ref().map(|schema| {
            if for_read {
                schema.clone_for_read_shared()
            } else {
                schema.clone()
            }
        }))
    }

    pub(super) fn native_read_snapshot_active(&self) -> bool {
        read_snapshot_active(self.db_ops.namespaced_store().disk_pack_runtime())
    }

    pub(super) async fn native_schema_snapshot(
        &self,
        name: &str,
    ) -> Result<Arc<CoreMemoryValue<SchemaReadSnapshot>>, SchemaError> {
        let snapshot = self.schema_memory_snapshot(name).await?;
        if let Some(canonical) = snapshot
            .schema
            .as_ref()
            .map(|schema| &schema.name)
            .filter(|canonical| canonical.as_str() != name)
        {
            // Query execution checks the returned schema's own name. Capture
            // that exact state, rather than substituting the alias's state.
            self.schema_memory_snapshot(canonical).await?;
        }
        Ok(snapshot)
    }

    async fn schema_memory_snapshot(
        &self,
        name: &str,
    ) -> Result<Arc<CoreMemoryValue<SchemaReadSnapshot>>, SchemaError> {
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let cache = self.db_ops.schemas().snapshot_owner();
        if let Some(snapshot) = core_memory_get(runtime.clone(), cache, name)? {
            return Ok(snapshot);
        }
        let snapshot = self.db_ops.schemas().read_snapshot(name).await?;
        let bytes = snapshot_bytes(&snapshot)?;
        Ok(core_record_insert(
            runtime,
            cache,
            name.to_string(),
            snapshot,
            snapshot_sources(name),
            bytes,
        )?)
    }

    pub(super) fn captured_schema_metadata(
        &self,
        name: &str,
    ) -> Result<Option<Option<Schema>>, SchemaError> {
        let snapshot = read_snapshot_get::<CoreMemoryValue<SchemaReadSnapshot>>(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            self.db_ops.schemas().snapshot_owner(),
            name,
        )?;
        Ok(snapshot.map(|snapshot| snapshot.schema.clone()))
    }

    pub(super) fn captured_schema_state(
        &self,
        name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        Ok(read_snapshot_get::<CoreMemoryValue<SchemaReadSnapshot>>(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            self.db_ops.schemas().snapshot_owner(),
            name,
        )?
        .map(|snapshot| snapshot.state))
    }
}
