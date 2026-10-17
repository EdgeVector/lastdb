//! Native queries use their own catalog values, never a live shared cache.

use super::{Schema, SchemaCore, SchemaState};
use crate::db_operations::schema_store::SchemaReadSnapshot;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
use std::sync::Arc;

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

    /// The directory is read on an exact-name miss and expires with this call.
    ///
    /// # Errors
    /// Returns an error for an incomplete or unreadable captured directory.
    pub async fn captured_name_candidates(
        &self,
    ) -> Result<Option<Arc<Vec<crate::schema::SchemaWithState>>>, SchemaError> {
        if !self.native_read_snapshot_active() {
            return Ok(None);
        }
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let cache = Arc::as_ptr(&self.schemas) as usize;
        let key = "\0native_schema_name_directory";
        if let Some(directory) = read_snapshot_get(runtime.clone(), cache, key)? {
            return Ok(Some(directory));
        }
        let directory = self.db_ops.schemas().read_name_directory().await?;
        Ok(Some(read_snapshot_insert(
            runtime,
            cache,
            key.into(),
            directory,
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
    ) -> Result<Arc<SchemaReadSnapshot>, SchemaError> {
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let cache = Arc::as_ptr(&self.schemas) as usize;
        if let Some(snapshot) = read_snapshot_get(runtime.clone(), cache, name)? {
            return Ok(snapshot);
        }
        let snapshot = self.db_ops.schemas().read_snapshot(name).await?;
        if let Some(canonical) = snapshot
            .schema
            .as_ref()
            .map(|schema| &schema.name)
            .filter(|canonical| canonical.as_str() != name)
        {
            // Query execution checks the returned schema's own name. Read
            // that exact state from the same held view, never copy alias state.
            if read_snapshot_get::<SchemaReadSnapshot>(runtime.clone(), cache, canonical)?.is_none()
            {
                let value = self.db_ops.schemas().read_snapshot(canonical).await?;
                read_snapshot_insert(runtime.clone(), cache, canonical.clone(), value)?;
            }
        }
        Ok(read_snapshot_insert(
            runtime,
            cache,
            name.to_string(),
            snapshot,
        )?)
    }

    pub(super) fn captured_schema_metadata(
        &self,
        name: &str,
    ) -> Result<Option<Option<Schema>>, SchemaError> {
        let snapshot = read_snapshot_get::<SchemaReadSnapshot>(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            Arc::as_ptr(&self.schemas) as usize,
            name,
        )?;
        Ok(snapshot.map(|snapshot| snapshot.schema.clone()))
    }

    pub(super) fn captured_schema_state(
        &self,
        name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        Ok(read_snapshot_get::<SchemaReadSnapshot>(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            Arc::as_ptr(&self.schemas) as usize,
            name,
        )?
        .map(|snapshot| snapshot.state))
    }
}
