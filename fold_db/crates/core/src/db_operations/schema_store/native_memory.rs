//! Typed schema publications use the same objects and dependencies as reads.

use super::{SchemaReadSnapshot, SchemaStore};
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::{core_memory_sources, transaction, CoreMemorySource};
use std::sync::Arc;

impl SchemaStore {
    pub(super) fn prepared_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<Arc<crate::resident::CoreMemoryValue<SchemaReadSnapshot>>>, SchemaError>
    {
        Ok(transaction::prepared_core_memory(
            &self.runtime,
            self.snapshot_owner(),
            name,
        )?)
    }

    pub(crate) fn snapshot_owner(&self) -> usize {
        Arc::as_ptr(&self.schemas_store) as usize
    }

    pub(super) async fn stage_native_snapshot(&self, name: &str) -> Result<(), SchemaError> {
        if !transaction::publication::active(&self.runtime) {
            return Ok(());
        }
        if self.prepared_snapshot(name)?.is_some() {
            return Ok(());
        }
        let snapshot = self.read_snapshot(name).await?;
        let bytes = snapshot_bytes(&snapshot)?;
        transaction::stage_core_memory(
            &self.runtime,
            self.snapshot_owner(),
            name.to_owned(),
            snapshot,
            snapshot_sources(name),
            bytes,
        )?;
        Ok(())
    }
}

pub(crate) fn snapshot_sources(name: &str) -> Vec<CoreMemorySource> {
    let mut sources = core_memory_sources("schemas", name.as_bytes());
    sources.extend(core_memory_sources("schema_states", name.as_bytes()));
    sources.extend(core_memory_sources("schema_superseded_by", name.as_bytes()));
    sources
}

pub(crate) fn snapshot_bytes(snapshot: &SchemaReadSnapshot) -> Result<usize, SchemaError> {
    let bytes = snapshot
        .schema
        .as_ref()
        .map(schema_bytes)
        .transpose()?
        .unwrap_or(1024);
    Ok(bytes.saturating_add(
        snapshot
            .successor
            .as_ref()
            .map_or(0, |name| name.len().saturating_mul(2)),
    ))
}

pub(crate) fn schema_bytes(schema: &Schema) -> Result<usize, SchemaError> {
    let wire = serde_json::to_vec(schema).map_err(|error| SchemaError::StorageRead {
        detail: format!("cannot account for captured schema memory: {error}"),
    })?;
    Ok(wire
        .len()
        .saturating_mul(8)
        .saturating_add(schema.runtime_fields.len().saturating_mul(2048))
        .saturating_add(1024))
}
