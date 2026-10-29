//! Typed schema publications use the same objects and dependencies as reads.

use super::{SchemaReadSnapshot, SchemaStore};
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::{core_memory_sources, transaction, CoreMemorySource};
use std::sync::Arc;

impl crate::resident::CoreRecordData for SchemaReadSnapshot {
    fn record_type(&self, collection: &str) -> std::any::TypeId {
        if collection == "schemas" {
            std::any::TypeId::of::<Schema>()
        } else {
            std::any::TypeId::of::<String>()
        }
    }
    fn record_slot(&self, collection: &str) -> usize {
        usize::from(collection == "schema_superseded_by")
    }

    fn clone_record(
        &self,
        collection: &str,
        target: std::any::TypeId,
    ) -> serde_json::Result<Option<crate::resident::TypedRecordValue>> {
        match collection {
            "schemas" if target == std::any::TypeId::of::<Schema>() => {
                let schema = Schema::from(self.schema.as_ref().expect("present schema").to_wire());
                Ok(Some(Box::new(schema)))
            }
            "schemas" => crate::resident::clone_record_value(
                self.schema.as_ref().expect("present schema"),
                target,
            ),
            "schema_superseded_by" => crate::resident::clone_record_value(
                self.successor.as_ref().expect("present successor"),
                target,
            ),
            _ => Ok(None),
        }
    }

    fn record_present(&self, collection: &str) -> Option<bool> {
        match collection {
            "schemas" => Some(self.schema.is_some()),
            "schema_superseded_by" if self.state == crate::schema::SchemaState::Blocked => {
                Some(self.successor.is_some())
            }
            // The default state does not establish whether its source was absent.
            _ => None,
        }
    }

    fn write_record(
        &self,
        collection: &str,
        out: &mut dyn std::io::Write,
    ) -> serde_json::Result<()> {
        match collection {
            "schemas" => serde_json::to_writer(out, self.schema.as_ref().expect("present schema")),
            "schema_superseded_by" => {
                serde_json::to_writer(out, self.successor.as_ref().expect("present successor"))
            }
            _ => unreachable!("unsupported schema source"),
        }
    }
}

impl SchemaStore {
    pub(super) fn prepared_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<Arc<crate::resident::CoreMemoryValue<SchemaReadSnapshot>>>, SchemaError>
    {
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(None);
        };
        Ok(transaction::prepared_core_memory(
            runtime,
            self.snapshot_owner(),
            name,
        )?)
    }

    pub(crate) fn snapshot_owner(&self) -> usize {
        Arc::as_ptr(&self.schemas_store) as usize
    }

    pub(super) async fn stage_native_snapshot(&self, name: &str) -> Result<(), SchemaError> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(());
        };
        if !transaction::publication::active(Some(Arc::clone(runtime))) {
            return Ok(());
        }
        if self.prepared_snapshot(name)?.is_some() {
            return Ok(());
        }
        let snapshot = self.read_snapshot(name).await?;
        let bytes = snapshot_bytes(&snapshot)?;
        transaction::stage_core_memory(
            runtime,
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
