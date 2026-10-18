//! Storage dependencies and conservative charges for existing core schemas.

use crate::db_operations::schema_store::SchemaReadSnapshot;
use crate::schema::{Schema, SchemaError, SchemaWithState};
use crate::storage::disk_pack::{core_memory_sources, CoreMemorySource};

pub(super) fn snapshot_sources(name: &str) -> Vec<CoreMemorySource> {
    let mut sources = core_memory_sources("schemas", name.as_bytes());
    for form in crate::kind_partition::read_forms(name) {
        sources.extend(core_memory_sources("schema_states", form.as_bytes()));
    }
    // Include absent successors too: a later supersession changes this object.
    sources.extend(core_memory_sources("schema_superseded_by", name.as_bytes()));
    sources
}

pub(super) fn directory_sources() -> Vec<CoreMemorySource> {
    vec![
        CoreMemorySource::collection("schemas"),
        CoreMemorySource::collection("schema_states"),
    ]
}

pub(super) fn snapshot_bytes(snapshot: &SchemaReadSnapshot) -> Result<usize, SchemaError> {
    let schema = snapshot
        .schema
        .as_ref()
        .map(schema_bytes)
        .transpose()?
        .unwrap_or(1024);
    Ok(schema.saturating_add(
        snapshot
            .successor
            .as_ref()
            .map_or(0, |name| name.len().saturating_mul(2)),
    ))
}

pub(super) fn directory_bytes(directory: &[SchemaWithState]) -> Result<usize, SchemaError> {
    directory.iter().try_fold(1024usize, |bytes, entry| {
        Ok(bytes.saturating_add(schema_bytes(&entry.schema)?))
    })
}

fn schema_bytes(schema: &Schema) -> Result<usize, SchemaError> {
    let wire = serde_json::to_vec(schema).map_err(|error| SchemaError::StorageRead {
        detail: format!("cannot account for captured schema memory: {error}"),
    })?;
    // Runtime fields contain their own strings and mapper containers. The
    // catalog serialization excludes them, so charge both forms explicitly.
    Ok(wire
        .len()
        .saturating_mul(8)
        .saturating_add(schema.runtime_fields.len().saturating_mul(2048))
        .saturating_add(1024))
}
