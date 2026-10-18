//! Storage dependencies and conservative charges for existing core schemas.

use crate::schema::{SchemaError, SchemaWithState};
use crate::storage::disk_pack::CoreMemorySource;

pub(super) fn directory_sources() -> Vec<CoreMemorySource> {
    vec![
        CoreMemorySource::collection("schemas"),
        CoreMemorySource::collection("schema_states"),
    ]
}

pub(super) fn directory_bytes(directory: &[SchemaWithState]) -> Result<usize, SchemaError> {
    directory.iter().try_fold(1024usize, |bytes, entry| {
        Ok(
            bytes.saturating_add(crate::db_operations::schema_store::schema_bytes(
                &entry.schema,
            )?),
        )
    })
}
