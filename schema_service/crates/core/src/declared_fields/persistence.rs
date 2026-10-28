//! Conditional declaration updates shared by durable and memory backends.

use super::DeclaredFieldRecord;
use schema_types::{FoldDbError, FoldDbResult};
use std::collections::HashMap;

pub fn validate_declaration_commit(
    existing: &HashMap<String, DeclaredFieldRecord>,
    record: &DeclaredFieldRecord,
    expected: Option<&DeclaredFieldRecord>,
) -> FoldDbResult<()> {
    if existing.get(&record.declaration_id) != expected
        || existing.values().any(|other| {
            other.owner_app_id == record.owner_app_id
                && other.handle == record.handle
                && other.declaration_id != record.declaration_id
        })
    {
        return Err(FoldDbError::Config(
            "declared_field_write_conflict: declaration changed concurrently; retry the declaration"
                .to_owned(),
        ));
    }
    Ok(())
}
