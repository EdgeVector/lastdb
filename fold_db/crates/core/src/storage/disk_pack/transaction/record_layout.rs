//! Every home is format 7 and stores records. It refuses a field key writer.
//!
//! The native create, update, delete and purge path and the peer import write
//! records. A request that stages a field key tip, a delete barrier or a
//! generation fails before native acceptance instead of mixing the two layouts
//! in one home.

use super::*;

pub(in crate::storage::disk_pack) fn require_record_layout(
    operations: &[HomeMutation],
) -> StorageResult<()> {
    for operation in operations {
        if matches!(
            tips_family(operation)?.as_deref(),
            Some("mk" | "rdel" | "mgp" | "mgr" | "mgd")
        ) {
            return Err(StorageError::InvalidOperation(
                "a format 7 home stores records; this writer still writes field key tips".into(),
            ));
        }
    }
    Ok(())
}

/// The key family of one row in the `tips` collection. A row of another
/// collection, and other metadata in the fallback collection, has none.
fn tips_family(operation: &HomeMutation) -> StorageResult<Option<String>> {
    if operation.collection != "tips" {
        return Ok(None);
    }
    let key = super::super::native_routing::decode(&operation.key)?;
    let Ok(key) = std::str::from_utf8(&key) else {
        return Ok(None);
    };
    let Ok(key) = crate::storage::current_keys::source_key(key) else {
        return Ok(None);
    };
    Ok(key.split([':', '\0']).next().map(str::to_owned))
}
