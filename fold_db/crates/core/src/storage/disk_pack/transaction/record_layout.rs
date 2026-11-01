//! Every home is format 7 and stores records. It refuses a field molecule writer.
//!
//! The native create, update, delete and purge path and the peer import write
//! records. A request that stages a field molecule tip, a delete barrier or a
//! generation fails before native acceptance instead of mixing the two layouts
//! in one home.

use super::*;

pub(in crate::storage::disk_pack) fn require_record_layout(
    operations: &[HomeMutation],
) -> StorageResult<()> {
    for operation in operations {
        if matches!(
            super::logical_capture::tips_family(operation)?.as_deref(),
            Some("mk" | "rdel" | "mgp" | "mgr" | "mgd")
        ) {
            return Err(StorageError::InvalidOperation(
                "a format 7 home stores records; this writer still writes field molecule tips"
                    .into(),
            ));
        }
    }
    Ok(())
}
