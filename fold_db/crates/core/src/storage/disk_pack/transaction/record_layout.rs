//! A format 7 home stores records. It refuses a field molecule writer.
//!
//! The native create and update path writes records in a format 7 home. The
//! other data writers (delete, purge, peer import, restore, aggregates) still
//! write field molecule tips, delete barriers or generations. Until each moves
//! to records, its request fails before native acceptance instead of mixing
//! the two layouts in one home.

use super::*;

pub(in crate::storage::disk_pack) fn require_record_layout(
    runtime: &PackRuntime,
    operations: &[HomeMutation],
) -> StorageResult<()> {
    if !runtime.record_home() {
        return Ok(());
    }
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
