//! An enabled capture home cannot accept an unlogged logical data change.

use super::*;

pub(crate) fn mark_logical_capture(runtime: &Arc<PackRuntime>) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation(
            "logical capture requires a native request transaction".into(),
        )
    })?;
    if !runtime.logical_capture_enabled() {
        return Err(StorageError::InvalidOperation(
            "logical capture is not enabled on this home".into(),
        ));
    }
    pending.logical_capture_ready.store(true, Ordering::Release);
    Ok(())
}

pub(in crate::storage::disk_pack) fn require_captured_changes(
    runtime: &PackRuntime,
    pending: Option<&PendingBatch>,
    operations: &[HomeMutation],
) -> StorageResult<()> {
    if !runtime.logical_capture_enabled()
        || pending.is_some_and(|batch| batch.logical_capture_ready.load(Ordering::Acquire))
    {
        return Ok(());
    }
    for operation in operations {
        require_metadata_only(operation)?;
    }
    Ok(())
}

fn require_metadata_only(operation: &HomeMutation) -> StorageResult<()> {
    if matches!(
        tips_family(operation)?.as_deref(),
        Some("mk" | "rk" | "rdel" | "mgp" | "mgr" | "mgd")
    ) {
        return Err(StorageError::InvalidOperation(
            "logical capture refuses this unsupported data writer before native acceptance".into(),
        ));
    }
    Ok(())
}

/// The key family of one row in the `tips` collection. A row of another
/// collection, and other metadata in the fallback collection, has none.
pub(super) fn tips_family(operation: &HomeMutation) -> StorageResult<Option<String>> {
    if operation.collection != "tips" {
        return Ok(None);
    }
    let key = super::super::native_routing::decode(&operation.key)?;
    let Ok(key) = std::str::from_utf8(&key) else {
        return Ok(None);
    };
    let Ok(key) = crate::storage::current_keys::source_key(key) else {
        // Other metadata in the fallback collection is not a molecule key.
        return Ok(None);
    };
    Ok(key.split([':', '\0']).next().map(str::to_owned))
}
