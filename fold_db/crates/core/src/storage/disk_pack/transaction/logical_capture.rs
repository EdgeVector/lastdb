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
        if changes_logical_data(operation)? {
            return Err(StorageError::InvalidOperation(
                "logical capture refuses this unsupported data writer before native acceptance"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn changes_logical_data(operation: &HomeMutation) -> StorageResult<bool> {
    if !matches!(operation.collection.as_str(), "tips" | "proteins") {
        return Ok(false);
    }
    let key = super::super::native_routing::decode(&operation.key)?;
    let Ok(key) = std::str::from_utf8(&key) else {
        return Ok(false);
    };
    let Ok(key) = crate::storage::current_keys::source_key(key) else {
        // Other metadata in the fallback collection is not a molecule key.
        return Ok(false);
    };
    let family = key.split([':', '\0']).next().unwrap_or_default();
    Ok(matches!(
        family,
        "mk" | "rdel" | "mgp" | "mgr" | "mgd" | "protein" | "molprot" | "fldprot" | "pfq"
    ))
}
