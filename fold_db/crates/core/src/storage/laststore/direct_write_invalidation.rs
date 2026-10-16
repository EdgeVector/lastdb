//! A direct LastStore write can change keys before it returns an error.

use std::sync::Mutex;

use super::{logical_path, LastStoreKvStore, LogicalResidentSet, StorageResult};

/// Invalidate the exact logical keys after every direct write attempt.
pub(super) fn after_direct_write<T>(
    logical: &Mutex<LogicalResidentSet>,
    ids: &[String],
    write: impl FnOnce() -> StorageResult<T>,
) -> StorageResult<T> {
    let result = write();
    let mut logical = logical.lock().expect("poison");
    logical.forget_all_records();
    for id in ids {
        logical_path::invalidate_written_id(&mut logical, id);
    }
    result
}

pub(super) fn after_transaction(
    logical: &Mutex<LogicalResidentSet>,
    store: &laststore::LastStore,
    ops: Vec<laststore::TxnOp>,
) -> StorageResult<()> {
    let ids: Vec<_> = ops.iter().map(|op| op.id().to_string()).collect();
    after_direct_write(logical, &ids, || {
        store.transaction(ops).map_err(LastStoreKvStore::map_error)
    })
}
