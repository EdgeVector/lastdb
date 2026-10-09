//! A direct LastStore write can change keys before it returns an error.

use std::sync::Mutex;

use super::{LogicalResidentSet, StorageResult};

/// Drop fetched records and negative hints after every direct write attempt.
pub(super) fn after_direct_write<T>(
    logical: &Mutex<LogicalResidentSet>,
    write: impl FnOnce() -> StorageResult<T>,
) -> StorageResult<T> {
    let result = write();
    logical.lock().expect("poison").forget_all_records();
    result
}
