//! Internal helpers for the noisy `FoldDbError::Config(...)` boilerplate that
//! wraps every poison check on the in-memory `RwLock` caches in `state.rs` /
//! `snapshot.rs` / `state_*.rs`.
//!
//! Error message text is intentionally identical to the strings these
//! helpers replace — preserving the API contract.

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use schema_types::FoldDbError;

pub(crate) fn read_lock<'a, T>(
    lock: &'a RwLock<T>,
    name: &str,
) -> Result<RwLockReadGuard<'a, T>, FoldDbError> {
    lock.read()
        .map_err(|_| FoldDbError::Config(format!("Failed to acquire {name} read lock")))
}

pub(crate) fn write_lock<'a, T>(
    lock: &'a RwLock<T>,
    name: &str,
) -> Result<RwLockWriteGuard<'a, T>, FoldDbError> {
    lock.write()
        .map_err(|_| FoldDbError::Config(format!("Failed to acquire {name} write lock")))
}
