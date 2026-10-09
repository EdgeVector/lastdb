use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};

use crate::schema::types::SchemaError;

/// Acquire a `Mutex<HashMap<String, T>>` lock, mapping poison errors to `SchemaError`.
pub(super) fn lock_map<'a, T>(
    map: &'a Mutex<HashMap<String, T>>,
    name: &str,
) -> Result<std::sync::MutexGuard<'a, HashMap<String, T>>, SchemaError> {
    map.lock()
        .map_err(|_| SchemaError::InvalidData(format!("Failed to acquire {name} lock")))
}

/// Acquire a **shared read** lock on an `RwLock<HashMap<String, T>>`, mapping
/// poison errors to `SchemaError`. The read path uses this so concurrent readers
/// don't serialize (see the `schemas` field doc).
pub(super) fn read_map<'a, T>(
    map: &'a RwLock<HashMap<String, T>>,
    name: &str,
) -> Result<std::sync::RwLockReadGuard<'a, HashMap<String, T>>, SchemaError> {
    map.read()
        .map_err(|_| SchemaError::InvalidData(format!("Failed to acquire {name} read lock")))
}

/// Acquire an **exclusive write** lock on an `RwLock<HashMap<String, T>>,
/// mapping poison errors to `SchemaError`. Used by the rare schema-mutation
/// paths (load/insert/remove), never on the read hot path.
pub(super) fn write_map<'a, T>(
    map: &'a RwLock<HashMap<String, T>>,
    name: &str,
) -> Result<std::sync::RwLockWriteGuard<'a, HashMap<String, T>>, SchemaError> {
    map.write()
        .map_err(|_| SchemaError::InvalidData(format!("Failed to acquire {name} write lock")))
}

/// Acquire a `Mutex<HashSet<String>>` guard, mapping poison errors to
/// `SchemaError`. The set counterpart of [`lock_map`].
pub(super) fn lock_set<'a>(
    set: &'a Mutex<HashSet<String>>,
    name: &str,
) -> Result<std::sync::MutexGuard<'a, HashSet<String>>, SchemaError> {
    set.lock()
        .map_err(|_| SchemaError::InvalidData(format!("Failed to acquire {name} lock")))
}
