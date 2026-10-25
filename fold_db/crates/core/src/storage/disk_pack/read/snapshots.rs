//! Typed metadata belongs to one complete native query view, never a live cache.

use super::{Arc, Mutex, PackRuntime, ReadContext, StorageError, StorageResult, READ};
use std::any::Any;
use std::collections::BTreeMap;

type CacheKey = (usize, String);
type Value = Arc<dyn Any + Send + Sync>;

#[derive(Default)]
pub(super) struct ReadSnapshots(Mutex<BTreeMap<CacheKey, Value>>);

fn context(runtime: Option<Arc<PackRuntime>>) -> Option<Arc<ReadContext>> {
    runtime.and_then(|runtime| {
        if super::super::transaction::PendingBatch::current(&runtime).is_some() {
            return None;
        }
        READ.try_with(Arc::clone)
            .ok()
            .filter(|context| context.identity == runtime.home_identity())
    })
}

pub(crate) fn read_snapshot_active(runtime: Option<Arc<PackRuntime>>) -> bool {
    context(runtime).is_some()
}

pub(crate) fn read_snapshot_get<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    let Some(context) = context(runtime) else {
        return Ok(None);
    };
    let snapshots = context.snapshots.0.lock().map_err(|_| poisoned())?;
    snapshots
        .get(&(cache, name.to_string()))
        .map(|value| {
            Arc::clone(value)
                .downcast::<T>()
                .map_err(|_| type_mismatch())
        })
        .transpose()
}

pub(crate) fn read_snapshot_insert<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
    name: String,
    value: T,
) -> StorageResult<Arc<T>> {
    read_snapshot_insert_arc(runtime, cache, name, Arc::new(value))
}

pub(super) fn read_snapshot_insert_arc<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
    name: String,
    value: Arc<T>,
) -> StorageResult<Arc<T>> {
    let context = context(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native metadata snapshot requires a query scope".into())
    })?;
    let mut snapshots = context.snapshots.0.lock().map_err(|_| poisoned())?;
    let key = (cache, name);
    if let Some(value) = snapshots.get(&key) {
        return Arc::clone(value)
            .downcast::<T>()
            .map_err(|_| type_mismatch());
    }
    if snapshots.len() >= 10_000 {
        return Err(StorageError::ReadResourceLimit {
            detail: "native metadata snapshot exceeds its complete call limit".into(),
        });
    }
    snapshots.insert(key, value.clone());
    Ok(value)
}

fn poisoned() -> StorageError {
    StorageError::BackendError("native metadata snapshot lock is poisoned".into())
}

fn type_mismatch() -> StorageError {
    StorageError::BackendError("native metadata snapshot type differs".into())
}
