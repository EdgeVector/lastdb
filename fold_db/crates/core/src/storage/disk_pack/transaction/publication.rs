//! Scope-local metadata and cache publication after the complete disk root.

use super::{Arc, BTreeMap, Mutex, PackRuntime, PendingBatch, StorageError, StorageResult};
use std::any::Any;

type Action = Box<dyn FnOnce() -> StorageResult<()> + Send>;
type CacheKey = (usize, String);
type Value = Arc<dyn Any + Send + Sync>;

pub(crate) fn active(runtime: Option<Arc<PackRuntime>>) -> bool {
    runtime
        .and_then(|runtime| PendingBatch::current(&runtime))
        .is_some()
}

#[derive(Default)]
struct Changes {
    entries: BTreeMap<CacheKey, (Value, Action)>,
    staged: usize,
}

#[derive(Default)]
pub(super) struct PendingPublications(Mutex<Changes>);

impl PendingPublications {
    fn lock(&self) -> StorageResult<std::sync::MutexGuard<'_, Changes>> {
        self.0.lock().map_err(|_| {
            StorageError::BackendError("native metadata publication lock is poisoned".into())
        })
    }

    fn stage(&self, key: CacheKey, value: Value, action: Action) -> StorageResult<()> {
        let mut changes = self.lock()?;
        if changes.staged >= 4096 {
            return Err(StorageError::InvalidOperation(
                "native mutation exceeds 4096 metadata publications".into(),
            ));
        }
        changes.entries.insert(key, (value, action));
        changes.staged += 1;
        Ok(())
    }

    pub(super) fn finish(&self) -> StorageResult<()> {
        let changes = std::mem::take(&mut *self.lock()?);
        let mut failure = None;
        for (_, (_, action)) in changes.entries {
            if let Err(error) = action() {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// The value is visible only to this native write scope until its root succeeds.
/// Outside a native write scope, retain the existing immediate cache behavior.
pub(crate) fn publish<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
    name: String,
    value: T,
    action: impl FnOnce() -> StorageResult<()> + Send + 'static,
) -> StorageResult<()> {
    match runtime.and_then(|runtime| PendingBatch::current(&runtime)) {
        Some(pending) => {
            pending
                .publications
                .stage((cache, name), Arc::new(value), Box::new(action))
        }
        None => action(),
    }
}

pub(crate) fn get<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    let Some(pending) = runtime.and_then(|runtime| PendingBatch::current(&runtime)) else {
        return Ok(None);
    };
    let value = pending
        .publications
        .lock()?
        .entries
        .get(&(cache, name.to_string()))
        .map(|(value, _)| {
            Arc::clone(value).downcast::<T>().map_err(|_| {
                StorageError::BackendError("native metadata cache type differs".into())
            })
        })
        .transpose();
    value
}

pub(crate) fn entries<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    cache: usize,
) -> StorageResult<Vec<(String, Arc<T>)>> {
    let Some(pending) = runtime.and_then(|runtime| PendingBatch::current(&runtime)) else {
        return Ok(Vec::new());
    };
    let changes = pending.publications.lock()?;
    changes
        .entries
        .iter()
        .filter(|((owner, _), _)| *owner == cache)
        .map(|((_, name), (value, _))| {
            Arc::clone(value)
                .downcast::<T>()
                .map(|value| (name.clone(), value))
                .map_err(|_| {
                    StorageError::BackendError("native metadata cache type differs".into())
                })
        })
        .collect()
}
