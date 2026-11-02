//! Scope-local metadata and cache publication after the complete disk root.

use super::{Arc, BTreeMap, Mutex, PackRuntime, PendingBatch, StorageError, StorageResult};
use std::any::Any;

type Action = Box<dyn FnOnce() -> StorageResult<()> + Send>;
type CacheKey = (usize, String);
type Value = Arc<dyn Any + Send + Sync>;

pub(crate) fn active(runtime: &PackRuntime) -> bool {
    PendingBatch::current(runtime).is_some()
}

#[derive(Default)]
struct Changes {
    entries: BTreeMap<CacheKey, (Value, Action)>,
    staged: usize,
}

#[derive(Default)]
pub(super) struct PendingPublications(Mutex<Changes>);

impl PendingPublications {
    pub(super) fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.lock()?.entries.is_empty())
    }

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

/// Private metadata joins the accepted view before global cache publication.
/// Later write scopes see that immutable accepted prefix. Ordinary queries use
/// typed core definitions and their captured disk root; global caches change
/// after the disk root.
/// Outside a native write scope, retain the existing immediate cache behavior.
pub(crate) fn publish<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: String,
    value: T,
    action: impl FnOnce() -> StorageResult<()> + Send + 'static,
) -> StorageResult<()> {
    match PendingBatch::current(runtime) {
        Some(pending) => {
            pending
                .publications
                .stage((cache, name), Arc::new(value), Box::new(action))
        }
        None => action(),
    }
}

pub(crate) fn get<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    for pending in write_views(runtime).into_iter().rev() {
        let value = pending
            .publications
            .lock()?
            .entries
            .get(&(cache, name.to_string()))
            .map(|(value, _)| Arc::clone(value));
        if let Some(value) = value {
            return value.downcast::<T>().map(Some).map_err(|_| type_mismatch());
        }
    }
    Ok(None)
}

pub(crate) fn entries<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
) -> StorageResult<Vec<(String, Arc<T>)>> {
    let mut entries = BTreeMap::new();
    for pending in write_views(runtime) {
        let changes = pending.publications.lock()?;
        for ((owner, name), (value, _)) in &changes.entries {
            if *owner == cache {
                entries.insert(
                    name.clone(),
                    Arc::clone(value)
                        .downcast::<T>()
                        .map_err(|_| type_mismatch())?,
                );
            }
        }
    }
    Ok(entries.into_iter().collect())
}

/// Preparation owns the gate, so accepted publications cannot drain mid-read.
/// Ordinary queries retain their separate immutable raw-store metadata view.
fn write_views(runtime: &PackRuntime) -> Vec<Arc<PendingBatch>> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return Vec::new();
    };
    let mut views = super::super::read::ReadContext::captured(runtime)
        .map_or_else(Vec::new, |context| context.accepted.as_ref().clone());
    views.push(pending);
    views
}

fn type_mismatch() -> StorageError {
    StorageError::BackendError("native metadata cache type differs".into())
}
