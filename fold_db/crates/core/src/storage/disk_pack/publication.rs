//! Staged in-memory metadata cache publications bound to the active write batch.

use super::read::{unpoison, ReadContext};
use super::transaction::PendingBatch;
use super::{PackRuntime, StorageError, StorageResult};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type CommitAction = Box<dyn FnOnce() -> StorageResult<()> + Send>;
type CachedValue = Arc<dyn Any + Send + Sync>;

#[derive(Default)]
struct StagedPublications {
    entries: BTreeMap<(usize, String), (CachedValue, CommitAction)>,
    total_staged: usize,
}

#[derive(Default)]
pub(in crate::storage::disk_pack) struct PendingPublications {
    inner: Mutex<StagedPublications>,
}

impl PendingPublications {
    pub(in crate::storage::disk_pack) fn is_empty(&self) -> StorageResult<bool> {
        Ok(unpoison(&self.inner).entries.is_empty())
    }

    pub(in crate::storage::disk_pack) fn finish(&self) -> StorageResult<()> {
        let mut first_err = None;
        for (_, (_, action)) in std::mem::take(&mut *unpoison(&self.inner)).entries {
            if let Err(err) = action() {
                first_err.get_or_insert(err);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

pub(crate) fn publish<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: String,
    value: T,
    action: impl FnOnce() -> StorageResult<()> + Send + 'static,
) -> StorageResult<()> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return action();
    };
    let mut staged = unpoison(&pending.publications.inner);
    if staged.total_staged >= 4096 {
        return Err(StorageError::invalid(
            "native mutation exceeds 4096 metadata publications",
        ));
    }
    staged
        .entries
        .insert((cache, name), (Arc::new(value), Box::new(action)));
    staged.total_staged += 1;
    Ok(())
}

fn cast<T: Any + Send + Sync>(value: &CachedValue) -> StorageResult<Arc<T>> {
    Arc::clone(value)
        .downcast::<T>()
        .map_err(|_| StorageError::backend("metadata cache type differs"))
}

pub(crate) fn get<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    let key = (cache, name.to_string());
    for batch in write_views(runtime).into_iter().rev() {
        if let Some((value, _)) = unpoison(&batch.publications.inner).entries.get(&key) {
            return cast(value).map(Some);
        }
    }
    Ok(None)
}

pub(crate) fn entries<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
) -> StorageResult<Vec<(String, Arc<T>)>> {
    let mut out = BTreeMap::new();
    for batch in write_views(runtime) {
        for ((owner, name), (value, _)) in &unpoison(&batch.publications.inner).entries {
            if *owner == cache {
                out.insert(name.clone(), cast(value)?);
            }
        }
    }
    Ok(out.into_iter().collect())
}

fn write_views(runtime: &PackRuntime) -> Vec<Arc<PendingBatch>> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return Vec::new();
    };
    let mut views =
        ReadContext::captured(runtime).map_or_else(Vec::new, |ctx| (*ctx.accepted_batches).clone());
    views.push(pending);
    views
}
