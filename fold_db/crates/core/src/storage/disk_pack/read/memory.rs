//! Shared core definitions are reusable only at the captured logical boundary.

use super::super::*;
use crate::resident::{CoreMemorySource, CoreMemoryValue, CorePath, CoreWarmSet};
use std::any::Any;

impl PackRuntime {
    /// Both queries and private write preparation own one complete native view.
    pub(crate) fn captured_view_active(&self) -> bool {
        super::ReadContext::active(self)
    }
}

impl super::ReadContext {
    pub(in crate::storage::disk_pack) fn require_prepared_view(
        &self,
        pending: &Arc<super::super::transaction::PendingBatch>,
    ) -> StorageResult<()> {
        self.validate_owner(Some(pending))?;
        let call = crate::read_receipts::lock(&self.call);
        self.require_room(&call, 0, 0)
    }

    /// Hold these paths for this call in the database's call hold owner.
    pub(in crate::storage::disk_pack) fn hold_core(
        &self,
        core: &mut CoreWarmSet,
        paths: &[CorePath],
    ) -> StorageResult<()> {
        let holds: &(dyn Any + Send + Sync) = &*self.core_holds;
        holds
            .downcast_ref::<super::core_holds::CoreCallHolds>()
            .ok_or_else(|| {
                StorageError::BackendError("native call holds belong to another memory".into())
            })?
            .hold(core, paths)
    }
}

/// The exact physical dependency of one core definition record.
pub(crate) fn core_memory_sources(namespace: &str, key: &[u8]) -> Vec<CoreMemorySource> {
    let (collection, key) = kv::coordinate(namespace, key);
    vec![CoreMemorySource {
        collection,
        key: Some(key),
    }]
}

pub(crate) fn core_memory_get<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    owner: usize,
    name: &str,
) -> StorageResult<Option<Arc<CoreMemoryValue<T>>>> {
    if let Some(value) = read_snapshot_get(runtime.clone(), owner, name)? {
        return Ok(Some(value));
    }
    if !read_snapshot_active(runtime.clone()) {
        return Ok(None);
    }
    let Some(runtime) = runtime else {
        return Ok(None);
    };
    let Some(context) = super::ReadContext::captured(&runtime) else {
        return Ok(None);
    };
    if context.hidden_prefix {
        return Ok(None);
    }
    let Some(mut current) = lock_current_core(&runtime, context.revision)? else {
        return Ok(None);
    };
    let value = current.core.memory_get(owner, name);
    drop(current);
    value
        .map(|value| {
            super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name.to_owned(), value)
        })
        .transpose()
}

/// A private writer may borrow a clean core definition only when none of its
/// exact dependencies changed in this request or its captured accepted prefix.
/// This never exposes a dirty record or admits a new shared cache entry.
pub(crate) fn core_memory_get_for_write<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    owner: usize,
    name: &str,
    sources: &[CoreMemorySource],
) -> StorageResult<Option<Arc<CoreMemoryValue<T>>>> {
    let Some(runtime) = runtime else {
        return Ok(None);
    };
    let Some(pending) = transaction::PendingBatch::current(&runtime) else {
        return Ok(None);
    };
    let Some(context) = super::ReadContext::captured(&runtime) else {
        return Ok(None);
    };
    context.require_prepared_view(&pending)?;
    if sources.is_empty() || sources.len() > 64 {
        return Ok(None);
    }
    for source in sources {
        // Whole-collection dependencies require a different proof. This
        // route accepts only complete, exact source identities.
        let Some(key) = &source.key else {
            return Ok(None);
        };
        if pending.lookup(&source.collection, key).is_some()
            || context
                .accepted
                .iter()
                .any(|prior| prior.lookup(&source.collection, key).is_some())
        {
            return Ok(None);
        }
    }
    let Some(mut current) = lock_current_core(&runtime, context.revision)? else {
        return Ok(None);
    };
    Ok(current.core.memory_get(owner, name))
}

pub(crate) fn core_memory_insert<T: Any + Send + Sync>(
    runtime: Option<Arc<PackRuntime>>,
    owner: usize,
    name: String,
    value: T,
    sources: Vec<CoreMemorySource>,
    bytes: usize,
) -> StorageResult<Arc<CoreMemoryValue<T>>> {
    if let Some(held) = read_snapshot_get(runtime.clone(), owner, &name)? {
        return Ok(held);
    }
    if !read_snapshot_active(runtime.clone()) {
        return Ok(CoreWarmSet::memory_uncached(value));
    }
    let runtime = runtime.ok_or_else(lock_error)?;
    let context = super::ReadContext::captured(&runtime).ok_or_else(lock_error)?;
    if context.hidden_prefix {
        let value = CoreWarmSet::memory_uncached(value);
        return super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name, value);
    }
    let value = if let Some(mut current) = lock_current_core(&runtime, context.revision)? {
        // A concurrent reader may finish the same slow path first.
        current.core.memory_get(owner, &name).unwrap_or_else(|| {
            current
                .core
                .memory_insert(owner, name.clone(), value, sources, bytes)
        })
    } else {
        CoreWarmSet::memory_uncached(value)
    };
    super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name, value)
}

fn lock_error() -> StorageError {
    StorageError::BackendError("core definition memory lock is unavailable".into())
}

/// Hold the published view, then the core, while the captured revision matches.
/// A newer view returns `None` and does not take the core lock. `core` drops
/// before the view, which is the same order as the previous direct locks.
fn lock_current_core(
    runtime: &PackRuntime,
    revision: u64,
) -> StorageResult<Option<CurrentCore<'_>>> {
    let view = runtime.view.read().map_err(|_| lock_error())?;
    if view.revision != revision {
        return Ok(None);
    }
    let core = runtime.core().lock().map_err(|_| lock_error())?;
    Ok(Some(CurrentCore { core, _view: view }))
}

struct CurrentCore<'a> {
    core: std::sync::MutexGuard<'a, CoreWarmSet>,
    _view: std::sync::RwLockReadGuard<'a, super::super::view::NativeReadView>,
}
