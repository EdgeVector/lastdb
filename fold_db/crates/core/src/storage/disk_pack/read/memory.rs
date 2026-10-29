//! Shared core definitions are reusable only at the captured logical boundary.

use super::super::*;
use crate::resident::{CoreMemorySource, CoreMemoryValue, CoreWarmSet};
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
    if let Some(value) = context.core_model.memory_get(owner, name) {
        return super::snapshots::read_snapshot_insert_arc(
            Some(runtime),
            owner,
            name.to_owned(),
            value,
        )
        .map(Some);
    }
    let view = runtime.view.read().map_err(|_| lock_error())?;
    if view.revision != context.revision {
        return Ok(None);
    }
    let value = runtime
        .core
        .lock()
        .map_err(|_| lock_error())?
        .memory_get(owner, name);
    drop(view);
    value
        .map(|value| {
            super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name.to_owned(), value)
        })
        .transpose()
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
    if let Some(value) = context.core_model.memory_get(owner, &name) {
        return super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name, value);
    }
    let view = runtime.view.read().map_err(|_| lock_error())?;
    let value = if view.revision == context.revision {
        let mut core = runtime.core.lock().map_err(|_| lock_error())?;
        // A concurrent reader may finish the same slow path first.
        core.memory_get(owner, &name)
            .unwrap_or_else(|| core.memory_insert(owner, name.clone(), value, sources, bytes))
    } else {
        CoreWarmSet::memory_uncached(value)
    };
    drop(view);
    super::snapshots::read_snapshot_insert_arc(Some(runtime), owner, name, value)
}

fn lock_error() -> StorageError {
    StorageError::BackendError("core definition memory lock is unavailable".into())
}

/// Expose an exact source reader over this same typed allocation, not bytes.
pub(crate) fn core_record_insert<T: crate::resident::CoreRecordData>(
    runtime: Option<Arc<PackRuntime>>,
    owner: usize,
    name: String,
    value: T,
    sources: Vec<CoreMemorySource>,
    bytes: usize,
) -> StorageResult<Arc<CoreMemoryValue<T>>> {
    let held = core_memory_insert(runtime.clone(), owner, name.clone(), value, sources, bytes)?;
    if let Some(runtime) = runtime {
        if let Some(context) = super::ReadContext::captured(&runtime) {
            let view = runtime.view.read().map_err(|_| lock_error())?;
            if view.revision == context.revision && !context.hidden_prefix {
                runtime
                    .core
                    .lock()
                    .map_err(|_| lock_error())?
                    .expose_memory_record(owner, &name, &held);
            }
        }
    }
    Ok(held)
}
