//! Reuse one exclusion across the chunks of an exact native request.

use super::*;
use std::any::Any;

/// A lease belongs only to the current request, never a prior accepted view.
/// Resource delivery releases it after durable publication. The lookup keeps
/// weak references, so old read snapshots cannot prolong the exclusion.
pub(crate) fn lease<T: Any + Send + Sync>(
    runtime: Option<&PackRuntime>,
    name: String,
    acquire: impl FnOnce() -> Option<T>,
) -> StorageResult<Option<Arc<T>>> {
    let Some(pending) = runtime.and_then(PendingBatch::current) else {
        return Ok(acquire().map(Arc::new));
    };
    let mut leases = pending
        .leases
        .lock()
        .map_err(|_| StorageError::BackendError("native request lease lock poisoned".into()))?;
    if let Some(value) = leases.get(&name).and_then(std::sync::Weak::upgrade) {
        return value
            .downcast::<T>()
            .map(Some)
            .map_err(|_| StorageError::BackendError("native request lease type differs".into()));
    }
    if leases.len() >= 4096 {
        return Err(StorageError::WriteResourceLimit {
            detail: "native request exceeds 4096 named resources".into(),
        });
    }
    let Some(value) = acquire().map(Arc::new) else {
        return Ok(None);
    };
    pending
        .resources
        .lock()
        .map_err(|_| StorageError::BackendError("native scoped resource lock poisoned".into()))?
        .push(Arc::clone(&value) as resources::Resource);
    let erased: Arc<dyn Any + Send + Sync> = value.clone();
    leases.insert(name, Arc::downgrade(&erased));
    Ok(Some(value))
}
