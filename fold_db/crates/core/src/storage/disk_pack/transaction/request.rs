//! Complete accepted requests share one bounded queue and disk owner.

use super::*;
use crate::storage::laststore::LastStoreKvStore;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
    Memory,
    Durable,
    Background,
}

pub(crate) async fn request_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    memory_ack: bool,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(
        runtime,
        if memory_ack {
            Reply::Memory
        } else {
            Reply::Durable
        },
        future,
    )
    .await
}

/// Return after durability; unreserved work seals the current collection window.
pub(crate) async fn background_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(runtime, Reply::Background, future).await
}

async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    reply: Reply,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    let Some(runtime) = runtime else {
        return future.await;
    };
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let delay = match reply {
        Reply::Memory => Some(super::super::space::memory_flush_delay()?),
        Reply::Background => Some(std::time::Duration::from_millis(500)),
        Reply::Durable => None,
    };
    let gate = runtime.enter_prepare().await;
    let pending = Arc::new(PendingBatch::new(&runtime));
    let result = read::write_scope(
        Arc::clone(&runtime),
        Arc::clone(&pending),
        WRITE.scope(Arc::clone(&pending), future),
    )
    .await??;
    let operations = pending.freeze_operations();
    let delay = delay.filter(|_| {
        !pending.durable_only.load(Ordering::Acquire) && pending.has_reserved_raw_memory()
    });
    let owner = Arc::clone(&runtime);
    // A cancelled pre-admission caller cannot release the queue's older quota.
    let (prepared, gate) =
        LastStoreKvStore::run_blocking(move || Ok((owner.prepare_queued(operations)?, gate)))
            .await?;
    let delay = protect_paths(&runtime, &pending, delay)?;
    let wait = reply != Reply::Memory || delay.is_none();
    // Publication, ownership transfer and worker creation contain no await.
    let sequence = runtime.accept_queued(pending, prepared, delay)?;
    drop(gate);
    if wait {
        runtime.wait_for_queue(sequence).await?;
    }
    Ok(result)
}

fn protect_paths(
    runtime: &PackRuntime,
    pending: &PendingBatch,
    delay: Option<std::time::Duration>,
) -> StorageResult<Option<std::time::Duration>> {
    let paths = std::mem::take(&mut *pending.paths.lock().expect("native private paths"));
    if delay.is_none() {
        return Ok(None);
    }
    let protection = match super::super::logical_paths::PathProtection::reserve(runtime, &paths) {
        Ok(protection) => protection,
        Err(StorageError::WriteResourceLimit { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    *pending
        .path_protection
        .lock()
        .expect("native typed path protection") = Some(protection);
    Ok(delay)
}
