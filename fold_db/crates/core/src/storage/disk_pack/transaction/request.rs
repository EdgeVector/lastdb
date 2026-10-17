//! An owned complete request survives cancellation and owns its flush deadline.

use super::*;
use crate::storage::laststore::LastStoreKvStore;
use tracing::Instrument;

pub(crate) async fn request_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    memory_ack: bool,
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
    let delay = if memory_ack {
        Some(super::super::space::memory_flush_delay()?)
    } else {
        None
    };
    let gate = runtime.enter_write().await;
    let pending = Arc::new(PendingBatch::new(&runtime));
    let result = read::write_scope(
        Arc::clone(&runtime),
        WRITE.scope(Arc::clone(&pending), future),
    )
    .await??;
    let operations = pending.freeze_operations();
    let delay = delay.filter(|_| !pending.durable_only.load(Ordering::Acquire));
    let owner = Arc::clone(&runtime);
    // Preparation owns the gate even if the caller stops while disk capacity
    // is checked. No accepted view exists until all reservations succeed.
    let (prepared, gate) = LastStoreKvStore::run_blocking(move || {
        let prepared = if operations.is_empty() {
            None
        } else {
            Some(owner.prepare_commit(operations)?)
        };
        Ok((prepared, gate))
    })
    .await?;
    if let Some(prepared) = &prepared {
        pending
            .retained
            .lock()
            .expect("native request resources")
            .push(Box::new(Arc::clone(&prepared.protection)));
    }
    let delay = delay.filter(|_| prepared.is_some());
    let delay = protect_paths(&runtime, &pending, delay)?;
    let memory_ack = delay.is_some();
    if delay.is_some() {
        runtime.install_accepted(Arc::clone(&pending))?;
    }
    let (sent, received) = tokio::sync::oneshot::channel();
    // No await separates accepted publication from the owned worker transfer.
    tokio::spawn(
        async move {
            let outcome = finish(
                Arc::clone(&runtime),
                Arc::clone(&pending),
                prepared,
                gate,
                delay,
            )
            .await;
            if outcome.is_err() {
                runtime.failed.store(true, Ordering::Release);
                *runtime
                    .failed_request
                    .lock()
                    .expect("native failed request") = Some(pending);
            }
            let _ = sent.send(outcome);
        }
        .in_current_span(),
    );
    if !memory_ack {
        received.await.map_err(|_| {
            StorageError::BackendError(
                "native complete request owner stopped before its result".into(),
            )
        })??;
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
    protection.publish()?;
    pending
        .retained
        .lock()
        .expect("native request resources")
        .push(Box::new(protection));
    Ok(delay)
}

async fn finish(
    runtime: Arc<PackRuntime>,
    pending: Arc<PendingBatch>,
    prepared: Option<super::super::write::PreparedCommit>,
    gate: tokio::sync::OwnedMutexGuard<()>,
    delay: Option<std::time::Duration>,
) -> StorageResult<()> {
    if let Some(delay) = delay {
        tokio::select! {
            () = tokio::time::sleep(delay) => {},
            () = runtime.flush_notify.notified() => {},
        }
    }
    let owner = Arc::clone(&runtime);
    let committed = Arc::clone(&pending);
    LastStoreKvStore::run_blocking(move || {
        let _gate = gate;
        if let Some(prepared) = prepared {
            owner.commit_prepared(prepared)?;
        }
        if let Err(error) = committed.publications.finish() {
            owner.failed.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    })
    .await?;
    let actions = std::mem::take(
        &mut *pending
            .after_commit
            .lock()
            .expect("native post-commit work"),
    );
    for action in actions {
        action.await?;
    }
    Ok(())
}
