//! Durable cloud purge state operations.

use super::worker::worker;
use super::*;

pub(super) fn is_running(host: &Host, id: &str) -> Result<bool, String> {
    Ok(runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .get(&host.home)
        .is_some_and(|runtime| runtime.running.as_deref() == Some(id)))
}

pub(super) fn spawn(host: &Host, id: &str) -> Result<(), String> {
    let (host, stop, cancel) = {
        let mut runtimes = runtimes()
            .lock()
            .map_err(|_| "cloud purge runtime lock failed")?;
        let runtime = runtimes
            .get_mut(&host.home)
            .ok_or("cloud purge runtime is not attached")?;
        if let Some(running) = &runtime.running {
            if running == id {
                return Ok(());
            }
            return Err("another cloud purge worker is active".into());
        }
        let host = runtime.host.upgrade().ok_or("cloud purge host closed")?;
        runtime.running = Some(id.into());
        runtime.cancel.store(false, Ordering::Release);
        (host, Arc::clone(&runtime.stop), Arc::clone(&runtime.cancel))
    };
    let id = id.to_string();
    let home = host.home.clone();
    let span = tracing::Span::current();
    let result = std::thread::Builder::new()
        .name("lastdb-cloud-purge".into())
        .spawn(move || {
            span.in_scope(|| worker(&host, &id, &stop, &cancel));
            if let Ok(mut runtimes) = runtimes().lock() {
                if let Some(runtime) = runtimes.get_mut(&host.home) {
                    runtime.running = None;
                }
            }
        });
    if let Err(error) = result {
        if let Ok(mut runtimes) = runtimes().lock() {
            if let Some(runtime) = runtimes.get_mut(&home) {
                runtime.running = None;
            }
        }
        return Err(format!("start cloud purge worker: {error}"));
    }
    Ok(())
}

pub(crate) fn cached_engine(host: &Host) -> Option<Arc<fold_db::sync::SyncEngine>> {
    runtimes().lock().ok()?.get(&host.home)?.engine.clone()
}

pub(crate) fn local_writes_paused(host: &Host) -> Result<bool, String> {
    Ok(runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .get(&host.home)
        .is_some_and(|runtime| runtime.running.is_some() || runtime.pause_operation.is_some()))
}

pub(crate) fn cache_engine(
    host: &Host,
    engine: Arc<fold_db::sync::SyncEngine>,
) -> Result<(), String> {
    let mut entries = runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?;
    let Some(runtime) = entries.get_mut(&host.home) else {
        return Ok(());
    };
    let owner = runtime.host.upgrade().ok_or("cloud purge host closed")?;
    crate::cloud_purge_adoption::install(&owner, &engine, Arc::clone(&runtime.stop))
        .map_err(|error| error.to_string())?;
    runtime.engine = Some(engine);
    Ok(())
}

pub(crate) fn has_write_fence(host: &Host, id: &str) -> Result<bool, String> {
    Ok(runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .get(&host.home)
        .and_then(|runtime| runtime.fence.as_ref())
        .is_some_and(|(owner, _)| owner == id))
}

pub(crate) fn write_fence(
    host: &Host,
    id: &str,
) -> Result<Option<Arc<fold_db::sync::engine::CloudPurgeWriteFence>>, String> {
    Ok(runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .get(&host.home)
        .and_then(|runtime| runtime.fence.as_ref())
        .filter(|(owner, _)| owner == id)
        .map(|(_, fence)| Arc::clone(fence)))
}

pub(super) async fn ensure_fence(
    host: &Host,
    id: &str,
) -> Result<Arc<fold_db::sync::engine::CloudPurgeWriteFence>, String> {
    if let Some((owner, fence)) = runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .get(&host.home)
        .and_then(|runtime| runtime.fence.clone())
    {
        if owner != id {
            return Err("another purge owns the selected write fence".into());
        }
        return Ok(fence);
    }
    let engine = crate::cloud_purge_job::source_engine(host).await?;
    let fence = Arc::new(
        engine
            .acquire_cloud_purge_write_fence()
            .await
            .map_err(|error| error.to_string())?,
    );
    let mut entries = runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?;
    let runtime = entries
        .get_mut(&host.home)
        .ok_or("cloud purge runtime is not attached")?;
    runtime.fence = Some((id.into(), Arc::clone(&fence)));
    runtime.pause_operation = Some(id.into());
    Ok(fence)
}

pub(super) fn release_fence(host: &Host, id: &str) -> Result<(), String> {
    let mut entries = runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?;
    if let Some(runtime) = entries.get_mut(&host.home) {
        if runtime.fence.as_ref().is_some_and(|(owner, _)| owner == id) {
            runtime.fence = None;
        }
        if runtime.pause_operation.as_deref() == Some(id) {
            runtime.pause_operation = None;
        }
    }
    Ok(())
}

pub fn release_for_shutdown(home: &Path) -> Result<(), String> {
    let mut entries = runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?;
    if let Some(runtime) = entries.get_mut(home) {
        if runtime.running.is_some() {
            return Err("cloud purge worker is still active at shutdown".into());
        }
        // Owner routes stay paused until this process exits. The next boot reacquires the guard.
        runtime.fence = None;
    }
    Ok(())
}

/// A clean shutdown receipt must follow the selected-device worker's last write.
pub async fn drain(home: &Path) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
    loop {
        let active = runtimes()
            .lock()
            .map_err(|_| "cloud purge runtime lock failed")?
            .get(home)
            .is_some_and(|runtime| runtime.running.is_some());
        if !active {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("cloud purge worker did not drain within 180 seconds".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
