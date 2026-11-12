//! Bounded polling and verified refresh for a daemon's received databases.
use crate::share_cache::{ShareCache, ShareCacheError};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::Instrument;

const POLL: Duration = Duration::from_secs(5);
const SHUTDOWN_CHECK: Duration = Duration::from_millis(250);

/// Start one worker. All filesystem verification and replay stay on the
/// blocking pool so shared cache work cannot occupy an async request worker.
pub fn spawn(home: PathBuf, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_failure = None;
        while !shutdown.load(Ordering::Acquire) {
            let runtime = Handle::current();
            let home = home.clone();
            let stopping = Arc::clone(&shutdown);
            let outcome =
                tokio::task::spawn_blocking(move || refresh(&runtime, &home, &stopping)).await;
            let failure = match outcome {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("received Share refresh task failed".to_string()),
            };
            if let Some(message) = &failure {
                if last_failure.as_ref() != Some(message) {
                    tracing::warn!(target: "lastdbd::received_shares", error = %message, "Share refresh pending");
                }
            }
            last_failure = failure;
            tokio::time::sleep(POLL).await;
        }
    }.in_current_span())
}

fn refresh(runtime: &Handle, home: &Path, shutdown: &AtomicBool) -> Result<(), ShareCacheError> {
    let Some(cache) = ShareCache::open_managed(home, false)? else {
        return Ok(());
    };
    let mut result = Ok(());
    for id in cache.managed_ids()? {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let entry = match cache.managed_record(home, &id) {
            Ok(Some(entry)) => entry,
            Ok(None) => continue,
            Err(error) => {
                result = Err(error);
                continue;
            }
        };
        if entry.signed_log || entry.expires_ms <= fold_db::clock::unix_millis() {
            continue;
        }
        let outcome = runtime.block_on(async {
            tokio::select! {
                value = cache.refresh(&id) => Some(value),
                () = stopping(shutdown) => None,
            }
        });
        match outcome {
            None => break,
            Some(Ok(_) | Err(ShareCacheError::Busy | ShareCacheError::Unavailable)) => {}
            Some(Err(error)) => result = Err(error),
        }
    }
    result
}

async fn stopping(shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(SHUTDOWN_CHECK).await;
    }
}
