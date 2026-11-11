//! Reconcile saved keyed queries after a source store commits.
//!
//! `PackRuntime::subscribe_durable_writes` is the watch the publication-home
//! automatic publisher already uses. That publisher runs in a different
//! process, on a legacy publication home, and uploads cloud recovery points.
//! It does not see saved queries. This task runs in the owner daemon, which
//! holds `personal` and the owned siblings. The watch carries a counter, not
//! keys. Each wake reads one catalog record and reconciles only those saved
//! queries through the ordinary owner routes. A second pass writes nothing
//! when the ledger is unchanged, so the ledger write does not loop.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fold_db::fold_db_core::FoldDB;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::host::Host;

const PERSONAL: &str = "personal";

/// Follow source commits. `follow` is false for a publication-home daemon:
/// that process's database is not a saved-query source.
pub fn spawn(
    host: Arc<Host>,
    socket: PathBuf,
    shutdown: Arc<AtomicBool>,
    follow: bool,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            if follow {
                supervise(host, socket, shutdown).await;
            } else {
                wait_until_stopped(&shutdown).await;
            }
        }
        .in_current_span(),
    )
}

async fn supervise(host: Arc<Host>, socket: PathBuf, shutdown: Arc<AtomicBool>) {
    let mut registry = host.owned.subscribe_stores();
    let mut running = BTreeMap::new();
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        if let Err(error) = sync_sources(&host, &socket, &shutdown, &mut running) {
            tracing::warn!(
                target: "lastdbd::saved_query",
                error = %error,
                "saved query sources were not updated"
            );
        }
        tokio::select! {
            changed = registry.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
    for (_, task) in running {
        let _ = task.await;
    }
}

fn sync_sources(
    host: &Host,
    socket: &Path,
    shutdown: &Arc<AtomicBool>,
    running: &mut BTreeMap<String, JoinHandle<()>>,
) -> Result<(), String> {
    let owned = host
        .owned
        .stores()
        .map_err(|_| "database registry is unavailable".to_owned())?;
    let mut want = BTreeSet::new();
    want.insert(PERSONAL.to_owned());
    ensure_follower(socket, shutdown, running, PERSONAL, Arc::clone(&host.db));
    for (id, db) in owned {
        want.insert(id.clone());
        ensure_follower(socket, shutdown, running, id, db);
    }
    let stale: Vec<String> = running
        .keys()
        .filter(|key| !want.contains(*key))
        .cloned()
        .collect();
    for key in stale {
        if let Some(task) = running.remove(&key) {
            task.abort();
        }
    }
    Ok(())
}

fn ensure_follower(
    socket: &Path,
    shutdown: &Arc<AtomicBool>,
    running: &mut BTreeMap<String, JoinHandle<()>>,
    selector: impl Into<String>,
    db: Arc<FoldDB>,
) {
    let selector = selector.into();
    if running.contains_key(&selector) {
        return;
    }
    let socket = socket.to_path_buf();
    let shutdown = Arc::clone(shutdown);
    let key = selector.clone();
    let task = tokio::spawn(
        async move {
            follow(socket, selector, db, shutdown).await;
        }
        .in_current_span(),
    );
    running.insert(key, task);
}

async fn follow(socket: PathBuf, selector: String, db: Arc<FoldDB>, shutdown: Arc<AtomicBool>) {
    let mut writes = db.db_ops().native_runtime().subscribe_durable_writes();
    if !wait_until_accepting(&socket, &shutdown).await {
        return;
    }
    while !shutdown.load(Ordering::Acquire) {
        run_once(&socket, &selector).await;
        if !wait_for_change(&mut writes, &shutdown).await {
            break;
        }
    }
}

async fn run_once(socket: &std::path::Path, selector: &str) {
    let socket = socket.to_path_buf();
    let selector = selector.to_owned();
    let joined = tokio::task::spawn_blocking(move || {
        crate::saved_query_run::reconcile_catalog(&socket, &selector)
    })
    .await;
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(
            target: "lastdbd::saved_query",
            error = %error,
            "saved query reconcile failed"
        ),
        Err(_) => {
            tracing::warn!(target: "lastdbd::saved_query", "saved query reconcile stopped");
        }
    }
}

async fn wait_for_change(writes: &mut watch::Receiver<u64>, shutdown: &AtomicBool) -> bool {
    loop {
        tokio::select! {
            changed = writes.changed() => return changed.is_ok(),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if shutdown.load(Ordering::Acquire) {
                    return false;
                }
            }
        }
    }
}

async fn wait_until_accepting(socket: &std::path::Path, shutdown: &AtomicBool) -> bool {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return false;
        }
        let path = socket.to_path_buf();
        let connected = tokio::task::spawn_blocking(move || {
            std::os::unix::net::UnixStream::connect(&path).is_ok()
        })
        .await
        .unwrap_or(false);
        if connected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_until_stopped(shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
