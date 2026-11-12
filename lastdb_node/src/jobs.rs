//! Host jobs: background and on-demand work over a booted [`Host`](crate::host::Host).

pub mod ttl_sweep;

/// Reconcile saved keyed queries after a source store commits.
pub mod saved_query {
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

    /// Follow source commits (`follow` is false for a publication-home daemon).
    pub fn spawn(
        host: Arc<Host>,
        socket: PathBuf,
        shutdown: Arc<AtomicBool>,
        follow: bool,
    ) -> JoinHandle<()> {
        tokio::spawn(
            async move {
                if follow {
                    supervise(&host, &socket, &shutdown).await;
                } else {
                    while !shutdown.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
            .in_current_span(),
        )
    }

    async fn supervise(host: &Host, socket: &Path, shutdown: &Arc<AtomicBool>) {
        let mut registry = host.owned.subscribe_stores();
        let mut running = BTreeMap::new();
        while !shutdown.load(Ordering::Acquire) {
            if let Err(error) = sync_sources(host, socket, shutdown, &mut running) {
                tracing::warn!(
                    target: "lastdbd::saved_query", error = %error,
                    "saved query sources were not updated"
                );
            }
            tokio::select! {
                changed = registry.changed() => if changed.is_err() { break; },
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
        let mut want = BTreeSet::from([PERSONAL.to_owned()]);
        ensure_follower(
            socket,
            shutdown,
            running,
            PERSONAL.to_owned(),
            Arc::clone(&host.db),
        );
        for (id, db) in owned {
            want.insert(id.clone());
            ensure_follower(socket, shutdown, running, id, db);
        }
        running.retain(|key, task| {
            if want.contains(key) {
                true
            } else {
                task.abort();
                false
            }
        });
        Ok(())
    }

    fn ensure_follower(
        socket: &Path,
        shutdown: &Arc<AtomicBool>,
        running: &mut BTreeMap<String, JoinHandle<()>>,
        selector: String,
        db: Arc<FoldDB>,
    ) {
        if running.contains_key(&selector) {
            return;
        }
        let (socket, shutdown, key) =
            (socket.to_path_buf(), Arc::clone(shutdown), selector.clone());
        let task = tokio::spawn(follow(socket, selector, db, shutdown).in_current_span());
        running.insert(key, task);
    }

    async fn follow(socket: PathBuf, selector: String, db: Arc<FoldDB>, shutdown: Arc<AtomicBool>) {
        let mut writes = db.db_ops().native_runtime().subscribe_durable_writes();
        loop {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            let path = socket.clone();
            if tokio::task::spawn_blocking(move || {
                std::os::unix::net::UnixStream::connect(path).is_ok()
            })
            .await
            .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        while !shutdown.load(Ordering::Acquire) {
            let (sock, sel) = (socket.clone(), selector.clone());
            match tokio::task::spawn_blocking(move || {
                crate::saved_query_run::reconcile_catalog(&sock, &sel)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(target: "lastdbd::saved_query", error = %error, "saved query reconcile failed");
                }
                Err(_) => {
                    tracing::warn!(target: "lastdbd::saved_query", "saved query reconcile stopped");
                }
            }
            if !wait_for_change(&mut writes, &shutdown).await {
                break;
            }
        }
    }

    async fn wait_for_change(writes: &mut watch::Receiver<u64>, shutdown: &AtomicBool) -> bool {
        loop {
            tokio::select! {
                changed = writes.changed() => return changed.is_ok(),
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    if shutdown.load(Ordering::Acquire) { return false; }
                }
            }
        }
    }
}
