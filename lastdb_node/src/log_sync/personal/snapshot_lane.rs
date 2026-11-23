//! The one lane that uploads a snapshot while sync turns go on.
//!
//! A snapshot uploads tens of megabytes. Done inside a turn, it holds back every entry and
//! every acknowledgement for as long as the upload takes. A turn instead freezes the files at
//! the one instant when the spool is acknowledged (see `snapshot::run`) and hands the rest, the
//! hashing, the file uploads, the `snapshot` entry and the trim, to this lane. The lane is a
//! task that the turn starts and that ends when its snapshot is done. There is at most one, so
//! there is one snapshot at a time. It has no timer and no queue.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use tokio::task::JoinHandle;
use tracing::Instrument;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static LANES: LazyLock<Mutex<HashMap<PathBuf, (u64, JoinHandle<()>)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// True while any snapshot upload lane is running.
pub(super) fn busy() -> bool {
    lock().values().any(|(_, task)| !task.is_finished())
}

/// True while `home`'s snapshot upload lane is running.
pub(super) fn busy_for(home: &Path) -> bool {
    lock()
        .get(home)
        .is_some_and(|(_, task)| !task.is_finished())
}

/// Start the lane for `home` with `work`. The caller checked [`busy_for`] first.
pub(super) fn start_for(home: &Path, work: impl FnOnce(u64) -> PinBoxFuture) {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let task = tokio::spawn(work(id).in_current_span());
    lock().insert(home.to_path_buf(), (id, task));
}

pub(super) type PinBoxFuture = std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Mark lane `id` of `home` finished before waking the sync loop, so the woken turn never
/// observes its own lane as still busy.
pub(super) fn finish_for(home: &Path, id: u64) {
    let mut lanes = lock();
    if lanes.get(home).is_some_and(|(held, _)| *held == id) {
        lanes.remove(home);
    }
}

/// End every active lane and wait until it is gone. The daemon calls this when it stops.
pub(crate) async fn stop() {
    let tasks: Vec<JoinHandle<()>> = lock().drain().map(|(_, (_, task))| task).collect();
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<PathBuf, (u64, JoinHandle<()>)>> {
    LANES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
