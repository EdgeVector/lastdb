//! The one lane that uploads a snapshot while sync turns go on.
//!
//! A snapshot uploads tens of megabytes. Done inside a turn, it holds back every entry and
//! every acknowledgement for as long as the upload takes. A turn instead freezes the files at
//! the one instant when the spool is acknowledged (see `snapshot::run`) and hands the rest, the
//! hashing, the file uploads, the `snapshot` entry and the trim, to this lane. The lane is a
//! task that the turn starts and that ends when its snapshot is done. There is at most one, so
//! there is one snapshot at a time. It has no timer and no queue.

use std::future::Future;
use std::sync::Mutex;

use tokio::task::JoinHandle;

static LANE: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

/// True while a snapshot is uploading.
pub(super) fn busy() -> bool {
    lock().as_ref().is_some_and(|task| !task.is_finished())
}

/// Start the lane with `work`. The caller checked [`busy`] first.
pub(super) fn start(work: impl Future<Output = ()> + Send + 'static) {
    *lock() = Some(tokio::spawn(work));
}

/// End the lane and wait until it is gone. The daemon calls this when it stops. Everything the
/// lane has not yet written stays unwritten: the files it uploaded are on the relay and are
/// skipped the next time, the request file stays, and the next start takes the snapshot again.
/// The lane has no await between the append of the `snapshot` entry and the write of its
/// records, so it never stops in between.
pub(crate) async fn stop() {
    let task = lock().take();
    if let Some(task) = task {
        task.abort();
        let _ = task.await;
    }
}

fn lock() -> std::sync::MutexGuard<'static, Option<JoinHandle<()>>> {
    LANE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
