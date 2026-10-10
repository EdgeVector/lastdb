//! Join the abort-path cloud pause restores before a final cloud sync.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;

// lastdbd owns one FoldDB per process. A library process with multiple FoldDBs
// conservatively joins every outstanding restore before either one syncs.
static RESTORES: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
// A failed Drop cannot prove that the pause cleared. Keep this sticky so no
// later shutdown in the same process can publish a clean receipt.
static RESTORE_FAILED: AtomicBool = AtomicBool::new(false);

pub(crate) fn track(handle: JoinHandle<()>) {
    RESTORES.lock().unwrap().push(handle);
}

pub(crate) fn mark_failed() {
    RESTORE_FAILED.store(true, Ordering::Release);
}

pub(crate) async fn drain(deadline: Instant) -> Result<(), String> {
    loop {
        let handles = std::mem::take(&mut *RESTORES.lock().unwrap());
        if handles.is_empty() {
            break;
        }
        for handle in handles {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(format!("cloud pause restore failed: {error}")),
                Err(_) => return Err("cloud pause restore did not finish in time".to_string()),
            }
        }
    }
    if RESTORE_FAILED.load(Ordering::Acquire) {
        return Err("cloud pause restore failed without a runtime".to_string());
    }
    Ok(())
}
