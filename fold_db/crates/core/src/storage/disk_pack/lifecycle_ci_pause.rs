//! One-time CI publication latch. This module and its callers must not merge.

use super::PackRuntime;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

static ACTIVE: Mutex<Option<(uuid::Uuid, Arc<Pause>)>> = Mutex::new(None);

pub(crate) struct Pause {
    armed: AtomicBool,
    pub(crate) reached: Notify,
    pub(crate) resume: Notify,
    pub(crate) cleanup_blocked: Notify,
}

pub(crate) fn arm(runtime: &PackRuntime) -> Arc<Pause> {
    let pause = Arc::new(Pause {
        armed: AtomicBool::new(true),
        reached: Notify::new(),
        resume: Notify::new(),
        cleanup_blocked: Notify::new(),
    });
    *ACTIVE.lock().expect("CI pause") = Some((runtime.home_identity(), Arc::clone(&pause)));
    pause
}

pub(crate) async fn before_publication(runtime: &PackRuntime) {
    let pause = current(runtime).filter(|pause| pause.armed.swap(false, Ordering::AcqRel));
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

pub(crate) fn note_cleanup_blocked(runtime: &PackRuntime) {
    if let Some(pause) = current(runtime) {
        pause.cleanup_blocked.notify_one();
    }
}

fn current(runtime: &PackRuntime) -> Option<Arc<Pause>> {
    ACTIVE
        .lock()
        .expect("CI pause")
        .as_ref()
        .filter(|(identity, _)| *identity == runtime.home_identity())
        .map(|(_, pause)| Arc::clone(pause))
}
