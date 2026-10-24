//! Host-owned admission and completion. Publication follows release of worker resources.
use super::{error::PauseState, BackupError, Result};
use cloud_backup_protocol::Id;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard,
};
use std::thread::JoinHandle;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOrigin {
    Manual,
    Auto { consent_revision: u64 },
}
#[derive(Clone, Debug)]
pub struct Invocation {
    pub request_id: Id,
    pub token: u64,
    pub origin: RunOrigin,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationOutcome {
    Complete,
    Paused(PauseState),
}
#[derive(Clone, Debug)]
pub struct Completion {
    pub invocation: Invocation,
    pub outcome: InvocationOutcome,
}
#[derive(Clone, Debug, Default)]
pub struct RuntimeSnapshot {
    pub active: Option<Invocation>,
    pub last_completion: Option<Completion>,
    pub wake_revision: u64,
    pub shutting_down: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainError {
    Timeout,
    WorkerPanic,
}
impl std::fmt::Display for DrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Timeout => "native backup drain deadline expired",
            Self::WorkerPanic => "native backup worker panicked",
        })
    }
}
impl std::error::Error for DrainError {}

struct State {
    next_token: u64,
    pause: Option<Arc<AtomicBool>>,
    thread: Option<JoinHandle<()>>,
    panicked: bool,
}
pub struct BackupRuntime {
    instance_id: uuid::Uuid,
    transition: Mutex<()>,
    state: Mutex<State>,
    changed: watch::Sender<RuntimeSnapshot>,
}
impl Default for BackupRuntime {
    fn default() -> Self {
        Self::new()
    }
}
impl BackupRuntime {
    pub fn new() -> Self {
        let (changed, _) = watch::channel(RuntimeSnapshot::default());
        Self {
            instance_id: uuid::Uuid::new_v4(),
            transition: Mutex::new(()),
            state: Mutex::new(State {
                next_token: 0,
                pause: None,
                thread: None,
                panicked: false,
            }),
            changed,
        }
    }
    pub fn instance_id(&self) -> uuid::Uuid {
        self.instance_id
    }
    pub fn transition(&self) -> Result<MutexGuard<'_, ()>> {
        self.transition.lock().map_err(|_| BackupError::WorkerPanic)
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.changed.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<RuntimeSnapshot> {
        self.changed.subscribe()
    }
    pub fn wake(&self) {
        self.changed
            .send_modify(|state| state.wake_revision = state.wake_revision.wrapping_add(1));
    }
    /// Called while the owner transition lock protects consent and admission.
    pub(super) fn check_admission(&self) -> Result<()> {
        if self.changed.borrow().shutting_down {
            Err(BackupError::Shutdown)
        } else {
            Ok(())
        }
    }
    pub(super) fn start<F>(
        self: &Arc<Self>,
        request_id: Id,
        origin: RunOrigin,
        run: F,
    ) -> Result<()>
    where
        F: FnOnce(Arc<AtomicBool>) -> InvocationOutcome + Send + 'static,
    {
        self.check_admission()?;
        let mut state = self.state.lock().map_err(|_| BackupError::WorkerPanic)?;
        if self.changed.borrow().active.is_some() {
            return Err(BackupError::Busy);
        }
        if let Some(previous) = state.thread.take() {
            state.panicked |= previous.join().is_err();
        }
        state.next_token = state.next_token.checked_add(1).ok_or(BackupError::Limit)?;
        let invocation = Invocation {
            request_id,
            token: state.next_token,
            origin,
        };
        let pause = Arc::new(AtomicBool::new(false));
        let worker_pause = Arc::clone(&pause);
        let runtime = Arc::clone(self);
        let worker_invocation = invocation.clone();
        let thread = std::thread::Builder::new()
            .name("lastdb-native-backup".into())
            .spawn(move || {
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(worker_pause)))
                        .unwrap_or_else(|_| {
                            InvocationOutcome::Paused(BackupError::WorkerPanic.pause_state())
                        });
                // The run closure owns and drops the client, runtime, file lock and source
                // before another invocation can enter or the controller sees completion.
                runtime.finish(worker_invocation, outcome);
            })
            .map_err(|_| BackupError::Source)?;
        state.pause = Some(pause);
        state.thread = Some(thread);
        self.changed
            .send_modify(|snapshot| snapshot.active = Some(invocation));
        Ok(())
    }
    fn finish(&self, invocation: Invocation, outcome: InvocationOutcome) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pause = None;
        state.panicked |= matches!(
            outcome,
            InvocationOutcome::Paused(PauseState {
                reason: super::error::PauseReason::WorkerPanic,
                ..
            })
        );
        self.changed.send_modify(|snapshot| {
            snapshot.active = None;
            snapshot.last_completion = Some(Completion {
                invocation,
                outcome,
            });
        });
    }
    pub fn begin_shutdown(&self) {
        let _transition = self
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pause) = &state.pause {
            pause.store(true, Ordering::Release);
        }
        self.changed
            .send_modify(|snapshot| snapshot.shutting_down = true);
    }
    pub async fn drain(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), DrainError> {
        self.begin_shutdown();
        let mut changes = self.subscribe();
        loop {
            if changes.borrow_and_update().active.is_none() {
                break;
            }
            tokio::time::timeout_at(deadline, changes.changed())
                .await
                .map_err(|_| DrainError::Timeout)?
                .map_err(|_| DrainError::WorkerPanic)?;
        }
        let mut state = self.state.lock().map_err(|_| DrainError::WorkerPanic)?;
        if let Some(thread) = state.thread.take() {
            state.panicked |= thread.join().is_err();
        }
        if state.panicked {
            Err(DrainError::WorkerPanic)
        } else {
            Ok(())
        }
    }
}
