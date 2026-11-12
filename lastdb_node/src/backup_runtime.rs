//! Backup admission, worker lifecycle, resource-drained completion, and typed errors.
use crate::cloud_backup_client::byte_backup::BackupTransportError;
use cloud_backup_protocol::{ErrorCode, Id, ProtocolError};
use fold_db::cloud_backup_plan::PlanError;
use fold_db::storage::error::{NativeProofCause, StorageError};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard, PoisonError,
};
use std::thread::JoinHandle;
use tokio::sync::watch;

pub type Result<T> = std::result::Result<T, BackupError>;

const WAIT: &str = "native backup awaits cloud verification; resume this job";
const FAILED: &str = "native backup cloud request failed; resume this job";
const EDIT_WAIT: &str = "share publication is waiting for an unfinished edit";
const QUOTA_MSG: &str = "native backup cloud quota reached; the saved job retains its source";
const REFUSED_MSG: &str =
    "native backup cloud request was refused; the saved job retains its source";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Capturing,
    Uploading,
    Inventory,
    Committing,
    Published,
    Closed,
}

#[derive(Clone, Debug)]
pub struct Invocation {
    pub request_id: Id,
    pub token: u64,
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

#[derive(Default)]
struct State {
    pause: Option<Arc<AtomicBool>>,
    thread: Option<JoinHandle<()>>,
    panicked: bool,
}

fn lock_unpoisoned<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

pub struct BackupRuntime {
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
        Self {
            transition: Mutex::new(()),
            state: Mutex::new(State::default()),
            changed: watch::channel(RuntimeSnapshot::default()).0,
        }
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
        self.changed.send_modify(|_| {});
    }

    pub fn begin_shutdown(&self) {
        let _t = lock_unpoisoned(&self.transition);
        if let Some(p) = &lock_unpoisoned(&self.state).pause {
            p.store(true, Ordering::Release);
        }
        self.changed.send_modify(|s| s.shutting_down = true);
    }

    pub async fn drain(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), DrainError> {
        self.begin_shutdown();
        let mut changes = self.subscribe();
        while changes.borrow_and_update().active.is_some() {
            tokio::time::timeout_at(deadline, changes.changed())
                .await
                .map_err(|_| DrainError::Timeout)?
                .map_err(|_| DrainError::WorkerPanic)?;
        }
        let mut state = self.state.lock().map_err(|_| DrainError::WorkerPanic)?;
        state.join_prev();
        (!state.panicked)
            .then_some(())
            .ok_or(DrainError::WorkerPanic)
    }
}

impl State {
    fn join_prev(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.panicked |= thread.join().is_err();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Retry,
    Attention,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Temporary,
    Disabled,
    Busy,
    Missing,
    Identity,
    Invalid,
    Disk,
    Source,
    Unsupported,
    ResourceLimit,
    Quota,
    Credentials,
    Authority,
    Refused,
    Closing,
    Shutdown,
    WorkerPanic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseState {
    pub disposition: Disposition,
    pub reason: PauseReason,
}

#[derive(Debug)]
pub enum BackupError {
    Disabled,
    Busy,
    Missing,
    Identity,
    Invalid,
    Disk,
    Source,
    Limit,
    Waiting,
    Compacting,
    AdmissionPending,
    Closing,
    Shutdown,
    WorkerPanic,
    Authority,
    PublicationFile,
    PublicationEditPending,
    Cloud(BackupTransportError),
}

impl BackupError {
    pub fn pause_state(&self) -> PauseState {
        let reason = classify(self).0;
        let disposition = if matches!(
            reason,
            PauseReason::Temporary | PauseReason::Busy | PauseReason::Shutdown
        ) {
            Disposition::Retry
        } else {
            Disposition::Attention
        };
        PauseState {
            disposition,
            reason,
        }
    }
}

fn classify(err: &BackupError) -> (PauseReason, &'static str) {
    use {BackupError as E, PauseReason::*};
    match err {
        E::Disabled => (Disabled, "native backup awaits release activation"),
        E::Busy => (Busy, "a native backup job is active"),
        E::Missing => (Missing, "no saved native backup job exists"),
        E::Identity => (Identity, "native backup account or home differs"),
        E::Invalid => (Invalid, "native backup saved state differs"),
        E::Disk => (Disk, "native backup disk operation failed"),
        E::Source => (Source, "native backup source needs attention"),
        E::Limit => (ResourceLimit, "native backup resource limit reached"),
        E::Waiting => (Temporary, WAIT),
        E::Compacting => (
            Temporary,
            "native backup awaits snapshot compaction; resume this job",
        ),
        E::AdmissionPending => (Temporary, "automatic native backup awaits cloud admission"),
        E::Closing => (Closing, "native backup closure is requested"),
        E::Shutdown => (Shutdown, "native backup paused for daemon shutdown"),
        E::WorkerPanic => (WorkerPanic, "native backup worker needs attention"),
        E::Authority => (Authority, "native backup cloud authority needs attention"),
        E::PublicationFile => (Unsupported, "share publication supports inline files only"),
        E::PublicationEditPending => (Temporary, EDIT_WAIT),
        E::Cloud(e) => cloud_info(e),
    }
}

fn cloud_info(e: &BackupTransportError) -> (PauseReason, &'static str) {
    use {BackupTransportError as T, ErrorCode::*, PauseReason as P, PauseReason::*};
    match e {
        T::Authentication => (Credentials, "native backup credentials were refused"),
        T::Integrity(_) => (
            Identity,
            "native backup upload bytes differ from the saved input",
        ),
        T::InvalidResponse(_) => (Identity, FAILED),
        T::Configuration(_) => (Invalid, FAILED),
        T::Source | T::Task => (Source, FAILED),
        T::Transport | T::Timeout | T::Http(408 | 429 | 500..) => (Temporary, FAILED),
        T::Http(_) => (Refused, FAILED),
        T::Protocol(p) => match p.code {
            QuotaExceeded => (Quota, QUOTA_MSG),
            ErrorCode::ResourceLimit => (
                P::ResourceLimit,
                "native backup cloud resource limit reached",
            ),
            Unauthorized | GenerationConflict | StreamAuthorityConflict | PredecessorConflict => (
                Authority,
                "native backup cloud authority changed; close or review this job",
            ),
            IntegrityMismatch | IdentityConflict | RecoveryKeyMismatch => (
                Identity,
                "native backup cloud identity or byte proof differs",
            ),
            ProviderPending | OutcomeUnknown | RetryLater => (Temporary, WAIT),
            UnsupportedCapability => (Unsupported, "native backup cloud capability is unsupported"),
            _ => (Refused, REFUSED_MSG),
        },
    }
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(classify(self).1)
    }
}
impl std::error::Error for BackupError {}
macro_rules! from_err {
    ($($t:ty => $f:expr),+) => {
        $(impl From<$t> for BackupError { fn from(v: $t) -> Self { ($f)(v) } })+
    };
}
from_err!(BackupTransportError => Self::Cloud, std::io::Error => |_| Self::Disk, ProtocolError => |_| Self::Invalid);
impl From<PlanError> for BackupError {
    fn from(e: PlanError) -> Self {
        match e {
            PlanError::SourceLength => Self::Identity,
            PlanError::ExtentBound => Self::Limit,
            PlanError::Invalid(_) | PlanError::Protocol(_) => Self::Invalid,
        }
    }
}
impl From<StorageError> for BackupError {
    fn from(e: StorageError) -> Self {
        use {NativeProofCause as P, StorageError as S};
        match e {
            S::NativeProof { cause, .. } => match cause {
                P::ReadResource | P::WriteResource => Self::Limit,
                P::Io { .. } | P::StorageFull => Self::Disk,
                _ => Self::Source,
            },
            S::ReadResourceLimit { .. }
            | S::WriteResourceLimit { .. }
            | S::RecordTooLarge { .. }
            | S::MutationLimit { .. } => Self::Limit,
            S::StorageFull { .. } | S::IoError(_) => Self::Disk,
            S::SnapshotCompactionPending => Self::Compacting,
            S::PublicationEditPending => Self::PublicationEditPending,
            S::PublicationFile => Self::PublicationFile,
            _ => Self::Source,
        }
    }
}
