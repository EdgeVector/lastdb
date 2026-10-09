//! Bounded post-commit mutation-log capture for the serving store.
//!
//! The request reserves queue capacity before the local commit. The local store
//! then commits and returns without waiting for marker or pin-log storage. One
//! worker preserves capture order. Queue admission failure rejects the request
//! before the local commit.

use crate::fold_db_core::mutation_manager::{
    AuthorClockPersistBarrier, CloudCapturePolicy, CloudCaptureState, CloudMutationReceipt,
    CloudPublicationState, CloudPublicationTarget,
};
use crate::fold_db_core::pending_task_tracker::{PendingTask, PendingTaskTracker};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::traits::{
    ExecutionModel, FlushBehavior, KvMutation, KvStore, NamespacedStore, PartitionedScan,
    PhysicalScanCursor, PhysicalScanPage, ReadCostStats,
};
use crate::sync::engine::CaptureMode;
use crate::sync::log::MutationEnvelope;
use crate::sync::{policy, SyncEngine, SyncError};
use async_trait::async_trait;
use std::cell::RefCell;
use std::future::Future;
use std::sync::{Arc, OnceLock, RwLock};

mod kv_store;
mod logical_commit;
mod namespaced;
mod queue;

use kv_store::*;
pub(crate) use logical_commit::*;
pub(crate) use namespaced::MutationLogCaptureNamespacedStore;
use queue::*;

const CAPTURE_QUEUE_CAPACITY: usize = 1024;
const CAPTURE_QUEUE_ADMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

fn elapsed_micros(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[derive(Default)]
struct LogicalCommitScope {
    /// When true, KvStore wrappers must not clone values or record_put.
    suppress_kv: bool,
    /// When false, do not append a MutationIntent after the future (replay).
    record_intent: bool,
    /// Set once resident tips are already visible. An Err after that must
    /// still stage capture; an Err before it must not.
    published: bool,
    envelopes: Vec<MutationEnvelope>,
}

/// Record that the in-scope local write already published resident tips.
///
/// Capture wraps the whole persist-wait / flush / mixed-erase tail. Those
/// tails can still return Err after RAM already shows the new tips. This
/// flag is what lets the wrapper stage `MutationIntent` on that path.
pub(crate) fn mark_logical_commit_published() {
    let _ = LOGICAL_COMMIT_SCOPE.try_with(|slot| {
        slot.borrow_mut().published = true;
    });
}

tokio::task_local! {
    static LOGICAL_COMMIT_SCOPE: RefCell<LogicalCommitScope>;
}

fn leftover_body_digest(value: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(value).into()
}

fn task_local_suppressed() -> bool {
    LOGICAL_COMMIT_SCOPE
        .try_with(|slot| slot.borrow().suppress_kv)
        .unwrap_or(false)
}

fn in_suppressed_capture(router: Option<&MutationLogCaptureRouter>) -> bool {
    if task_local_suppressed() {
        return true;
    }
    router.is_some_and(MutationLogCaptureRouter::is_kv_suppressed)
}

/// Engine slot shared by every namespace handle. It is empty while Cloud Sync
/// is off and can be populated when credentials enable sync after boot.
pub(crate) struct MutationLogCaptureRouter {
    engine: RwLock<Option<Arc<SyncEngine>>>,
    activation_gate: Arc<tokio::sync::RwLock<()>>,
    sender: OnceLock<tokio::sync::mpsc::Sender<CaptureJob>>,
    pending_tasks: Arc<PendingTaskTracker>,
    queue_capacity: usize,
    /// Leftover KvStore capture suppress depth for this serving store.
    ///
    /// `LOGICAL_COMMIT_SCOPE` is a tokio task-local. LastStore persist on a
    /// current-thread runtime runs the serving put on `spawn_blocking`, which
    /// is a different task and does not inherit that local. Depth on the
    /// router is visible there without coupling parallel tests that own a
    /// different router.
    kv_suppress_depth: std::sync::atomic::AtomicU32,
}

pub(crate) struct RouterKvSuppressGuard {
    router: Arc<MutationLogCaptureRouter>,
}

impl Drop for RouterKvSuppressGuard {
    fn drop(&mut self) {
        self.router
            .kv_suppress_depth
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl MutationLogCaptureRouter {
    /// Hold new logical and direct KV mutations outside one snapshot cut.
    pub(crate) async fn fence_mutations(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.activation_gate).write_owned().await
    }

    async fn enter_mutation(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        Arc::clone(&self.activation_gate).read_owned().await
    }

    /// Attach the engine only after every earlier local-only write finishes.
    /// The factory uses `set_engine` before it serves requests; live enable
    /// must use this fenced form. Keep related serving engine slots inside
    /// `attach_serving_slots`, so the first new writer sees them all attached.
    pub(crate) async fn set_engine_after_mutations<F>(
        &self,
        engine: Arc<SyncEngine>,
        attach_serving_slots: F,
    ) where
        F: FnOnce(),
    {
        let _activation_guard = self.activation_gate.write().await;
        attach_serving_slots();
        self.set_engine(engine);
    }

    pub(crate) fn enter_kv_suppress(self: &Arc<Self>) -> RouterKvSuppressGuard {
        self.kv_suppress_depth
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RouterKvSuppressGuard {
            router: Arc::clone(self),
        }
    }

    pub(crate) fn is_kv_suppressed(&self) -> bool {
        self.kv_suppress_depth
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    }

    pub(crate) fn set_engine(&self, engine: Arc<SyncEngine>) {
        *self
            .engine
            .write()
            .expect("mutation-log capture router poisoned") = Some(engine);
    }

    fn engine(&self) -> Option<Arc<SyncEngine>> {
        self.engine
            .read()
            .expect("mutation-log capture router poisoned")
            .clone()
    }

    fn sender(&self) -> StorageResult<tokio::sync::mpsc::Sender<CaptureJob>> {
        if let Some(sender) = self.sender.get() {
            return Ok(sender.clone());
        }
        tokio::runtime::Handle::try_current().map_err(|error| {
            StorageError::BackendError(format!(
                "mutation capture queue requires an async runtime: {error}"
            ))
        })?;
        let (sender, receiver) = tokio::sync::mpsc::channel(self.queue_capacity.max(1));
        if self.sender.set(sender.clone()).is_ok() {
            // lint:spawn-bare-ok process-lifetime capture queue — not request-scoped.
            tokio::spawn(run_capture_queue(receiver));
            return Ok(sender);
        }
        drop(receiver);
        Ok(self
            .sender
            .get()
            .expect("capture sender initialized by concurrent caller")
            .clone())
    }

    async fn reserve_capture(self: &Arc<Self>) -> StorageResult<Option<CaptureReservation>> {
        let Some(engine) = self.engine() else {
            return Ok(None);
        };
        let admission_started = std::time::Instant::now();
        let sender = match self.sender() {
            Ok(sender) => sender,
            Err(error) => {
                let elapsed_us = elapsed_micros(admission_started);
                engine
                    .capture_queue_admission_us
                    .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
                crate::request_phases::add_phase_us(
                    crate::request_phases::RequestPhase::SyncCapture,
                    elapsed_us,
                );
                return Err(error);
            }
        };
        let permit_result =
            tokio::time::timeout(CAPTURE_QUEUE_ADMISSION_TIMEOUT, sender.reserve_owned()).await;
        let elapsed_us = elapsed_micros(admission_started);
        engine
            .capture_queue_admission_us
            .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
        crate::request_phases::add_phase_us(
            crate::request_phases::RequestPhase::SyncCapture,
            elapsed_us,
        );
        let permit = permit_result
            .map_err(|_| {
                // Typed, not `BackendError`: this is transient backpressure that
                // clears itself, so the caller must see a retryable status
                // instead of "invalid data", and the node must not raise one
                // ERROR per refused write (Sentry issue 7699865707).
                engine
                    .capture_queue_rejections
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                StorageError::CaptureQueueFull {
                    waited_ms: u64::try_from(CAPTURE_QUEUE_ADMISSION_TIMEOUT.as_millis())
                        .unwrap_or(u64::MAX),
                }
            })?
            .map_err(|_| {
                StorageError::BackendError(
                    "mutation capture queue is closed; retry before local commit".to_string(),
                )
            })?;
        Ok(Some(CaptureReservation {
            permit,
            engine,
            pending_task: self.pending_tasks.begin(),
        }))
    }

    pub(crate) async fn wait_for_completion(&self, timeout: std::time::Duration) -> bool {
        self.pending_tasks.wait_for_completion(timeout).await
    }
}

impl Default for MutationLogCaptureRouter {
    fn default() -> Self {
        Self {
            engine: RwLock::new(None),
            activation_gate: Arc::new(tokio::sync::RwLock::new(())),
            sender: OnceLock::new(),
            pending_tasks: Arc::new(PendingTaskTracker::new()),
            queue_capacity: CAPTURE_QUEUE_CAPACITY,
            kv_suppress_depth: std::sync::atomic::AtomicU32::new(0),
        }
    }
}
