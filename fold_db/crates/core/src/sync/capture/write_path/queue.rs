//! Capture queue jobs and the single worker that drains them in order.

use super::logical_intent::process_logical_intent;
use super::physical_jobs::{process_delete, process_mixed, process_physical_digest};
use super::*;

pub(super) struct CaptureReservation {
    pub(super) permit: tokio::sync::mpsc::OwnedPermit<CaptureJob>,
    pub(super) engine: Arc<SyncEngine>,
    pub(super) pending_task: PendingTask,
}

impl CaptureReservation {
    pub(super) fn submit(self, kind: CaptureJobKind) {
        let Self {
            permit,
            engine,
            pending_task,
        } = self;
        engine
            .capture_queue_jobs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _sender = permit.send(CaptureJob {
            engine,
            kind,
            queued_at: std::time::Instant::now(),
            _pending_task: pending_task,
            mutation_admission: current_mutation_admission(),
        });
    }
}

pub(super) struct CaptureJob {
    pub(super) engine: Arc<SyncEngine>,
    pub(super) kind: CaptureJobKind,
    pub(super) queued_at: std::time::Instant,
    pub(super) _pending_task: PendingTask,
    pub(super) mutation_admission: Option<MutationAdmission>,
}

pub(super) enum CaptureJobKind {
    LogicalIntent {
        envelopes: Vec<MutationEnvelope>,
        author_clock_barrier: Option<AuthorClockPersistBarrier>,
        record_now: bool,
        completion: Option<tokio::sync::oneshot::Sender<LogicalCaptureCompletion>>,
    },
    PhysicalDigest {
        namespace: String,
        keys: Vec<Vec<u8>>,
        digests: Vec<(Vec<u8>, [u8; 32])>,
    },
    /// Membership and schema authority bodies, with other rows still hashed.
    ApplyablePut {
        namespace: String,
        items: Vec<(Vec<u8>, Vec<u8>)>,
        digests: Vec<(Vec<u8>, [u8; 32])>,
    },
    ApplyableMixed {
        namespace: String,
        keys: Vec<Vec<u8>>,
        changes: Vec<crate::sync::log::LogicalChange>,
        digests: Vec<(Vec<u8>, [u8; 32])>,
    },
    Delete {
        namespace: String,
        keys: Vec<Vec<u8>>,
        batch: bool,
    },
    Mixed {
        namespace: String,
        keys: Vec<Vec<u8>>,
        digests: Vec<(Vec<u8>, [u8; 32])>,
        deletes: Vec<Vec<u8>>,
    },
}

pub(super) enum LogicalCaptureCompletion {
    MarkerDurable,
    MarkerFailed(String),
    Appended(crate::sync::engine::MutationLogAppendReceipt),
    AppendFailed(String),
}

pub(super) async fn run_capture_queue(mut receiver: tokio::sync::mpsc::Receiver<CaptureJob>) {
    while let Some(job) = receiver.recv().await {
        // Each job runs in its own supervised task. A bare `.await` here
        // would let a panic inside `process_capture_job` unwind straight
        // through this loop, dropping `receiver` and permanently killing the
        // sole consumer of the capture queue for the rest of the process —
        // every capture submitted afterward would sit in a channel nobody
        // reads and still get marked "done" when its PendingTask guard drops
        // on receiver teardown. Spawning isolates the panic in the join
        // handle so this loop keeps draining the queue.
        let engine = Arc::clone(&job.engine);
        let capture = with_existing_mutation_admission(
            job.mutation_admission.clone(),
            process_capture_job(job),
        );
        if let Err(join_error) = tokio::spawn(capture).await {
            if let Ok(panic) = join_error.try_into_panic() {
                let panics = engine
                    .capture_queue_worker_panics
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                tracing::error!(
                    capture_queue_worker_panics = panics,
                    "mutation-log capture job panicked: {}; the job's capture was lost \
                     but the queue worker keeps running so later captures are not \
                     silently dropped",
                    capture_job_panic_message(&panic),
                );
            }
        }
    }
}

pub(super) fn capture_job_panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

pub(super) async fn process_capture_job(job: CaptureJob) {
    let CaptureJob {
        engine,
        kind,
        queued_at,
        _pending_task,
        mutation_admission: _mutation_admission,
    } = job;
    engine.capture_queue_delay_us.fetch_add(
        elapsed_micros(queued_at),
        std::sync::atomic::Ordering::Relaxed,
    );
    match kind {
        CaptureJobKind::LogicalIntent {
            envelopes,
            author_clock_barrier,
            record_now,
            completion,
        } => {
            process_logical_intent(
                &engine,
                envelopes,
                author_clock_barrier,
                record_now,
                completion,
            )
            .await;
        }
        CaptureJobKind::PhysicalDigest {
            namespace,
            keys,
            digests,
        } => process_physical_digest(&engine, namespace, keys, digests).await,
        CaptureJobKind::ApplyablePut {
            namespace,
            items,
            digests,
        } => {
            catalog_capture::process_catalog_put(&engine, &namespace, &items, &digests).await;
        }
        CaptureJobKind::ApplyableMixed {
            namespace,
            keys,
            changes,
            digests,
        } => {
            let op = crate::sync::log::LogOp::LogicalCommit { changes };
            catalog_capture::process_catalog_capture(
                &engine,
                &namespace,
                &keys,
                op,
                &digests,
                "applyable_mixed",
            )
            .await;
        }
        CaptureJobKind::Delete {
            namespace,
            keys,
            batch,
        } => process_delete(&engine, namespace, keys, batch).await,
        CaptureJobKind::Mixed {
            namespace,
            keys,
            digests,
            deletes,
        } => process_mixed(&engine, namespace, keys, digests, deletes).await,
    }
    drop(_pending_task);
}

pub(super) async fn stage_marker(
    engine: &Arc<SyncEngine>,
    namespace: &str,
    keys: &[Vec<u8>],
) -> Option<Vec<u8>> {
    match engine.stage_capture_reexport(namespace, keys).await {
        Ok(marker) => Some(marker),
        Err(error) => {
            tracing::error!(
                target: "fold_db::sync::mutation_log",
                namespace,
                error = %error,
                "failed to persist crash-safe capture re-export intent; direct capture will still be attempted"
            );
            None
        }
    }
}

pub(super) async fn clear_marker(engine: &Arc<SyncEngine>, namespace: &str, marker: Option<&[u8]>) {
    if let Some(marker) = marker {
        let cleanup_started = std::time::Instant::now();
        if let Err(error) = engine.clear_capture_reexport(marker).await {
            engine.record_capture_reexport_failure(&error).await;
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                namespace,
                error = %error,
                "captured mutation but could not clear re-export marker; durable receipt will guard its retry"
            );
        }
        engine.capture_cleanup_us.fetch_add(
            elapsed_micros(cleanup_started),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

pub(super) async fn finish_capture(
    engine: &Arc<SyncEngine>,
    namespace: &str,
    marker: Option<Vec<u8>>,
    result: Result<u64, String>,
    operation: &'static str,
) {
    if let Err(error) = result {
        let message = format!(
            "mutation-log write-path capture failed after committed {operation} in '{namespace}': {error}"
        );
        engine.record_capture_reexport_failure(&message).await;
        engine
            .record_sync_failure(&SyncError::Storage(message.clone()))
            .await;
        tracing::error!(
            target: "fold_db::sync::mutation_log",
            namespace,
            operation,
            error = %error,
            local_write_committed = true,
            "mutation-log capture failed; local write remains committed"
        );
    } else {
        clear_marker(engine, namespace, marker.as_deref()).await;
    }
}
