// lint:file-size-ok moved verbatim from write_path.rs; one concern per file
//! Capture queue jobs and the single worker that drains them in order.

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
        });
    }
}

pub(super) struct CaptureJob {
    pub(super) engine: Arc<SyncEngine>,
    pub(super) kind: CaptureJobKind,
    pub(super) queued_at: std::time::Instant,
    pub(super) _pending_task: PendingTask,
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
    /// Small leftover catalog rows that a member node must apply from the log.
    ApplyablePut {
        namespace: String,
        items: Vec<(Vec<u8>, Vec<u8>)>,
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
        if let Err(join_error) = tokio::spawn(process_capture_job(job)).await {
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
    // lint:fn-size-ok verbatim move from write_path.rs; splitting this function is separate work
    let CaptureJob {
        engine,
        kind,
        queued_at,
        _pending_task,
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
            // Product capture is async. Append the cloud row in this same
            // step, under the drain lock, and delete the marker before any
            // scan can append it a second time. Off and legacy capture stay
            // on the later scan.
            let product_mutation_log =
                matches!(engine.config.capture_mode, CaptureMode::MutationLog)
                    && !engine.config.legacy_personal_cloud_sync;
            let _direct_drain_guard = if record_now || product_mutation_log {
                Some(engine.capture_reexport_drain_lock.lock().await)
            } else {
                None
            };
            if let Some(barrier) = author_clock_barrier {
                if let Err(error) = barrier.wait().await {
                    let error =
                        format!("logical mutation capture lost its author-clock barrier: {error}");
                    engine
                        .record_sync_failure(&SyncError::Storage(error.clone()))
                        .await;
                    tracing::error!(
                        %error,
                        "author-clock barrier failed after local commit; staging MutationIntent anyway"
                    );
                    // Do not return. Resident tips are already visible, so a
                    // missing marker is a silent cloud hole. Stage, then let
                    // the tick drain the intent.
                }
            }
            let stage_started = std::time::Instant::now();
            let marker = engine.stage_mutation_intent_reexport(&envelopes).await;
            engine.capture_stage_us.fetch_add(
                elapsed_micros(stage_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            let marker = match marker {
                Ok(marker) => marker,
                Err(error) => {
                    engine.record_capture_reexport_failure(&error).await;
                    // Best effort after the durable-marker failure. The
                    // response stays failed, but a working pin-log path can
                    // still preserve the mutation before the next retry.
                    let fallback = engine
                        .record_op_with_publication(
                            crate::sync::mutation_intent::mutation_intent_op(envelopes),
                        )
                        .await;
                    if let Err(fallback_error) = fallback {
                        engine
                            .record_capture_reexport_failure(&fallback_error)
                            .await;
                    }
                    if let Some(completion) = completion {
                        let _ = completion.send(LogicalCaptureCompletion::MarkerFailed(error));
                    }
                    return;
                }
            };
            if record_now {
                let record_started = std::time::Instant::now();
                let result = if matches!(engine.config.capture_mode, CaptureMode::MutationLog)
                    && !engine.config.legacy_personal_cloud_sync
                {
                    engine
                        .record_mutation_intent_marker_with_publication(marker.clone(), envelopes)
                        .await
                } else {
                    engine
                        .record_op_with_publication(
                            crate::sync::mutation_intent::mutation_intent_op(envelopes),
                        )
                        .await
                }
                .and_then(|append| {
                        if append.targets.is_empty() {
                            Err(
                                "exact mutation-log append produced no required targets; durable marker retained"
                                    .to_string(),
                            )
                        } else {
                            Ok(append)
                        }
                    });
                engine.capture_record_us.fetch_add(
                    elapsed_micros(record_started),
                    std::sync::atomic::Ordering::Relaxed,
                );
                if result.is_ok() {
                    clear_marker(&engine, "mutation_intent", Some(&marker)).await;
                    engine.note_mutation_log_local_append();
                    engine.wake.notify_one();
                } else if let Err(error) = &result {
                    engine.record_capture_reexport_failure(error).await;
                    engine
                        .record_sync_failure(&SyncError::Storage(format!(
                            "logical mutation capture failed after local commit: {error}"
                        )))
                        .await;
                }
                if let Some(completion) = completion {
                    let completed = match result {
                        Ok(append) => LogicalCaptureCompletion::Appended(append),
                        Err(error) => LogicalCaptureCompletion::AppendFailed(error),
                    };
                    let _ = completion.send(completed);
                }
            } else if product_mutation_log {
                let result = engine
                    .record_mutation_intent_marker_with_publication(marker.clone(), envelopes)
                    .await
                    .and_then(|append| {
                        if append.targets.is_empty() {
                            Err(
                                "exact mutation-log append produced no required targets; durable marker retained"
                                    .to_string(),
                            )
                        } else {
                            Ok(append)
                        }
                    });
                if result.is_ok() {
                    clear_marker(&engine, "mutation_intent", Some(&marker)).await;
                } else if let Err(error) = &result {
                    engine.record_capture_reexport_failure(error).await;
                    // The marker stays. Wake the scan so it can retry.
                    engine.wake.notify_one();
                }
                if let Some(completion) = completion {
                    let _ = completion.send(LogicalCaptureCompletion::MarkerDurable);
                }
            } else {
                engine.wake.notify_one();
                if let Some(completion) = completion {
                    let _ = completion.send(LogicalCaptureCompletion::MarkerDurable);
                }
            }
        }
        CaptureJobKind::PhysicalDigest {
            namespace,
            keys,
            digests,
        } => {
            let stage_started = std::time::Instant::now();
            let marker = stage_marker(&engine, &namespace, &keys).await;
            engine.capture_stage_us.fetch_add(
                elapsed_micros(stage_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            let record_started = std::time::Instant::now();
            let result = engine.record_physical_digest(&namespace, &digests).await;
            engine.capture_record_us.fetch_add(
                elapsed_micros(record_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            finish_capture(&engine, &namespace, marker, result, "physical_digest").await;
        }
        CaptureJobKind::ApplyablePut { namespace, items } => {
            let keys: Vec<Vec<u8>> = items.iter().map(|(key, _)| key.clone()).collect();
            let stage_started = std::time::Instant::now();
            let marker = stage_marker(&engine, &namespace, &keys).await;
            engine.capture_stage_us.fetch_add(
                elapsed_micros(stage_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            let record_started = std::time::Instant::now();
            let result = if items.len() == 1 {
                engine
                    .record_put(&namespace, &items[0].0, &items[0].1)
                    .await
            } else {
                engine.record_batch_put(&namespace, &items).await
            };
            engine.capture_record_us.fetch_add(
                elapsed_micros(record_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            finish_capture(&engine, &namespace, marker, result, "applyable_put").await;
        }
        CaptureJobKind::Delete {
            namespace,
            keys,
            batch,
        } => {
            let stage_started = std::time::Instant::now();
            let marker = stage_marker(&engine, &namespace, &keys).await;
            engine.capture_stage_us.fetch_add(
                elapsed_micros(stage_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            let record_started = std::time::Instant::now();
            let result = if batch {
                engine.record_batch_delete(&namespace, &keys).await
            } else {
                engine.record_delete(&namespace, &keys[0]).await
            };
            engine.capture_record_us.fetch_add(
                elapsed_micros(record_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            finish_capture(&engine, &namespace, marker, result, "delete").await;
        }
        CaptureJobKind::Mixed {
            namespace,
            keys,
            digests,
            deletes,
        } => {
            let stage_started = std::time::Instant::now();
            let marker = stage_marker(&engine, &namespace, &keys).await;
            engine.capture_stage_us.fetch_add(
                elapsed_micros(stage_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            let record_started = std::time::Instant::now();
            let result = async {
                let mut recorded = 0;
                if !digests.is_empty() {
                    recorded += engine.record_physical_digest(&namespace, &digests).await?;
                }
                if !deletes.is_empty() {
                    recorded += engine.record_batch_delete(&namespace, &deletes).await?;
                }
                Ok(recorded)
            }
            .await;
            engine.capture_record_us.fetch_add(
                elapsed_micros(record_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            finish_capture(&engine, &namespace, marker, result, "mixed").await;
        }
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
