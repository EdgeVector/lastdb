//! Queue-worker handling of one logical-intent capture job.

use super::*;
use crate::sync::engine::MutationLogAppendReceipt;

type Completion = Option<tokio::sync::oneshot::Sender<LogicalCaptureCompletion>>;

pub(super) async fn process_logical_intent(
    engine: &Arc<SyncEngine>,
    envelopes: Vec<MutationEnvelope>,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
    record_now: bool,
    completion: Completion,
) {
    // Product capture is async. Append the cloud row in this same
    // step, under the drain lock, and delete the marker before any
    // scan can append it a second time. Off and legacy capture stay
    // on the later scan.
    let product_mutation_log = matches!(engine.config.capture_mode, CaptureMode::MutationLog)
        && !engine.config.legacy_personal_cloud_sync;
    let _direct_drain_guard = if record_now || product_mutation_log {
        Some(engine.capture_reexport_drain_lock.lock().await)
    } else {
        None
    };
    if let Some(barrier) = author_clock_barrier {
        wait_author_clock_barrier(engine, barrier).await;
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
            handle_marker_failure(engine, envelopes, error, completion).await;
            return;
        }
    };
    if record_now {
        record_intent_now(engine, marker, envelopes, completion).await;
    } else if product_mutation_log {
        append_product_marker(engine, marker, envelopes, completion).await;
    } else {
        engine.wake.notify_one();
        if let Some(completion) = completion {
            let _ = completion.send(LogicalCaptureCompletion::MarkerDurable);
        }
    }
}

async fn wait_author_clock_barrier(engine: &Arc<SyncEngine>, barrier: AuthorClockPersistBarrier) {
    if let Err(error) = barrier.wait().await {
        let error = format!("logical mutation capture lost its author-clock barrier: {error}");
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

async fn handle_marker_failure(
    engine: &Arc<SyncEngine>,
    envelopes: Vec<MutationEnvelope>,
    error: String,
    completion: Completion,
) {
    engine.record_capture_reexport_failure(&error).await;
    // Best effort after the durable-marker failure. The
    // response stays failed, but a working pin-log path can
    // still preserve the mutation before the next retry.
    let fallback = engine
        .record_op_with_publication(crate::sync::mutation_intent::mutation_intent_op(envelopes))
        .await;
    if let Err(fallback_error) = fallback {
        engine
            .record_capture_reexport_failure(&fallback_error)
            .await;
    }
    if let Some(completion) = completion {
        let _ = completion.send(LogicalCaptureCompletion::MarkerFailed(error));
    }
}

fn require_append_targets(
    append: MutationLogAppendReceipt,
) -> Result<MutationLogAppendReceipt, String> {
    if append.targets.is_empty() {
        Err(
            "exact mutation-log append produced no required targets; durable marker retained"
                .to_string(),
        )
    } else {
        Ok(append)
    }
}

async fn record_intent_now(
    engine: &Arc<SyncEngine>,
    marker: Vec<u8>,
    envelopes: Vec<MutationEnvelope>,
    completion: Completion,
) {
    let record_started = std::time::Instant::now();
    let result = if matches!(engine.config.capture_mode, CaptureMode::MutationLog)
        && !engine.config.legacy_personal_cloud_sync
    {
        engine
            .record_mutation_intent_marker_with_publication(marker.clone(), envelopes)
            .await
    } else {
        engine
            .record_op_with_publication(crate::sync::mutation_intent::mutation_intent_op(envelopes))
            .await
    }
    .and_then(require_append_targets);
    engine.capture_record_us.fetch_add(
        elapsed_micros(record_started),
        std::sync::atomic::Ordering::Relaxed,
    );
    if result.is_ok() {
        clear_marker(engine, "mutation_intent", Some(&marker)).await;
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
}

async fn append_product_marker(
    engine: &Arc<SyncEngine>,
    marker: Vec<u8>,
    envelopes: Vec<MutationEnvelope>,
    completion: Completion,
) {
    let result = engine
        .record_mutation_intent_marker_with_publication(marker.clone(), envelopes)
        .await
        .and_then(require_append_targets);
    if result.is_ok() {
        clear_marker(engine, "mutation_intent", Some(&marker)).await;
    } else if let Err(error) = &result {
        engine.record_capture_reexport_failure(error).await;
        // The marker stays. Wake the scan so it can retry.
        engine.wake.notify_one();
    }
    if let Some(completion) = completion {
        let _ = completion.send(LogicalCaptureCompletion::MarkerDurable);
    }
}
