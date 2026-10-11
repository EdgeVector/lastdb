//! Queue-worker handling of physical-digest, delete, and mixed capture jobs.

use super::*;

/// Stage a marker, run `record`, and time both steps in the engine counters.
async fn stage_and_record<F>(
    engine: &Arc<SyncEngine>,
    namespace: &str,
    keys: &[Vec<u8>],
    record: F,
) -> (Option<Vec<u8>>, Result<u64, String>)
where
    F: Future<Output = Result<u64, String>>,
{
    let stage_started = std::time::Instant::now();
    let marker = stage_marker(engine, namespace, keys).await;
    engine.capture_stage_us.fetch_add(
        elapsed_micros(stage_started),
        std::sync::atomic::Ordering::Relaxed,
    );
    let record_started = std::time::Instant::now();
    let result = record.await;
    engine.capture_record_us.fetch_add(
        elapsed_micros(record_started),
        std::sync::atomic::Ordering::Relaxed,
    );
    (marker, result)
}

pub(super) async fn process_physical_digest(
    engine: &Arc<SyncEngine>,
    namespace: String,
    keys: Vec<Vec<u8>>,
    digests: Vec<(Vec<u8>, [u8; 32])>,
) {
    let (marker, result) = stage_and_record(
        engine,
        &namespace,
        &keys,
        engine.record_physical_digest(&namespace, &digests),
    )
    .await;
    finish_capture(engine, &namespace, marker, result, "physical_digest").await;
}

pub(super) async fn process_delete(
    engine: &Arc<SyncEngine>,
    namespace: String,
    keys: Vec<Vec<u8>>,
    batch: bool,
) {
    let record = async {
        if batch {
            engine.record_batch_delete(&namespace, &keys).await
        } else {
            engine.record_delete(&namespace, &keys[0]).await
        }
    };
    let (marker, result) = stage_and_record(engine, &namespace, &keys, record).await;
    finish_capture(engine, &namespace, marker, result, "delete").await;
}

pub(super) async fn process_mixed(
    engine: &Arc<SyncEngine>,
    namespace: String,
    keys: Vec<Vec<u8>>,
    digests: Vec<(Vec<u8>, [u8; 32])>,
    deletes: Vec<Vec<u8>>,
) {
    let record = async {
        let mut recorded = 0;
        if !digests.is_empty() {
            recorded += engine.record_physical_digest(&namespace, &digests).await?;
        }
        if !deletes.is_empty() {
            recorded += engine.record_batch_delete(&namespace, &deletes).await?;
        }
        Ok(recorded)
    };
    let (marker, result) = stage_and_record(engine, &namespace, &keys, record).await;
    finish_capture(engine, &namespace, marker, result, "mixed").await;
}
