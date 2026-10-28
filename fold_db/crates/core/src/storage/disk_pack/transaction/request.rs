//! Complete accepted requests share one bounded queue and disk owner.

use super::*;

pub(crate) async fn request_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(runtime, future, false).await
}

/// Return after the complete private background commit reaches disk.
pub(crate) async fn background_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(runtime, future, true).await
}

async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
    background: bool,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    let Some(runtime) = runtime else {
        return future.await;
    };
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let mut timing = super::super::timing::start();
    let gate = runtime.enter_prepare().await;
    runtime.require_restore_validated()?;
    super::super::timing::mark(&mut timing, "admission_us");
    let held_from = std::time::Instant::now();
    let pending = Arc::new(PendingBatch::new(&runtime));
    let result = read::write_scope(
        Arc::clone(&runtime),
        Arc::clone(&pending),
        WRITE.scope(
            Arc::clone(&pending),
            crate::write_preparation::capture(future),
        ),
    )
    .await;
    // A scope that staged nothing is not counted as a writer (`blocked::opened`): a poll
    // that opens an empty scope (`verify-refs` does, at the start of a run) must not
    // keep a background job in its duty-paced mode.
    if !pending.is_empty().unwrap_or(false) {
        super::blocked::opened();
    }
    // Refused complete mutations never publish their earlier private stages.
    // Preserve this classification even if an inner helper changed its error.
    if let Some(error) = pending.preparation_limit() {
        return Err(error.into());
    }
    let result = result??;
    super::super::timing::mark(&mut timing, "logical_prepare_us");
    if pending.is_empty()? {
        // Preserve an explicit durability fence without adding an empty batch.
        let target = runtime.seal_queue();
        drop(gate);
        note_gate_hold(held_from, 0, 0, &BatchShape::default());
        runtime.wait_for_queue(target).await?;
        super::blocked::note(held_from.elapsed());
        return Ok(result);
    }
    let operations = pending.freeze_operations();
    let operations_len = operations.len();
    let shape = BatchShape::of(&operations);
    // A merge can publish an equivalent physical root during logical
    // preparation. Admission binds owned quota to this logical frontier.
    let publication = Arc::clone(&runtime.write_gate).lock_owned().await;
    let owner = Arc::clone(&runtime);
    // A cancelled pre-admission caller cannot release the queue's older quota.
    let (prepared, gate, publication) =
        run_blocking(move || Ok((owner.prepare_queued(operations)?, gate, publication))).await?;
    super::super::timing::mark(&mut timing, "capacity_prepare_us");
    protect_paths(&runtime, &pending)?;
    // Every caller waits for the complete snapshot and clean core publication.
    // Publication, ownership transfer and worker creation contain no await.
    let pending_timing = Arc::clone(&pending);
    let sequence = runtime.accept_queued(pending, prepared)?;
    drop(publication);
    drop(gate);
    super::super::timing::mark(&mut timing, "accept_us");
    let hold = note_gate_hold(held_from, operations_len, sequence, &shape);
    runtime.wait_for_queue(sequence).await?;
    super::super::timing::mark(&mut timing, "durable_wait_us");
    super::super::timing::request(timing, sequence, background);
    super::blocked::note(if background {
        hold.saturating_add(pending_timing.flush_blocked())
    } else {
        held_from.elapsed()
    });
    Ok(result)
}

/// The kinds of operation in one accepted batch, for a run that reads the log.
/// Tips, atom bodies, atom counters and blob counters of one request share one
/// batch, so a delete on zero shows its deletes, and the blob counter change that
/// goes with them, under one sequence number. The molecule headers (with the slots
/// and records counters) and the binding records and counters join the same batch.
#[derive(Default)]
struct BatchShape {
    tip_puts: u32,
    tip_deletes: u32,
    body_puts: u32,
    body_deletes: u32,
    counter_puts: u32,
    counter_deletes: u32,
    blob_counter_puts: u32,
    blob_counter_deletes: u32,
    blob_queue_puts: u32,
    blob_queue_deletes: u32,
    header_puts: u32,
    binding_puts: u32,
    binding_deletes: u32,
}

impl BatchShape {
    fn of(operations: &[HomeMutation]) -> Self {
        let mut shape = Self::default();
        if !tracing::enabled!(target: "fold_db::write_gate", tracing::Level::DEBUG) {
            return shape;
        }
        for operation in operations {
            // A key of a database scope or a share starts with the scope. The
            // structural family follows it.
            let key = operation.key.as_str();
            let key = super::super::super::current_keys::source_key(key).unwrap_or(key);
            let put = operation.body.is_some();
            let slot = if key.starts_with("mk:") {
                if put {
                    &mut shape.tip_puts
                } else {
                    &mut shape.tip_deletes
                }
            } else if key.starts_with("atom:") && key.ends_with("\0b") {
                if put {
                    &mut shape.body_puts
                } else {
                    &mut shape.body_deletes
                }
            } else if key.starts_with("atom:") && key.ends_with("\0c") {
                if put {
                    &mut shape.counter_puts
                } else {
                    &mut shape.counter_deletes
                }
            } else if key.starts_with("bref:v3:c:") {
                if put {
                    &mut shape.blob_counter_puts
                } else {
                    &mut shape.blob_counter_deletes
                }
            } else if key.starts_with("bref:v3:q:") || key.starts_with("bref:v3:d\0") {
                if put {
                    &mut shape.blob_queue_puts
                } else {
                    &mut shape.blob_queue_deletes
                }
            } else if key.starts_with("mh:") && put {
                &mut shape.header_puts
            } else if key.starts_with("mref:v1:") {
                if put {
                    &mut shape.binding_puts
                } else {
                    &mut shape.binding_deletes
                }
            } else {
                continue;
            };
            *slot += 1;
        }
        shape
    }
}

/// Say how long a request held the write gate, for a run that reads the log, and
/// return the hold. The hold covers preparation, staging and acceptance. The
/// durable wait comes after the gate is free and is not counted here. A batch that
/// seals the queue keeps writers waiting through that wait too (`blocked.rs`).
fn note_gate_hold(
    held_from: std::time::Instant,
    operations: usize,
    sequence: u64,
    shape: &BatchShape,
) -> std::time::Duration {
    let held = held_from.elapsed();
    tracing::debug!(
        target: "fold_db::write_gate",
        hold_us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX),
        operations,
        sequence,
        tip_puts = shape.tip_puts,
        tip_deletes = shape.tip_deletes,
        body_puts = shape.body_puts,
        body_deletes = shape.body_deletes,
        counter_puts = shape.counter_puts,
        counter_deletes = shape.counter_deletes,
        blob_counter_puts = shape.blob_counter_puts,
        blob_counter_deletes = shape.blob_counter_deletes,
        blob_queue_puts = shape.blob_queue_puts,
        blob_queue_deletes = shape.blob_queue_deletes,
        header_puts = shape.header_puts,
        binding_puts = shape.binding_puts,
        binding_deletes = shape.binding_deletes,
        "native write gate released"
    );
    held
}

fn protect_paths(runtime: &PackRuntime, pending: &PendingBatch) -> StorageResult<()> {
    // Recheck the complete staged batch before consuming its typed paths. A
    // late metadata stage must not reuse an insufficient memory owner. The
    // durable fallback invalidates/refetches records without an uncharged cache.
    if !pending.has_reserved_raw_memory() {
        pending
            .raw_memory_reservation
            .lock()
            .expect("native model reservation")
            .take();
    }
    let paths = std::mem::take(&mut *pending.paths.lock().expect("native private paths"));
    let definitions = std::mem::take(
        &mut *pending
            .definitions
            .lock()
            .expect("native private definitions"),
    )
    .into_values()
    .collect();
    let owner = pending
        .raw_memory_reservation
        .lock()
        .expect("native model reservation")
        .clone();
    let publication = super::super::logical_paths::PathPublication { definitions, owner };
    let protection =
        match super::super::logical_paths::PathProtection::reserve(runtime, &paths, publication) {
            Ok(protection) => protection,
            Err(StorageError::WriteResourceLimit { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
    *pending
        .path_protection
        .lock()
        .expect("native typed path protection") = Some(protection);
    Ok(())
}
