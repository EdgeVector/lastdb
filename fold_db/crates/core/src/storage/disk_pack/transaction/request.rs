//! Complete accepted requests share one bounded queue and disk owner.

use super::*;
use crate::storage::laststore::LastStoreKvStore;

/// Every request waits for its complete disk publication before success.
pub(crate) async fn request_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    early_publication: bool,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(runtime, early_publication, future).await
}

/// Background work uses the same immediate disk boundary as caller writes.
pub(crate) async fn background_scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope(runtime, true, future).await
}

async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    early_publication: bool,
    future: impl std::future::Future<Output = Result<T, E>>,
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
    runtime.wait_for_part_room().await;
    let gate = runtime.enter_prepare().await;
    let held_from = std::time::Instant::now();
    super::blocked::opened();
    let pending = Arc::new(PendingBatch::new(&runtime));
    let result = read::write_scope(
        Arc::clone(&runtime),
        Arc::clone(&pending),
        WRITE.scope(Arc::clone(&pending), future),
    )
    .await;
    // Refused complete mutations never publish their earlier private stages.
    // Preserve this classification even if an inner helper changed its error.
    if let Some(error) = pending.preparation_limit() {
        return Err(error.into());
    }
    let result = result??;
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
    let early_publication = early_publication
        && !pending.durable_only.load(Ordering::Acquire)
        && pending.has_reserved_raw_memory();
    let owner = Arc::clone(&runtime);
    // A cancelled pre-admission caller cannot release the queue's older quota.
    let (prepared, gate) =
        LastStoreKvStore::run_blocking(move || Ok((owner.prepare_queued(operations)?, gate)))
            .await?;
    let early_publication = protect_paths(&runtime, &pending, early_publication)?;
    // Publication, ownership transfer and worker creation contain no await.
    let sequence = runtime.accept_queued(pending, prepared, early_publication)?;
    drop(gate);
    note_gate_hold(held_from, operations_len, sequence, &shape);
    runtime.wait_for_queue(sequence).await?;
    super::blocked::note(held_from.elapsed());
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
/// record the hold. The hold covers preparation, staging and acceptance. The
/// durable wait comes after the gate is free and is not counted here. A batch that
/// seals the queue keeps writers waiting through that wait too (`blocked.rs`).
fn note_gate_hold(
    held_from: std::time::Instant,
    operations: usize,
    sequence: u64,
    shape: &BatchShape,
) {
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
}

fn protect_paths(
    runtime: &PackRuntime,
    pending: &PendingBatch,
    early_publication: bool,
) -> StorageResult<bool> {
    let paths = std::mem::take(&mut *pending.paths.lock().expect("native private paths"));
    let absences = std::mem::take(&mut *pending.absences.lock().expect("native private absences"));
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
    let changes =
        super::super::logical_paths::controls::expanded_coordinates(&pending.changed_coordinates());
    let publication = super::super::logical_paths::PathPublication {
        definitions,
        owner,
        changed_sources: changes,
        early: early_publication,
    };
    let protection = match super::super::logical_paths::PathProtection::reserve(
        runtime,
        &paths,
        &absences,
        publication,
    ) {
        Ok(protection) => protection,
        Err(StorageError::WriteResourceLimit { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    let early_publication = early_publication && protection.memory_ready();
    *pending
        .path_protection
        .lock()
        .expect("native typed path protection") = Some(protection);
    Ok(early_publication)
}
