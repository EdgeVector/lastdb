//! Complete accepted requests share one bounded queue, disk owner, and duty-cost tracker.

use super::*;

pub(crate) mod blocked {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    static WRITE_SCOPES: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Cost {
        pub(crate) blocked: Duration,
        pub(crate) scopes: u64,
    }

    #[derive(Default)]
    struct JobCost {
        blocked_us: AtomicU64,
        scopes: AtomicU64,
    }

    tokio::task_local! {
        static JOB: Arc<JobCost>;
    }

    pub(crate) async fn measure<F: Future>(work: F) -> (F::Output, Cost) {
        let job = Arc::new(JobCost::default());
        let output = JOB.scope(Arc::clone(&job), work).await;
        let cost = Cost {
            blocked: Duration::from_micros(job.blocked_us.load(Ordering::Acquire)),
            scopes: job.scopes.load(Ordering::Acquire),
        };
        (output, cost)
    }

    pub(in crate::storage::disk_pack::transaction) fn opened() {
        WRITE_SCOPES.fetch_add(1, Ordering::AcqRel);
        let _ = JOB.try_with(|job| job.scopes.fetch_add(1, Ordering::AcqRel));
    }

    pub(in crate::storage::disk_pack::transaction) fn note(blocked: Duration) {
        let micros = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
        let _ = JOB.try_with(|job| job.blocked_us.fetch_add(micros, Ordering::AcqRel));
    }

    pub(crate) fn write_scopes() -> u64 {
        WRITE_SCOPES.load(Ordering::Acquire)
    }
}

pub(crate) async fn request_scope<T, E>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope_inner(runtime, future, false).await
}

pub(crate) async fn background_scope<T, E>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    scope_inner(runtime, future, true).await
}

async fn scope_inner<T, E>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = Result<T, E>>,
    background: bool,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let gate = runtime.enter_prepare().await;
    let held_from = std::time::Instant::now();
    let pending = Arc::new(PendingBatch::new(&runtime));
    let result = read::write_scope(
        Arc::clone(&runtime),
        Arc::clone(&pending),
        WRITE.scope(Arc::clone(&pending), future),
    )
    .await;
    if !pending.is_empty().unwrap_or(false) {
        blocked::opened();
    }
    if let Some(error) = pending.preparation_limit() {
        return Err(error.into());
    }
    let result = result??;
    pending.require_final_format()?;
    if pending.is_empty()? {
        let target = runtime.seal_queue();
        drop(gate);
        note_gate_hold(held_from, 0, 0, &BatchShape::default());
        runtime.wait_for_queue(target).await?;
        blocked::note(held_from.elapsed());
        return Ok(result);
    }
    let operations = pending.freeze_operations();
    let operations_len = operations.len();
    let shape = BatchShape::of(&operations);
    let publication = Arc::clone(&runtime.write_gate).lock_owned().await;
    let owner = Arc::clone(&runtime);
    let (prepared, gate, publication) =
        run_blocking(move || Ok((owner.prepare_queued(operations)?, gate, publication))).await?;
    let pending_timing = Arc::clone(&pending);
    let sequence = runtime.accept_queued(pending, prepared)?;
    drop(publication);
    drop(gate);
    let hold = note_gate_hold(held_from, operations_len, sequence, &shape);
    runtime.wait_for_queue(sequence).await?;
    blocked::note(if background {
        hold.saturating_add(pending_timing.flush_blocked())
    } else {
        held_from.elapsed()
    });
    Ok(result)
}

#[derive(Default)]
struct BatchShape {
    record_puts: u32,
    record_deletes: u32,
    alternate_puts: u32,
    alternate_deletes: u32,
    file_puts: u32,
    file_deletes: u32,
}

impl BatchShape {
    fn of(operations: &[HomeMutation]) -> Self {
        let mut shape = Self::default();
        if !tracing::enabled!(target: "fold_db::write_gate", tracing::Level::DEBUG) {
            return shape;
        }
        for operation in operations {
            let key = operation.key.as_str();
            let (puts, deletes) =
                if operation.collection == crate::storage::disk_pack::physical::CAS_COLLECTION {
                    (&mut shape.file_puts, &mut shape.file_deletes)
                } else if key.starts_with(lastdb_model::record_space::RECORD_PREFIX) {
                    (&mut shape.record_puts, &mut shape.record_deletes)
                } else if key.starts_with(lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX) {
                    (&mut shape.alternate_puts, &mut shape.alternate_deletes)
                } else {
                    continue;
                };
            let slot = if operation.body.is_some() {
                puts
            } else {
                deletes
            };
            *slot += 1;
        }
        shape
    }
}

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
        record_puts = shape.record_puts,
        record_deletes = shape.record_deletes,
        alternate_puts = shape.alternate_puts,
        alternate_deletes = shape.alternate_deletes,
        file_puts = shape.file_puts,
        file_deletes = shape.file_deletes,
        "native write gate released"
    );
    held
}
