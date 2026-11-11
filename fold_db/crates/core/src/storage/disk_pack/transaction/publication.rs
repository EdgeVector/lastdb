//! Scope-local metadata publication, duty-cost tracking, and operation accounting.

use super::{
    Arc, BTreeMap, HomeMutation, Mutex, PackRuntime, PendingBatch, PendingData, StorageError,
    StorageResult,
};
use std::any::Any;
use std::collections::BTreeSet;

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

type Action = Box<dyn FnOnce() -> StorageResult<()> + Send>;
type CacheKey = (usize, String);
type Value = Arc<dyn Any + Send + Sync>;

pub(crate) fn active(runtime: &PackRuntime) -> bool {
    PendingBatch::current(runtime).is_some()
}

#[derive(Default)]
struct Changes {
    entries: BTreeMap<CacheKey, (Value, Action)>,
    staged: usize,
}

#[derive(Default)]
pub(in crate::storage::disk_pack::transaction) struct PendingPublications(Mutex<Changes>);

impl PendingPublications {
    pub(in crate::storage::disk_pack::transaction) fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.lock()?.entries.is_empty())
    }

    fn lock(&self) -> StorageResult<std::sync::MutexGuard<'_, Changes>> {
        self.0.lock().map_err(|_| {
            StorageError::BackendError("native metadata publication lock is poisoned".into())
        })
    }

    fn stage(&self, key: CacheKey, value: Value, action: Action) -> StorageResult<()> {
        let mut changes = self.lock()?;
        if changes.staged >= 4096 {
            return Err(StorageError::InvalidOperation(
                "native mutation exceeds 4096 metadata publications".into(),
            ));
        }
        changes.entries.insert(key, (value, action));
        changes.staged += 1;
        Ok(())
    }

    pub(in crate::storage::disk_pack::transaction) fn finish(&self) -> StorageResult<()> {
        let changes = std::mem::take(&mut *self.lock()?);
        let mut failure = None;
        for (_, (_, action)) in changes.entries {
            if let Err(error) = action() {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

pub(crate) fn publish<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: String,
    value: T,
    action: impl FnOnce() -> StorageResult<()> + Send + 'static,
) -> StorageResult<()> {
    match PendingBatch::current(runtime) {
        Some(pending) => {
            pending
                .publications
                .stage((cache, name), Arc::new(value), Box::new(action))
        }
        None => action(),
    }
}

pub(crate) fn get<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    for pending in write_views(runtime).into_iter().rev() {
        let value = pending
            .publications
            .lock()?
            .entries
            .get(&(cache, name.to_string()))
            .map(|(value, _)| Arc::clone(value));
        if let Some(value) = value {
            return value.downcast::<T>().map(Some).map_err(|_| type_mismatch());
        }
    }
    Ok(None)
}

pub(crate) fn entries<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
) -> StorageResult<Vec<(String, Arc<T>)>> {
    let mut entries = BTreeMap::new();
    for pending in write_views(runtime) {
        let changes = pending.publications.lock()?;
        for ((owner, name), (value, _)) in &changes.entries {
            if *owner == cache {
                entries.insert(
                    name.clone(),
                    Arc::clone(value)
                        .downcast::<T>()
                        .map_err(|_| type_mismatch())?,
                );
            }
        }
    }
    Ok(entries.into_iter().collect())
}

fn write_views(runtime: &PackRuntime) -> Vec<Arc<PendingBatch>> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return Vec::new();
    };
    let mut views = super::super::read::ReadContext::captured(runtime)
        .map_or_else(Vec::new, |context| context.accepted.as_ref().clone());
    views.push(pending);
    views
}

fn type_mismatch() -> StorageError {
    StorageError::BackendError("native metadata cache type differs".into())
}

fn counts(data: &PendingData, operations: &[HomeMutation]) -> (usize, usize, usize, usize) {
    let mut unique: BTreeSet<_> = data
        .latest
        .keys()
        .map(|(c, k)| (c.as_str(), k.as_str()))
        .collect();
    unique.extend(
        operations
            .iter()
            .map(|op| (op.collection.as_str(), op.key.as_str())),
    );
    (
        data.operations.len(),
        operations.len(),
        data.operations.len().saturating_add(operations.len()),
        unique.len(),
    )
}

pub(in crate::storage::disk_pack::transaction) fn refusal(
    data: &PendingData,
    operations: &[HomeMutation],
    limit: usize,
) -> StorageError {
    let (current, incoming, projected, unique) = counts(data, operations);
    StorageError::MutationLimit {
        detail: format!(
            "complete native mutation exceeds {limit} storage operations: current={current}, incoming={incoming}, projected={projected}, projected_unique={unique}"
        ),
    }
}

pub(in crate::storage::disk_pack::transaction) fn record_stage(
    data: &PendingData,
    operations: &[HomeMutation],
    limit: usize,
) {
    if !tracing::enabled!(target: "lastdb::native_write_budget", tracing::Level::DEBUG) {
        return;
    }
    let (current, incoming, projected, unique) = counts(data, operations);
    tracing::debug!(
        target: "lastdb::native_write_budget",
        current_operations = current,
        incoming_operations = incoming,
        projected_operations = projected,
        projected_unique_operations = unique,
        operation_limit = limit,
        "native preparation cumulative stored operations"
    );
}

#[derive(Default)]
pub(in crate::storage::disk_pack::transaction) struct BatchShape {
    record_puts: u32,
    record_deletes: u32,
    alternate_puts: u32,
    alternate_deletes: u32,
    file_puts: u32,
    file_deletes: u32,
}

impl BatchShape {
    pub(in crate::storage::disk_pack::transaction) fn of(operations: &[HomeMutation]) -> Self {
        let mut shape = Self::default();
        if !tracing::enabled!(target: "fold_db::write_gate", tracing::Level::DEBUG) {
            return shape;
        }
        for op in operations {
            let key = op.key.as_str();
            let (puts, deletes) =
                if op.collection == crate::storage::disk_pack::physical::CAS_COLLECTION {
                    (&mut shape.file_puts, &mut shape.file_deletes)
                } else if key.starts_with(lastdb_model::record_space::RECORD_PREFIX) {
                    (&mut shape.record_puts, &mut shape.record_deletes)
                } else if key.starts_with(lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX) {
                    (&mut shape.alternate_puts, &mut shape.alternate_deletes)
                } else {
                    continue;
                };
            if op.body.is_some() {
                *puts += 1;
            } else {
                *deletes += 1;
            }
        }
        shape
    }

    pub(in crate::storage::disk_pack::transaction) fn log(
        &self,
        held_from: std::time::Instant,
        operations: usize,
        sequence: u64,
    ) -> std::time::Duration {
        let held = held_from.elapsed();
        tracing::debug!(
            target: "fold_db::write_gate",
            hold_us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX),
            operations,
            sequence,
            record_puts = self.record_puts,
            record_deletes = self.record_deletes,
            alternate_puts = self.alternate_puts,
            alternate_deletes = self.alternate_deletes,
            file_puts = self.file_puts,
            file_deletes = self.file_deletes,
            "native write gate released"
        );
        held
    }
}

pub(in crate::storage::disk_pack::transaction) fn note_gate_hold(
    held_from: std::time::Instant,
    operations: usize,
    sequence: u64,
    ops: &[HomeMutation],
) -> std::time::Duration {
    BatchShape::of(ops).log(held_from, operations, sequence)
}

impl PendingBatch {
    pub(in crate::storage::disk_pack::transaction) fn remember_preparation_limit(
        &self,
        error: StorageError,
    ) -> StorageError {
        if let StorageError::MutationLimit { detail } = &error {
            self.preparation_limit
                .lock()
                .expect("native complete mutation limit")
                .get_or_insert_with(|| detail.clone());
        }
        error
    }

    pub(in crate::storage::disk_pack::transaction) fn preparation_limit(
        &self,
    ) -> Option<StorageError> {
        self.preparation_limit
            .lock()
            .expect("native complete mutation limit")
            .as_ref()
            .map(|detail| StorageError::MutationLimit {
                detail: detail.clone(),
            })
    }
}

pub(in crate::storage) fn batch_bytes(
    operations: &[HomeMutation],
    operation_limit: usize,
) -> StorageResult<u64> {
    if operations.len() > operation_limit {
        return Err(StorageError::MutationLimit {
            detail: format!("complete native batch exceeds {operation_limit} operations"),
        });
    }
    let mut bytes = 256u64;
    for op in operations {
        if op.collection.len() > 65531 || op.key.len() > 65535 {
            return Err(StorageError::InvalidOperation(
                "native collection or key exceeds format bounds".into(),
            ));
        }
        bytes = bytes
            .checked_add(
                (160 + op.collection.len()
                    + 2 * op.key.len()
                    + op.body.as_ref().map_or(0, Vec::len)) as u64,
            )
            .filter(|&b| b <= lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES)
            .ok_or_else(|| StorageError::MutationLimit {
                detail: "complete native batch exceeds pending memory allowance".into(),
            })?;
    }
    Ok(bytes)
}
