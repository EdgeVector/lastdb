//! Scope-local metadata publication, duty-cost tracking, and operation accounting.

use super::{
    Arc, BTreeMap, HomeMutation, Mutex, PackRuntime, PendingBatch, PendingData, StorageError,
    StorageResult,
};
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
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
        Some(p) => p
            .publications
            .stage((cache, name), Arc::new(value), Box::new(action)),
        None => action(),
    }
}

pub(crate) fn get<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
    name: &str,
) -> StorageResult<Option<Arc<T>>> {
    let key = (cache, name.to_string());
    for pending in write_views(runtime).into_iter().rev() {
        if let Some((val, _)) = pending.publications.lock()?.entries.get(&key) {
            return Arc::clone(val)
                .downcast::<T>()
                .map(Some)
                .map_err(|_| type_mismatch());
        }
    }
    Ok(None)
}

pub(crate) fn entries<T: Any + Send + Sync>(
    runtime: &PackRuntime,
    cache: usize,
) -> StorageResult<Vec<(String, Arc<T>)>> {
    let mut out = BTreeMap::new();
    for pending in write_views(runtime) {
        for ((owner, name), (val, _)) in &pending.publications.lock()?.entries {
            if *owner == cache {
                out.insert(
                    name.clone(),
                    Arc::clone(val)
                        .downcast::<T>()
                        .map_err(|_| type_mismatch())?,
                );
            }
        }
    }
    Ok(out.into_iter().collect())
}

fn write_views(runtime: &PackRuntime) -> Vec<Arc<PendingBatch>> {
    let Some(pending) = PendingBatch::current(runtime) else {
        return Vec::new();
    };
    let mut views = super::super::read::ReadContext::captured(runtime)
        .map_or_else(Vec::new, |ctx| ctx.accepted.as_ref().clone());
    views.push(pending);
    views
}

fn type_mismatch() -> StorageError {
    StorageError::BackendError("native metadata cache type differs".into())
}

pub(in crate::storage::disk_pack::transaction) fn refusal(
    data: &PendingData,
    operations: &[HomeMutation],
    limit: usize,
) -> StorageError {
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
    let (cur, inc) = (data.operations.len(), operations.len());
    StorageError::MutationLimit {
        detail: format!(
            "complete native mutation exceeds {limit} storage operations: current={cur}, incoming={inc}, projected={}, projected_unique={}",
            cur.saturating_add(inc),
            unique.len()
        ),
    }
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
            .clone()
            .map(|detail| StorageError::MutationLimit { detail })
    }
}

pub(in crate::storage::disk_pack) fn batch_bytes(
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
        let body = op.body.as_ref().map_or(0, Vec::len);
        bytes = bytes
            .checked_add((160 + op.collection.len() + 2 * op.key.len() + body) as u64)
            .filter(|&b| b <= PENDING_MEMORY_BYTES)
            .ok_or_else(|| StorageError::MutationLimit {
                detail: "complete native batch exceeds pending memory allowance".into(),
            })?;
    }
    Ok(bytes)
}
