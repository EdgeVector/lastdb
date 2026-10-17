use super::*;
use std::collections::BTreeMap;

mod accounting;
mod headers;
pub(crate) use headers::note_header_write;
pub(crate) mod publication;
mod request;
pub(crate) use request::{background_scope, request_scope};
type AfterCommit = std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send>>;

pub(super) struct PendingRecord {
    pub(super) collection: String,
    pub(super) key: String,
    pub(super) body: Option<Vec<u8>>,
}

#[derive(Default)]
struct PendingData {
    operations: Vec<Arc<PendingRecord>>,
    latest: BTreeMap<(String, String), usize>,
    bytes: u64,
}

pub(super) struct PendingBatch {
    identity: uuid::Uuid,
    data: Mutex<PendingData>,
    publications: publication::PendingPublications,
    retained: Mutex<Vec<Box<dyn Send>>>,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    durable_only: AtomicBool,
    paths: Mutex<Vec<super::logical_paths::LogicalPathRecord>>,
    header_versions: Mutex<headers::HeaderVersions>,
    operation_limit: usize,
}

tokio::task_local! {
    static WRITE: Arc<PendingBatch>;
}

impl PendingBatch {
    fn new(runtime: &PackRuntime) -> Self {
        Self {
            identity: runtime.home.descriptor().identity,
            data: Mutex::new(PendingData::default()),
            publications: publication::PendingPublications::default(),
            retained: Mutex::new(Vec::new()),
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            durable_only: AtomicBool::new(false),
            paths: Mutex::new(Vec::new()),
            header_versions: Mutex::new(headers::HeaderVersions::default()),
            operation_limit: runtime
                .view
                .read()
                .expect("native complete write view")
                .current
                .batch_operation_limit(),
        }
    }
    pub(super) fn current(runtime: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|batch| batch.identity == runtime.home.descriptor().identity)
    }

    pub(super) fn lookup(&self, collection: &str, key: &str) -> Option<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .get(&(collection.to_string(), key.to_string()))
            .map(|&at| Arc::clone(&data.operations[at]))
    }

    pub(super) fn operation_limit(&self) -> usize {
        self.operation_limit
    }

    pub(super) fn matching(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .iter()
            .filter(|((name, key), _)| {
                name == collection && key.as_bytes() >= start && key.as_bytes() < end
            })
            .map(|(_, at)| Arc::clone(&data.operations[*at]))
            .collect()
    }

    pub(super) fn stage(&self, operations: Vec<HomeMutation>) -> StorageResult<()> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(StorageError::InvalidOperation(
                "native accepted batch is immutable".into(),
            ));
        }
        validate(&operations, self.operation_limit)?;
        let mut data = self.data.lock().expect("native pending batch");
        let count = data
            .operations
            .len()
            .checked_add(operations.len())
            .filter(|count| *count <= self.operation_limit)
            .ok_or_else(|| accounting::refusal(&data, &operations, self.operation_limit))?;
        accounting::record_stage(&data, &operations, self.operation_limit);
        let mut bytes = data.bytes;
        for operation in &operations {
            bytes = bytes
                .checked_add(
                    (160 + operation.collection.len()
                        + 2 * operation.key.len()
                        + operation.body.as_ref().map_or(0, Vec::len)) as u64,
                )
                .filter(|bytes| *bytes <= 128 * 1024 * 1024 - 256)
                .ok_or_else(|| {
                    StorageError::InvalidOperation(
                        "complete native mutation exceeds 128 MiB".into(),
                    )
                })?;
        }
        let additional = count - data.operations.len();
        data.operations
            .try_reserve(additional)
            .map_err(|_| StorageError::BackendError("native mutation allocation refused".into()))?;
        for operation in operations {
            let at = data.operations.len();
            data.latest
                .insert((operation.collection.clone(), operation.key.clone()), at);
            data.operations.push(Arc::new(PendingRecord {
                collection: operation.collection,
                key: operation.key,
                body: operation.body,
            }));
        }
        data.bytes = bytes;
        Ok(())
    }

    pub(super) fn freeze_operations(&self) -> Vec<HomeMutation> {
        self.frozen.store(true, Ordering::Release);
        self.data
            .lock()
            .expect("native pending batch")
            .operations
            .iter()
            .map(|record| HomeMutation {
                collection: record.collection.clone(),
                key: record.key.clone(),
                body: record.body.clone(),
            })
            .collect()
    }

    pub(super) fn changed_coordinates(&self) -> Vec<super::validity::ChangedCoordinate> {
        let data = self.data.lock().expect("native pending changes");
        let headers = self
            .header_versions
            .lock()
            .expect("native staged header versions");
        data.latest
            .iter()
            .map(|((collection, key), slot)| {
                let mut change = super::validity::changed(
                    collection.clone(),
                    key.clone(),
                    data.operations[*slot].body.as_deref(),
                );
                if let Some((digest, version)) = headers.entries.get(key) {
                    if change.digest.as_ref() == Some(digest) {
                        change.header = Some(*version);
                    }
                }
                change
            })
            .collect()
    }

    pub(super) fn finish_publications(&self) -> StorageResult<()> {
        self.publications.finish()
    }

    pub(super) async fn deliver(&self) -> StorageResult<()> {
        let actions =
            std::mem::take(&mut *self.after_commit.lock().expect("native post-commit work"));
        let mut actions: std::collections::VecDeque<_> = actions.into();
        while let Some(action) = actions.pop_front() {
            if let Err(error) = action.await {
                self.after_commit
                    .lock()
                    .expect("native post-commit work")
                    .extend(actions);
                return Err(error);
            }
        }
        Ok(())
    }
}

/// An unsupported memory mapping still completes its complete disk batch.
pub(crate) fn require_durable(runtime: &PackRuntime) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native durability choice requires a write scope".into())
    })?;
    pending.durable_only.store(true, Ordering::Release);
    Ok(())
}

/// Stage actual path content privately. Metadata and absences do not count.
pub(crate) fn stage_paths(
    runtime: &PackRuntime,
    records: Vec<super::logical_paths::LogicalPathRecord>,
) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native paths require a complete write scope".into())
    })?;
    let mut paths = pending.paths.lock().expect("native private paths");
    let bytes = paths
        .iter()
        .chain(&records)
        .try_fold(0usize, |total, record| {
            total
                .checked_add(record.body.len())
                .and_then(|n| n.checked_add(record.path.id().ok()?.len()))
                .filter(|n| *n <= 128 * 1024 * 1024)
                .ok_or_else(|| StorageError::WriteResourceLimit {
                    detail: "native private paths exceed 128 MiB".into(),
                })
        })?;
    if paths.len().saturating_add(records.len()) > 10_000 || bytes > 128 * 1024 * 1024 {
        return Err(StorageError::WriteResourceLimit {
            detail: "native private paths exceed their key cap".into(),
        });
    }
    paths.extend(records);
    Ok(())
}

/// Include owned stored bytes, publication copies, and canonical body copies.
pub(crate) fn memory_quote(runtime: &PackRuntime) -> StorageResult<u64> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native memory quote requires a write scope".into())
    })?;
    let stored = pending.data.lock().expect("native pending batch").bytes;
    let paths = pending
        .paths
        .lock()
        .expect("native private paths")
        .iter()
        .fold(0u64, |sum, record| {
            sum.saturating_add(record.body.len() as u64 + 1024)
        });
    Ok(stored
        .saturating_mul(4)
        .saturating_add(paths.saturating_mul(2)))
}

pub(super) fn validate(operations: &[HomeMutation], operation_limit: usize) -> StorageResult<()> {
    if operations.len() > operation_limit {
        return Err(StorageError::InvalidOperation(format!(
            "complete native batch exceeds {operation_limit} operations"
        )));
    }
    let mut bytes = 256u64;
    for operation in operations {
        if operation.collection.len() > 65531 || operation.key.len() > 65535 {
            return Err(StorageError::InvalidOperation(
                "native collection or key exceeds format bounds".into(),
            ));
        }
        bytes = bytes
            .checked_add(
                (160 + operation.collection.len()
                    + 2 * operation.key.len()
                    + operation.body.as_ref().map_or(0, Vec::len)) as u64,
            )
            .filter(|bytes| *bytes <= 128 * 1024 * 1024)
            .ok_or_else(|| {
                StorageError::InvalidOperation("complete native batch exceeds 128 MiB".into())
            })?;
    }
    Ok(())
}

/// Keep one complete logical job in one storage batch. Commit precedes dirty clear.
pub(crate) async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    request_scope(runtime, false, future).await
}

/// Keep the request's shutdown/defer resource with its complete stored batch.
pub(crate) fn retain(runtime: &PackRuntime, value: impl Send + 'static) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation(
            "native request resource requires one complete preparation scope".into(),
        )
    })?;
    pending
        .retained
        .lock()
        .expect("native request resources")
        .push(Box::new(value));
    Ok(())
}

/// External consumers see changes only after the complete root is durable.
pub(crate) fn after_commit(
    runtime: &PackRuntime,
    action: impl std::future::Future<Output = StorageResult<()>> + Send + 'static,
) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation(
            "native post-commit work requires one complete preparation scope".into(),
        )
    })?;
    pending
        .after_commit
        .lock()
        .expect("native post-commit work")
        .push(Box::pin(action));
    Ok(())
}
