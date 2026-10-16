use super::*;
use std::collections::BTreeMap;

pub(crate) mod publication;
mod request;
pub(crate) use request::request_scope;
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
        validate(&operations)?;
        let mut data = self.data.lock().expect("native pending batch");
        let count = data
            .operations
            .len()
            .checked_add(operations.len())
            .filter(|count| *count <= 4096)
            .ok_or_else(|| {
                StorageError::InvalidOperation(
                    "complete native mutation exceeds 4096 storage operations".into(),
                )
            })?;
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

    fn freeze_operations(&self) -> Vec<HomeMutation> {
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
}

pub(super) fn validate(operations: &[HomeMutation]) -> StorageResult<()> {
    if operations.len() > 4096 {
        return Err(StorageError::InvalidOperation(
            "complete native batch exceeds 4096 operations".into(),
        ));
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
