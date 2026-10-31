use super::*;
use std::collections::BTreeMap;

mod accounting;
pub(crate) mod blocked;
pub(super) mod effects;
mod empty;
mod leases;
mod logical_capture;
pub(crate) use logical_capture::mark_logical_capture;
pub(super) use logical_capture::require_captured_changes;
pub(crate) mod publication;
mod request;
mod resources;
pub(crate) use effects::{
    prepared_core_memory, retain_raw_memory_reservation, stage_core_memory, stage_paths,
};
pub(crate) use leases::lease;
pub(crate) use request::{background_scope, request_scope};
pub(crate) use resources::with_resource;
type AfterCommit = std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send>>;

/// One serialized operation in the existing private preparation and flush log.
#[derive(Debug)]
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
    resources: Mutex<Vec<resources::Resource>>,
    leases: Mutex<BTreeMap<String, std::sync::Weak<dyn std::any::Any + Send + Sync>>>,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    durable_only: AtomicBool,
    logical_capture_ready: AtomicBool,
    flush_blocked_us: std::sync::atomic::AtomicU64,
    /// The database's typed memory effects of this request: staged paths,
    /// definitions, their reservation, and the raw memory reservation.
    memory: Box<dyn BatchMemory>,
    preparation_limit: Mutex<Option<String>>,
    operation_limit: usize,
}

tokio::task_local! {
    static WRITE: Arc<PendingBatch>;
}

impl PendingBatch {
    fn new(runtime: &PackRuntime) -> Self {
        Self {
            identity: runtime.home_identity(),
            data: Mutex::new(PendingData::default()),
            publications: publication::PendingPublications::default(),
            retained: Mutex::new(Vec::new()),
            resources: Mutex::new(resources::capture(runtime)),
            leases: Mutex::new(BTreeMap::new()),
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            durable_only: AtomicBool::new(false),
            logical_capture_ready: AtomicBool::new(false),
            flush_blocked_us: std::sync::atomic::AtomicU64::new(0),
            memory: runtime.memory.open_batch(),
            preparation_limit: Mutex::new(None),
            operation_limit: runtime.batch_operation_limit(),
        }
    }
    pub(super) fn current(runtime: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|batch| batch.identity == runtime.home_identity())
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
        validate(&operations, self.operation_limit)
            .map_err(|error| self.remember_preparation_limit(error))?;
        let mut data = self.data.lock().expect("native pending batch");
        let count = data
            .operations
            .len()
            .checked_add(operations.len())
            .filter(|count| *count <= self.operation_limit)
            .ok_or_else(|| {
                self.remember_preparation_limit(accounting::refusal(
                    &data,
                    &operations,
                    self.operation_limit,
                ))
            })?;
        accounting::record_stage(&data, &operations, self.operation_limit);
        let mut bytes = data.bytes;
        for operation in &operations {
            bytes = bytes
                .checked_add(
                    (160 + operation.collection.len()
                        + 2 * operation.key.len()
                        + operation.body.as_ref().map_or(0, Vec::len)) as u64,
                )
                .filter(|bytes| {
                    *bytes <= lastdb_storage_policy::native_write::PENDING_MEMORY_BYTES - 256
                })
                .ok_or_else(|| {
                    self.remember_preparation_limit(StorageError::MutationLimit {
                        detail: "complete native mutation exceeds pending memory allowance".into(),
                    })
                })?;
        }
        let additional = count - data.operations.len();
        data.operations
            .try_reserve(additional)
            .map_err(|_| StorageError::BackendError("native mutation allocation refused".into()))?;
        self.memory.forget_superseded(&operations);
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
        // Source proofs are bounded preparation scratch, never accepted bodies.
        self.memory.freeze();
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

    pub(super) fn changed_coordinates(&self) -> Vec<(String, String)> {
        let data = self.data.lock().expect("native pending changes");
        data.latest.keys().cloned().collect()
    }

    pub(super) fn note_flush_blocked(&self, took: std::time::Duration) {
        self.flush_blocked_us.store(
            took.as_micros().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(super) fn flush_blocked(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.flush_blocked_us.load(Ordering::Acquire))
    }

    /// Verify final staged bytes, not the request size when quota was retained.
    pub(super) fn has_reserved_raw_memory(&self) -> bool {
        let required = self.memory_quote();
        self.memory
            .reserved_bytes()
            .is_some_and(|bytes| bytes >= required)
    }

    fn memory_quote(&self) -> u64 {
        let stored = self.data.lock().expect("native pending batch").bytes;
        let paths = self.memory.path_bytes();
        stored
            .saturating_mul(4)
            .saturating_add(paths.saturating_mul(2))
    }

    pub(super) fn finish_publications(&self) -> StorageResult<()> {
        self.publications.finish()
    }

    /// Publish the request's reserved typed effects. The caller holds the
    /// view write lock.
    pub(super) fn publish_memory(&self) -> StorageResult<()> {
        self.memory.publish()
    }

    /// Release the request's reserved typed paths after its publications.
    pub(super) fn release_memory(&self) {
        self.memory.release();
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

/// Include owned stored bytes, publication copies, and canonical body copies.
pub(crate) fn memory_quote(runtime: &PackRuntime) -> StorageResult<u64> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native memory quote requires a write scope".into())
    })?;
    Ok(pending.memory_quote())
}

pub(super) fn validate(operations: &[HomeMutation], operation_limit: usize) -> StorageResult<()> {
    batch_bytes(operations, operation_limit).map(|_| ())
}

/// Conservative complete-request charge, including fixed preparation overhead.
pub(super) fn batch_bytes(
    operations: &[HomeMutation],
    operation_limit: usize,
) -> StorageResult<u64> {
    if operations.len() > operation_limit {
        return Err(StorageError::MutationLimit {
            detail: format!("complete native batch exceeds {operation_limit} operations"),
        });
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
            .filter(|bytes| *bytes <= lastdb_storage_policy::native_write::PENDING_MEMORY_BYTES)
            .ok_or_else(|| StorageError::MutationLimit {
                detail: "complete native batch exceeds pending memory allowance".into(),
            })?;
    }
    Ok(bytes)
}

/// Keep one complete logical job in one storage batch. Commit precedes dirty clear.
pub(crate) async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    request_scope(runtime, future).await
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
