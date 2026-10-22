use super::*;
use std::collections::BTreeMap;

mod accounting;
pub(crate) mod blocked;
mod effects;
mod empty;
pub(crate) mod publication;
mod request;
pub(crate) use effects::{stage_absences, stage_core_memory, stage_paths};
pub(crate) use request::{background_scope, request_scope};
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
    raw_memory_reservation: Mutex<Option<Arc<crate::memory_budget::DeferReservation>>>,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    durable_only: AtomicBool,
    paths: Mutex<Vec<super::logical_paths::LogicalPathRecord>>,
    absences: Mutex<Vec<super::logical_paths::LogicalPathAbsence>>,
    definitions: Mutex<BTreeMap<(usize, String), crate::resident::PreparedCoreMemory>>,
    pub(super) path_protection: Mutex<Option<Arc<super::logical_paths::PathProtection>>>,
    source_receipts: Mutex<Option<Arc<Mutex<super::logical_paths::source::Sources>>>>,
    preparation_limit: Mutex<Option<String>>,
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
            raw_memory_reservation: Mutex::new(None),
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            durable_only: AtomicBool::new(false),
            paths: Mutex::new(Vec::new()),
            absences: Mutex::new(Vec::new()),
            definitions: Mutex::new(BTreeMap::new()),
            path_protection: Mutex::new(None),
            source_receipts: Mutex::new(None),
            preparation_limit: Mutex::new(None),
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

    pub(super) fn source_receipts(&self) -> Arc<Mutex<super::logical_paths::source::Sources>> {
        Arc::clone(
            self.source_receipts
                .lock()
                .expect("native write source receipts")
                .get_or_insert_with(|| {
                    Arc::new(Mutex::new(super::logical_paths::source::Sources::default()))
                }),
        )
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
                .filter(|bytes| *bytes <= 128 * 1024 * 1024 - 256)
                .ok_or_else(|| {
                    self.remember_preparation_limit(StorageError::MutationLimit {
                        detail: "complete native mutation exceeds 128 MiB".into(),
                    })
                })?;
        }
        let additional = count - data.operations.len();
        data.operations
            .try_reserve(additional)
            .map_err(|_| StorageError::BackendError("native mutation allocation refused".into()))?;
        effects::forget_superseded(self, &operations);
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
        drop(
            self.source_receipts
                .lock()
                .expect("native write source receipts")
                .take(),
        );
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

    pub(super) fn latest_records(&self) -> Vec<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending graph records");
        data.latest
            .values()
            .map(|slot| Arc::clone(&data.operations[*slot]))
            .collect()
    }

    pub(super) fn changed_coordinates(&self) -> Vec<(String, String)> {
        let data = self.data.lock().expect("native pending changes");
        data.latest.keys().cloned().collect()
    }

    /// Verify final staged bytes, not the request size when quota was retained.
    pub(super) fn has_reserved_raw_memory(&self) -> bool {
        let required = self.memory_quote();
        self.raw_memory_reservation
            .lock()
            .expect("native raw memory reservation")
            .as_ref()
            .is_some_and(|reservation| reservation.bytes() >= required)
    }

    fn memory_quote(&self) -> u64 {
        let stored = self.data.lock().expect("native pending batch").bytes;
        let paths = self
            .paths
            .lock()
            .expect("native private paths")
            .iter()
            .fold(0u64, |sum, record| {
                sum.saturating_add(record.retained_bytes() as u64)
            });
        let absences = self
            .absences
            .lock()
            .expect("native private absences")
            .iter()
            .fold(0u64, |sum, record| {
                sum.saturating_add(record.bytes() as u64)
            });
        stored
            .saturating_mul(4)
            .saturating_add(paths.saturating_add(absences).saturating_mul(2))
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

/// Include owned stored bytes, publication copies, and canonical body copies.
pub(crate) fn memory_quote(runtime: &PackRuntime) -> StorageResult<u64> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native memory quote requires a write scope".into())
    })?;
    Ok(pending.memory_quote())
}

/// Only this typed quota can authorize read-held raw write bodies before disk.
pub(crate) fn retain_raw_memory_reservation(
    runtime: &PackRuntime,
    reservation: crate::memory_budget::DeferReservation,
) -> StorageResult<()> {
    let pending = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("native raw memory quota requires a write scope".into())
    })?;
    *pending
        .raw_memory_reservation
        .lock()
        .expect("native raw memory reservation") = Some(Arc::new(reservation));
    Ok(())
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
            .filter(|bytes| *bytes <= 128 * 1024 * 1024)
            .ok_or_else(|| StorageError::MutationLimit {
                detail: "complete native batch exceeds 128 MiB".into(),
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
