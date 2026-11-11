use super::record_warm::WrittenRecord;
use super::*;
use std::collections::BTreeMap;

pub(crate) mod publication;
mod request;
pub(crate) use request::{background_scope, blocked, request_scope};

type AfterCommit = std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send>>;

/// One serialized operation in the existing private preparation and flush log.
#[derive(Debug)]
pub(in crate::storage::disk_pack) struct PendingRecord {
    pub(in crate::storage::disk_pack) collection: String,
    pub(in crate::storage::disk_pack) key: String,
    pub(in crate::storage::disk_pack) body: Option<Vec<u8>>,
}

#[derive(Default)]
pub(in crate::storage::disk_pack::transaction) struct PendingData {
    pub(in crate::storage::disk_pack::transaction) operations: Vec<Arc<PendingRecord>>,
    pub(in crate::storage::disk_pack::transaction) latest: BTreeMap<(String, String), usize>,
    bytes: u64,
    max_body_bytes: usize,
}

#[derive(Default)]
struct WarmRecords {
    records: Vec<WrittenRecord>,
    bytes: usize,
}

pub(in crate::storage) struct PendingBatch {
    identity: uuid::Uuid,
    data: Mutex<PendingData>,
    publications: publication::PendingPublications,
    after_commit: Mutex<Vec<AfterCommit>>,
    frozen: AtomicBool,
    flush_blocked_us: std::sync::atomic::AtomicU64,
    pub(in crate::storage::disk_pack::transaction) preparation_limit: Mutex<Option<String>>,
    operation_limit: usize,
    warm_records: Mutex<WarmRecords>,
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
            after_commit: Mutex::new(Vec::new()),
            frozen: AtomicBool::new(false),
            flush_blocked_us: std::sync::atomic::AtomicU64::new(0),
            preparation_limit: Mutex::new(None),
            operation_limit: runtime.batch_operation_limit(),
            warm_records: Mutex::new(WarmRecords::default()),
        }
    }

    pub(in crate::storage) fn current(runtime: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|batch| batch.identity == runtime.home_identity())
    }

    pub(in crate::storage::disk_pack::transaction) fn is_empty(&self) -> StorageResult<bool> {
        let unchanged = self
            .data
            .lock()
            .expect("native pending batch")
            .operations
            .is_empty()
            && self
                .after_commit
                .lock()
                .expect("native post-commit work")
                .is_empty();
        Ok(unchanged && self.publications.is_empty()?)
    }

    pub(in crate::storage::disk_pack) fn stage_warm_records(
        &self,
        records: Vec<WrittenRecord>,
        limit: usize,
    ) {
        let mut staged = self.warm_records.lock().expect("native warm records");
        for record in records {
            let Some(bytes) = staged
                .bytes
                .checked_add(record.bytes())
                .filter(|bytes| *bytes <= limit)
            else {
                break;
            };
            staged.bytes = bytes;
            staged.records.push(record);
        }
    }

    pub(in crate::storage::disk_pack) fn take_warm_records(&self) -> Vec<WrittenRecord> {
        let mut staged = self.warm_records.lock().expect("native warm records");
        staged.bytes = 0;
        std::mem::take(&mut staged.records)
    }

    pub(in crate::storage::disk_pack) fn lookup(
        &self,
        collection: &str,
        key: &str,
    ) -> Option<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .get(&(collection.to_string(), key.to_string()))
            .map(|&at| Arc::clone(&data.operations[at]))
    }

    pub(in crate::storage::disk_pack) fn operation_limit(&self) -> usize {
        self.operation_limit
    }

    pub(in crate::storage::disk_pack) fn matching(
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

    fn require_final_format(&self) -> StorageResult<()> {
        let data = self.data.lock().expect("native final format admission");
        crate::storage::format::records::require_final_record_sizes(
            data.max_body_bytes,
            data.latest.values().filter_map(|&at| {
                let record = &data.operations[at];
                record
                    .body
                    .as_ref()
                    .map(|body| (record.collection.as_str(), record.key.as_str(), body.len()))
            }),
        )
    }

    pub(in crate::storage::disk_pack) fn stage(
        &self,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<()> {
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
                self.remember_preparation_limit(publication::refusal(
                    &data,
                    &operations,
                    self.operation_limit,
                ))
            })?;
        publication::record_stage(&data, &operations, self.operation_limit);
        let mut bytes = data.bytes;
        let mut max_body_bytes = data.max_body_bytes;
        for operation in &operations {
            let body_bytes = operation.body.as_ref().map_or(0, Vec::len);
            max_body_bytes = max_body_bytes.max(body_bytes);
            bytes = bytes
                .checked_add(
                    (160 + operation.collection.len() + 2 * operation.key.len() + body_bytes)
                        as u64,
                )
                .filter(|bytes| {
                    *bytes <= lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES - 256
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
        data.max_body_bytes = max_body_bytes;
        Ok(())
    }

    pub(in crate::storage::disk_pack::transaction) fn freeze_operations(
        &self,
    ) -> Vec<HomeMutation> {
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

    pub(in crate::storage::disk_pack) fn changed_coordinates(&self) -> Vec<(String, String)> {
        let data = self.data.lock().expect("native pending changes");
        data.latest.keys().cloned().collect()
    }

    pub(in crate::storage::disk_pack) fn note_flush_blocked(&self, took: std::time::Duration) {
        self.flush_blocked_us.store(
            took.as_micros().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(in crate::storage::disk_pack::transaction) fn flush_blocked(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.flush_blocked_us.load(Ordering::Acquire))
    }

    pub(in crate::storage::disk_pack) fn finish_publications(&self) -> StorageResult<()> {
        self.publications.finish()
    }

    pub(in crate::storage::disk_pack) async fn deliver(&self) -> StorageResult<()> {
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

fn validate(operations: &[HomeMutation], operation_limit: usize) -> StorageResult<()> {
    batch_bytes(operations, operation_limit).map(|_| ())
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
            .filter(|bytes| {
                *bytes <= lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES
            })
            .ok_or_else(|| StorageError::MutationLimit {
                detail: "complete native batch exceeds pending memory allowance".into(),
            })?;
    }
    Ok(bytes)
}

pub(crate) async fn scope<T, E>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    request_scope(runtime, future).await
}

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
