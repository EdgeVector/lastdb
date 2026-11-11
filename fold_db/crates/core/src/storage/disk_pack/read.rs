use super::physical::{PhysicalRead, PhysicalRoot};
use super::transaction::{PendingBatch, PendingRecord};
use super::*;
use std::collections::BTreeMap;

const WRITE_READ_MAX_BYTES: usize = 1024 * 1024;
const WRITE_READ_MAX_VALUE_BYTES: usize = 16 * 1024;
type Coordinate = (String, String);

#[derive(Default)]
struct SavedWriteReads {
    values: BTreeMap<Coordinate, Option<Vec<u8>>>,
    bytes: usize,
}

#[derive(Default)]
struct WriteReads {
    saved: Mutex<SavedWriteReads>,
}

pub(in crate::storage::disk_pack) struct ReadContext {
    pub(in crate::storage::disk_pack) call: Arc<Mutex<PhysicalRead>>,
    identity: uuid::Uuid,
    failure: Arc<Mutex<Option<ReadFailure>>>,
    pub(in crate::storage::disk_pack) accepted: Arc<Vec<Arc<PendingBatch>>>,
    pub(in crate::storage::disk_pack) hidden_prefix: bool,
    pub(in crate::storage::disk_pack) revision: u64,
    current: PhysicalRoot,
    write_owner: Option<Arc<PendingBatch>>,
    write_reads: Option<Arc<WriteReads>>,
}

struct ReadFailure {
    resource: bool,
    detail: String,
}

tokio::task_local! {
    static READ: Arc<ReadContext>;
    static ITEM: Option<Arc<Mutex<Option<ReadFailure>>>>;
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn captured(runtime: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone).ok().filter(|context| {
            context.identity == runtime.home_identity()
                && context
                    .validate_owner(PendingBatch::current(runtime).as_ref())
                    .is_ok()
        })
    }

    pub(in crate::storage::disk_pack) fn active(runtime: &PackRuntime) -> bool {
        Self::captured(runtime).is_some()
    }

    pub(in crate::storage::disk_pack) fn new(
        runtime: &Arc<PackRuntime>,
    ) -> StorageResult<Arc<Self>> {
        Self::capture(runtime, None)
    }

    fn capture(
        runtime: &Arc<PackRuntime>,
        write_owner: Option<Arc<PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if runtime.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native disk or cache publication failed; complete recovery is required".into(),
            ));
        }
        let (preparation, hidden_prefix) = if write_owner.is_some() {
            runtime.preparation_prefix()
        } else {
            (Arc::new(Vec::new()), false)
        };
        runtime.adopt_merged_root()?;
        let view = runtime.view.read().expect("native complete read view");
        let mode = if write_owner.is_some() {
            super::physical::ReadMode::WritePreparation
        } else {
            super::physical::ReadMode::Ordinary(budget())
        };
        let call = runtime.physical_read(&view.current, mode);
        let accepted = preparation;
        let revision = view.revision;
        let current = view.current.clone();
        drop(view);
        let write_reads = write_owner
            .as_ref()
            .map(|_| Arc::new(WriteReads::default()));
        Ok(Arc::new(Self {
            call: Arc::new(Mutex::new(call)),
            accepted,
            hidden_prefix,
            revision,
            current,
            identity: runtime.home_identity(),
            failure: Arc::new(Mutex::new(None)),
            write_owner,
            write_reads,
        }))
    }

    pub(in crate::storage::disk_pack) async fn current(
        runtime: &Arc<PackRuntime>,
    ) -> StorageResult<Arc<Self>> {
        if let Ok(context) = READ.try_with(Arc::clone) {
            if context.identity == runtime.home_identity() {
                context.validate_owner(PendingBatch::current(runtime).as_ref())?;
                let failure = ITEM
                    .try_with(Clone::clone)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| Arc::new(Mutex::new(None)));
                return Ok(Arc::new(Self {
                    call: Arc::clone(&context.call),
                    identity: context.identity,
                    failure,
                    accepted: context.accepted.clone(),
                    hidden_prefix: context.hidden_prefix,
                    revision: context.revision,
                    current: context.current.clone(),
                    write_owner: context.write_owner.clone(),
                    write_reads: context.write_reads.clone(),
                }));
            }
            return Err(StorageError::InvalidOperation(
                "one native read scope cannot change home identity".into(),
            ));
        }
        let runtime = Arc::clone(runtime);
        super::run_blocking(move || Self::new(&runtime)).await
    }

    pub(in crate::storage::disk_pack) fn remember<T>(&self, result: &StorageResult<T>) {
        if let Err(error) = result {
            let mut failure = self.failure.lock().expect("native item failure");
            if failure.is_none() {
                *failure = Some(ReadFailure {
                    resource: matches!(error, StorageError::ReadResourceLimit { .. }),
                    detail: error.to_string(),
                });
            }
        }
    }

    pub(in crate::storage::disk_pack) fn remembered<T>(
        &self,
        result: StorageResult<T>,
    ) -> StorageResult<T> {
        self.remember(&result);
        result
    }

    pub(in crate::storage::disk_pack) fn owns_write(
        &self,
        pending: Option<&Arc<PendingBatch>>,
    ) -> bool {
        self.write_owner
            .as_ref()
            .zip(pending)
            .is_some_and(|(owner, pending)| Arc::ptr_eq(owner, pending))
    }

    pub(in crate::storage::disk_pack) fn validate_owner(
        &self,
        pending: Option<&Arc<PendingBatch>>,
    ) -> StorageResult<()> {
        if (self.write_owner.is_none() && pending.is_none()) || self.owns_write(pending) {
            return Ok(());
        }
        self.remembered(Err(StorageError::InvalidOperation(
            "native read scope cannot change its complete write owner".into(),
        )))
    }

    pub(in crate::storage::disk_pack) fn error(&self) -> Option<StorageError> {
        self.failure
            .lock()
            .expect("native item failure")
            .as_ref()
            .map(|failure| {
                if failure.resource {
                    StorageError::ReadResourceLimit {
                        detail: failure.detail.clone(),
                    }
                } else {
                    StorageError::BackendError(failure.detail.clone())
                }
            })
    }

    pub(in crate::storage::disk_pack) fn overlay_record(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        key: &str,
    ) -> Option<Arc<PendingRecord>> {
        pending
            .and_then(|batch| batch.lookup(collection, key))
            .or_else(|| {
                self.accepted
                    .iter()
                    .rev()
                    .find_map(|batch| batch.lookup(collection, key))
            })
    }

    pub(in crate::storage::disk_pack) fn overlay_rows(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let mut rows = BTreeMap::new();
        for batch in self.accepted.iter() {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        if let Some(batch) = pending {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        rows.into_values().collect()
    }

    pub(in crate::storage::disk_pack) fn reuse_disk_value(
        &self,
        call: &mut PhysicalRead,
        coordinate: &Coordinate,
    ) -> StorageResult<Option<Option<Vec<u8>>>> {
        let Some(reads) = &self.write_reads else {
            return Ok(None);
        };
        let saved = reads.saved.lock().map_err(|_| poisoned_write_reads())?;
        let Some(value) = saved.values.get(coordinate) else {
            return Ok(None);
        };
        let bytes = value.as_ref().map_or(0, Vec::len);
        self.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), bytes)?;
        Ok(Some(value.clone()))
    }

    pub(in crate::storage::disk_pack) fn remember_disk_value(
        &self,
        coordinate: &Coordinate,
        value: Option<&[u8]>,
    ) -> StorageResult<()> {
        let Some(reads) = &self.write_reads else {
            return Ok(());
        };
        let body_bytes = value.map_or(0, <[u8]>::len);
        if body_bytes > WRITE_READ_MAX_VALUE_BYTES {
            return Ok(());
        }
        let mut saved = reads.saved.lock().map_err(|_| poisoned_write_reads())?;
        let bytes = coordinate.0.len()
            + coordinate.1.len()
            + body_bytes
            + std::mem::size_of::<(Coordinate, Option<Vec<u8>>)>()
            + 8 * std::mem::size_of::<usize>();
        if bytes <= WRITE_READ_MAX_BYTES.saturating_sub(saved.bytes)
            && !saved.values.contains_key(coordinate)
        {
            saved
                .values
                .insert(coordinate.clone(), value.map(<[u8]>::to_vec));
            saved.bytes += bytes;
        }
        Ok(())
    }
}

fn poisoned_write_reads() -> StorageError {
    StorageError::BackendError("private write read results lock is poisoned".into())
}

/// One native read boundary shared by every child query or worker in a call.
#[derive(Clone)]
pub struct PackReadScope {
    context: Arc<ReadContext>,
}

impl PackReadScope {
    pub(crate) async fn for_runtime(runtime: &Arc<PackRuntime>) -> StorageResult<Self> {
        Ok(Self {
            context: ReadContext::current(runtime).await?,
        })
    }

    pub async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let future = Box::pin(future);
        READ.scope(
            Arc::clone(&self.context),
            ITEM.scope(Some(Arc::clone(&self.context.failure)), future),
        )
        .await
    }

    pub async fn run_shared<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let future = Box::pin(future);
        READ.scope(Arc::clone(&self.context), ITEM.scope(None, future))
            .await
    }

    pub async fn run_result<T, E>(
        &self,
        future: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
    {
        let result = self.run(future).await;
        if let Some(error) = self.context.error() {
            return Err(error.into());
        }
        result
    }
}

pub(in crate::storage::disk_pack) async fn write_scope<T>(
    runtime: Arc<PackRuntime>,
    pending: Arc<PendingBatch>,
    future: impl std::future::Future<Output = T>,
) -> StorageResult<T> {
    let context =
        super::run_blocking(move || ReadContext::capture(&runtime, Some(pending))).await?;
    let scope = PackReadScope { context };
    let result = scope.run(future).await;
    if let Some(error) = scope.context.error() {
        return Err(error);
    }
    Ok(result)
}
