use super::*;
use laststore::disk_pack::home::HomeRead;

mod holds;
mod overlay;
pub(crate) mod snapshots;

pub(super) struct ReadContext {
    pub(super) call: Arc<Mutex<HomeRead>>,
    identity: uuid::Uuid,
    failure: Arc<Mutex<Option<ReadFailure>>>,
    holds: Arc<holds::CallRecordHolds>,
    pub(super) accepted: Option<Arc<transaction::PendingBatch>>,
    snapshots: Arc<snapshots::ReadSnapshots>,
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
    pub(super) fn captured(runtime: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone)
            .ok()
            .filter(|context| context.identity == runtime.home.descriptor().identity)
    }

    pub(super) fn active(runtime: &PackRuntime) -> bool {
        READ.try_with(|context| context.identity == runtime.home.descriptor().identity)
            .unwrap_or(false)
    }

    pub(super) fn new(runtime: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        if runtime.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native disk or cache publication failed; complete recovery is required".into(),
            ));
        }
        // Keep the root selector locked until its file lifetime hold exists.
        // Runtime maintenance must replace this selector before reclaim.
        let view = runtime.view.read().expect("native complete read view");
        let call = runtime
            .home
            .read_committed(view.current.clone(), budget())
            .map_err(map)?;
        let accepted = view.accepted.clone();
        drop(view);
        Ok(Arc::new(Self {
            call: Arc::new(Mutex::new(call)),
            accepted,
            snapshots: Arc::new(snapshots::ReadSnapshots::default()),
            identity: runtime.home.descriptor().identity,
            failure: Arc::new(Mutex::new(None)),
            holds: Arc::new(holds::CallRecordHolds::new(Arc::clone(&runtime.logical))),
        }))
    }

    pub(super) async fn current(runtime: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        if let Ok(context) = READ.try_with(Arc::clone) {
            if context.identity == runtime.home.descriptor().identity {
                let failure = ITEM
                    .try_with(Clone::clone)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| Arc::new(Mutex::new(None)));
                return Ok(Arc::new(Self {
                    call: Arc::clone(&context.call),
                    identity: context.identity,
                    failure,
                    holds: Arc::clone(&context.holds),
                    accepted: context.accepted.clone(),
                    snapshots: Arc::clone(&context.snapshots),
                }));
            }
            return Err(StorageError::InvalidOperation(
                "one native read scope cannot change home identity".into(),
            ));
        }
        let runtime = Arc::clone(runtime);
        super::super::laststore::LastStoreKvStore::run_blocking(move || Self::new(&runtime)).await
    }

    pub(super) fn remember<T>(&self, result: &StorageResult<T>) {
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

    pub(super) fn hold_record(
        &self,
        logical: &mut LogicalResidentSet,
        collection: &str,
        key: &str,
    ) -> StorageResult<()> {
        self.holds.hold(logical, collection, key)
    }

    fn error(&self) -> Option<StorageError> {
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
}

/// One native read boundary shared by every child query or worker in a call.
#[derive(Clone)]
pub struct PackReadScope {
    context: Arc<ReadContext>,
}

impl PackReadScope {
    /// Capture a native root when this store uses packs. Legacy stores return None.
    pub async fn for_store(store: &Arc<dyn NamespacedStore>) -> StorageResult<Option<Self>> {
        let Some(runtime) = store.disk_pack_runtime() else {
            return Ok(None);
        };
        Ok(Some(Self {
            context: ReadContext::current(&runtime).await?,
        }))
    }

    /// Attach this same root and budget to an asynchronous child task.
    pub async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let future = Box::pin(future);
        READ.scope(
            Arc::clone(&self.context),
            ITEM.scope(Some(Arc::clone(&self.context.failure)), future),
        )
        .await
    }

    /// Share only the root and budget; each child query receives its own failure state.
    pub async fn run_shared<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let future = Box::pin(future);
        READ.scope(Arc::clone(&self.context), ITEM.scope(None, future))
            .await
    }

    /// Do not let an inner fallback turn a failed native read into an empty success.
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

/// Capture the latest complete root after the exclusive write gate is held.
/// An older enclosing query must not supply the comparison state for a mutation.
pub(super) async fn write_scope<T>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = T>,
) -> StorageResult<T> {
    let context =
        super::super::laststore::LastStoreKvStore::run_blocking(move || ReadContext::new(&runtime))
            .await?;
    let scope = PackReadScope { context };
    let result = scope.run(future).await;
    if let Some(error) = scope.context.error() {
        return Err(error);
    }
    Ok(result)
}

impl PackRuntime {
    pub(super) fn raw_get(
        &self,
        context: &Arc<ReadContext>,
        collection: &str,
        key: &str,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .raw_many(
                context,
                &[(collection.to_string(), key.to_string())],
                pending,
            )?
            .pop()
            .expect("one native coordinate"))
    }

    pub(super) fn raw_many(
        &self,
        context: &Arc<ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        laststore::record_requested_keys(coordinates.len() as u64);
        let result = self.raw_many_inner(context, coordinates, pending);
        context.remember(&result);
        result
    }

    fn raw_many_inner(
        &self,
        context: &Arc<ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if coordinates.len() > 10000 {
            return Err(StorageError::ReadResourceLimit {
                detail: "native batch exceeds 10000 keys".into(),
            });
        }
        let mut call = crate::read_receipts::lock(&context.call);
        let mut out = vec![None; coordinates.len()];
        let mut misses = Vec::new();
        for (slot, (collection, key)) in coordinates.iter().enumerate() {
            if let Some(record) = context.overlay_record(pending, collection, key) {
                call.account_memory_record(key.len(), record.body.as_ref().map_or(0, Vec::len))
                    .map_err(map)?;
                out[slot] = record.body.clone();
            } else {
                misses.push(slot);
            }
        }
        let addresses: Vec<_> = misses
            .iter()
            .map(|slot| coordinates[*slot].clone())
            .collect();
        let results = call.get_many(&addresses).map_err(map)?;
        for (slot, result) in misses.into_iter().zip(results) {
            out[slot] = result.map_err(map)?;
        }
        Ok(out)
    }
}
