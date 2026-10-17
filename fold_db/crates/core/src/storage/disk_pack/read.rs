use super::*;
use budget::call_error;
use laststore::disk_pack::home::HomeRead;

mod budget;
mod dependencies;
mod headers;
mod holds;
mod raw;
pub(crate) use headers::note_molecule_header;
mod overlay;
pub(crate) mod snapshots;

pub(super) struct ReadContext {
    pub(super) call: Arc<Mutex<HomeRead>>,
    identity: uuid::Uuid,
    failure: Arc<Mutex<Option<ReadFailure>>>,
    holds: Arc<holds::CallRecordHolds>,
    pub(super) accepted: Arc<Vec<Arc<transaction::PendingBatch>>>,
    snapshots: Arc<snapshots::ReadSnapshots>,
    pub(super) version: logical_paths::body::ReadVersion,
    dependencies: Arc<Mutex<validity::Dependencies>>,
    validity: Arc<Mutex<validity::Validity>>,
    pub(super) paths: Arc<Mutex<logical_paths::warm::ResolvedPaths>>,
    write_owner: Option<Arc<transaction::PendingBatch>>,
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
        READ.try_with(Arc::clone).ok().filter(|context| {
            context.identity == runtime.home.descriptor().identity
                && context
                    .validate_owner(transaction::PendingBatch::current(runtime).as_ref())
                    .is_ok()
        })
    }

    pub(super) fn active(runtime: &PackRuntime) -> bool {
        Self::captured(runtime).is_some()
    }

    pub(super) fn new(runtime: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        Self::capture(runtime, None)
    }

    fn capture(
        runtime: &Arc<PackRuntime>,
        write_owner: Option<Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if runtime.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native disk or cache publication failed; complete recovery is required".into(),
            ));
        }
        // Keep the root selector locked until its file lifetime hold exists.
        // Runtime maintenance must replace this selector before reclaim.
        let view = runtime.view.read().expect("native complete read view");
        let call = if write_owner.is_some() {
            runtime
                .home
                .read_for_write_preparation(view.current.clone(), &runtime.read_lease)
        } else {
            runtime
                .home
                .read_from_writer(view.current.clone(), budget(), &runtime.read_lease)
        }
        .map_err(map)?;
        let accepted = view.accepted.clone();
        let version = logical_paths::body::ReadVersion {
            generation: view.current.generation,
            accepted: if accepted.is_empty() {
                0
            } else {
                Arc::as_ptr(&accepted) as usize
            },
            revision: view.revision,
        };
        drop(view);
        Ok(Arc::new(Self {
            call: Arc::new(Mutex::new(call)),
            accepted,
            version,
            dependencies: Arc::new(Mutex::new(validity::Dependencies::default())),
            validity: Arc::clone(&runtime.validity),
            paths: Arc::new(Mutex::new(logical_paths::warm::ResolvedPaths::default())),
            snapshots: Arc::new(snapshots::ReadSnapshots::default()),
            identity: runtime.home.descriptor().identity,
            failure: Arc::new(Mutex::new(None)),
            holds: Arc::new(holds::CallRecordHolds::new(Arc::clone(&runtime.logical))),
            write_owner,
        }))
    }

    pub(super) async fn current(runtime: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        if let Ok(context) = READ.try_with(Arc::clone) {
            if context.identity == runtime.home.descriptor().identity {
                context.validate_owner(transaction::PendingBatch::current(runtime).as_ref())?;
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
                    version: context.version,
                    paths: Arc::clone(&context.paths),
                    dependencies: Arc::clone(&context.dependencies),
                    validity: Arc::clone(&context.validity),
                    write_owner: context.write_owner.clone(),
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

    fn owns_write(&self, pending: Option<&Arc<transaction::PendingBatch>>) -> bool {
        self.write_owner
            .as_ref()
            .zip(pending)
            .is_some_and(|(owner, pending)| Arc::ptr_eq(owner, pending))
    }

    pub(super) fn validate_owner(
        &self,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<()> {
        // Ordinary 10,000-key scopes also support generic pending KV overlays.
        // Only the larger preparation call binds its budget to one write owner.
        if self.write_owner.is_none() || self.owns_write(pending) {
            return Ok(());
        }
        let result = Err(StorageError::InvalidOperation(
            "native read scope cannot change its complete write owner".into(),
        ));
        self.remember(&result);
        result
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
    pending: Arc<transaction::PendingBatch>,
    future: impl std::future::Future<Output = T>,
) -> StorageResult<T> {
    let context = super::super::laststore::LastStoreKvStore::run_blocking(move || {
        ReadContext::capture(&runtime, Some(pending))
    })
    .await?;
    let scope = PackReadScope { context };
    let result = scope.run(future).await;
    if let Some(error) = scope.context.error() {
        return Err(error);
    }
    Ok(result)
}
