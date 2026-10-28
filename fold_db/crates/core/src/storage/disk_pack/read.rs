use super::physical::{PhysicalRead, PhysicalRoot};
use super::*;
use budget::call_error;

mod atoms;
mod budget;
mod core_holds;
pub(super) mod memory;
mod overlay;
mod preload;
mod probes;
mod raw;
pub(crate) mod snapshots;

pub(super) struct ReadContext {
    pub(super) call: Arc<Mutex<PhysicalRead>>,
    identity: uuid::Uuid,
    failure: Arc<Mutex<Option<ReadFailure>>>,
    pub(super) core_model: crate::resident::CoreModelSnapshot,
    // Only private write preparation can inspect the existing flush log.
    pub(super) accepted: Arc<Vec<Arc<transaction::PendingBatch>>>,
    pub(super) hidden_prefix: bool,
    snapshots: Arc<snapshots::ReadSnapshots>,
    pub(super) revision: u64,
    current: PhysicalRoot,
    write_owner: Option<Arc<transaction::PendingBatch>>,
    _preload_charge: Option<Arc<crate::resident::core_keys::CoreKeyReservation>>,
    // Fields drop in declaration order. Drop captured model/source owners
    // before the final holds owner trims paths, so dead call allocations do
    // not force the purge to discard unrelated live records.
    core_holds: Arc<core_holds::CoreCallHolds>,
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
            context.identity == runtime.home_identity()
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
        let (preparation, hidden_prefix) = if write_owner.is_some() {
            runtime.preparation_prefix()
        } else {
            (Arc::new(Vec::new()), false)
        };
        runtime.adopt_merged_root();
        // Keep the root selector locked until its file lifetime hold exists.
        // Runtime maintenance must replace this selector before reclaim.
        let view = runtime.view.read().expect("native complete read view");
        let mode = if write_owner.is_some() {
            super::physical::ReadMode::WritePreparation
        } else {
            super::physical::ReadMode::Ordinary(budget())
        };
        let call = runtime.physical_read(&view.current, mode);
        let accepted = preparation;
        let core_model = runtime
            .core
            .lock()
            .expect("captured typed core model")
            .dirty_snapshot();
        let revision = view.revision;
        let current = view.current.clone();
        drop(view);
        Ok(Arc::new(Self {
            call: Arc::new(Mutex::new(call)),
            accepted,
            hidden_prefix,
            core_model,
            revision,
            current,
            snapshots: Arc::new(snapshots::ReadSnapshots::default()),
            identity: runtime.home_identity(),
            failure: Arc::new(Mutex::new(None)),
            core_holds: Arc::new(core_holds::CoreCallHolds::new(Arc::clone(&runtime.core))),
            write_owner,
            _preload_charge: None,
        }))
    }

    pub(super) async fn current(runtime: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        runtime.require_restore_validated()?;
        if let Ok(context) = READ.try_with(Arc::clone) {
            if context.identity == runtime.home_identity() {
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
                    core_holds: Arc::clone(&context.core_holds),
                    accepted: context.accepted.clone(),
                    hidden_prefix: context.hidden_prefix,
                    core_model: context.core_model.clone(),
                    snapshots: Arc::clone(&context.snapshots),
                    revision: context.revision,
                    current: context.current.clone(),
                    write_owner: context.write_owner.clone(),
                    _preload_charge: context._preload_charge.clone(),
                }));
            }
            return Err(StorageError::InvalidOperation(
                "one native read scope cannot change home identity".into(),
            ));
        }
        let runtime = Arc::clone(runtime);
        super::run_blocking(move || Self::new(&runtime)).await
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

    pub(super) fn hold_core(
        &self,
        core: &mut crate::resident::CoreWarmSet,
        paths: &[crate::resident::CorePath],
    ) -> StorageResult<()> {
        self.core_holds.hold(core, paths)
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
        // Only private preparation reads the flush log and binds its budget
        // to one write owner. Ordinary reads use their captured typed core view.
        if (self.write_owner.is_none() && pending.is_none()) || self.owns_write(pending) {
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
        Self::for_runtime(&runtime).await.map(Some)
    }

    pub(crate) async fn for_runtime(runtime: &Arc<PackRuntime>) -> StorageResult<Self> {
        Ok(Self {
            context: ReadContext::current(runtime).await?,
        })
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
    let context =
        super::run_blocking(move || ReadContext::capture(&runtime, Some(pending))).await?;
    let scope = PackReadScope { context };
    let result = scope.run(future).await;
    if let Some(error) = scope.context.error() {
        return Err(error);
    }
    Ok(result)
}
