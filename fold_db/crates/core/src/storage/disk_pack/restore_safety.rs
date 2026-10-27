//! An interrupted restore stays write-blocked until its counters validate.

use super::*;

pub(crate) const PENDING_KEY: &str = "__lastdb_counter_restore_pending_v1";

tokio::task_local! {
    static RESTORING: Arc<PackRuntime>;
}

pub(super) fn active(runtime: &PackRuntime) -> bool {
    RESTORING
        .try_with(|owner| std::ptr::eq(owner.as_ref(), runtime))
        .unwrap_or(false)
}

pub(crate) async fn raw_main(runtime: Arc<PackRuntime>) -> StorageResult<Arc<dyn KvStore>> {
    PackNamespacedStore { runtime }.open_namespace("main").await
}

pub(crate) async fn exclusive<T, E: From<StorageError>>(
    runtime: Arc<PackRuntime>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if active(&runtime) || transaction::PendingBatch::current(&runtime).is_some() {
        return Err(StorageError::InvalidOperation(
            "restore requires its own exclusive scope".into(),
        )
        .into());
    }
    let _owner = Arc::clone(&runtime.restore_gate).lock_owned().await;
    let gate = runtime.enter_prepare().await;
    if runtime.serving.load(Ordering::Acquire) {
        return Err(
            StorageError::InvalidOperation("restore requires an inactive target".into()).into(),
        );
    }
    runtime.restore_blocked.store(true, Ordering::Release);
    drop(gate);
    let owner = Arc::clone(&runtime);
    RESTORING
        .scope(runtime, async {
            // Older accepted writes finish before the first restore batch.
            // The disk owner still needs the ordinary preparation gate to flush.
            owner.flush().await?;
            future.await
        })
        .await
}

impl PackRuntime {
    pub(crate) async fn begin_serving(self: &Arc<Self>) -> StorageResult<()> {
        let _owner = Arc::clone(&self.restore_gate).lock_owned().await;
        let _gate = self.enter_prepare().await;
        self.require_restore_validated()?;
        self.serving.store(true, Ordering::Release);
        self.resume_native_cleanup();
        Ok(())
    }

    pub(crate) fn restore_validation_pending(&self) -> bool {
        self.restore_blocked.load(Ordering::Acquire)
    }

    pub(super) fn require_restore_validated(&self) -> StorageResult<()> {
        if self.restore_validation_pending() && !active(self) {
            return Err(StorageError::InvalidOperation(
                "restore counters are unverified; cleanup and writes remain blocked".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn finish_restore_validation(&self) -> StorageResult<()> {
        if !active(self) {
            return Err(StorageError::InvalidOperation(
                "restore validation has no owner".into(),
            ));
        }
        self.restore_blocked.store(false, Ordering::Release);
        Ok(())
    }
}
