//! An interrupted restore stays write-blocked until its counters validate.

use super::*;
use std::fs::{File, OpenOptions};

pub(crate) const PENDING_KEY: &str = "__lastdb_counter_restore_pending_v1";
pub(super) const PENDING_FILE: &str = "counter-restore-validation-v1";
const PENDING_BYTES: &[u8] = b"version=1\n";

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

pub(super) fn pending(home: &DiskHome, path: &Path) -> StorageResult<bool> {
    let file = path.parent().map(|p| p.join(PENDING_FILE));
    if file
        .as_deref()
        .map(read_marker)
        .transpose()?
        .unwrap_or(false)
    {
        return Ok(true);
    }
    let mut reader = home.read(budget()).map_err(map)?;
    let key = PENDING_KEY.as_bytes();
    let collection = super::native_routing::collection_for_key(key);
    let key = super::native_routing::encode(key);
    Ok(reader
        .get_bounded(collection, &key, 8192)
        .map_err(map)?
        .is_some())
}

/// A missing target of a symlink is not an absent restore marker. Keep errors
/// fail-closed and bound the read even if a damaged marker grows unexpectedly.
fn read_marker(path: &Path) -> StorageResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(StorageError::InvalidOperation(
                "invalid restore validation marker".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    let bytes = super::restore::existing_bytes(path, PENDING_BYTES.len() as u64)?;
    if !PENDING_BYTES.starts_with(&bytes) {
        return Err(StorageError::InvalidOperation(
            "invalid restore validation marker".into(),
        ));
    }
    // A crash can leave a prefix after create/write; that still requires validation.
    Ok(true)
}

pub(super) fn mark_native(root: &Path) -> StorageResult<()> {
    let path = root.join(PENDING_FILE);
    if path.try_exists()? && !std::fs::symlink_metadata(&path)?.is_file() {
        return Err(StorageError::InvalidOperation(
            "invalid restore validation marker".into(),
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    super::restore::finish_file(&mut file, PENDING_BYTES)?;
    File::open(root)?.sync_all()?;
    Ok(())
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
        if let Some(home) = &self.node_home {
            let marker = home.root.join(PENDING_FILE);
            if read_marker(&marker)? {
                std::fs::remove_file(marker)?;
                File::open(&home.root)?.sync_all()?;
            }
        }
        self.restore_blocked.store(false, Ordering::Release);
        Ok(())
    }
}
