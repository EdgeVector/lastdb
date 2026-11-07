//! Durable holds on the existing shared record and file roots.

use super::{invalid, run_capture, run_source::RunSource, PackRuntime, RunBackupProfile};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::{map, run_blocking};
use crate::storage::error::{StorageError, StorageResult};
use laststore::disk_run::home::{CapturedRunHome, SHARED_BACKUP_DIR};
use std::fs::File;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

pub struct SharedRunCapture {
    source: RunSource,
    _admission: NativeCloudWorkGuard,
}

impl PackRuntime {
    /// Retain the original main owner lock through a stopped data-directory exchange.
    pub fn retain_rejoin_source_owner(&self) -> StorageResult<Arc<File>> {
        self.require_direct_shared_home()?;
        self.physical.home.retain_owner_lock().map_err(map)
    }

    /// Hold only the device-private generation for an offline rejoin.
    pub async fn capture_rejoin_private(
        self: &Arc<Self>,
        destination: &Path,
    ) -> StorageResult<CapturedRunHome> {
        self.require_direct_shared_home()?;
        let gate = self.enter_write().await;
        self.require_initialized()?;
        if self.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(invalid("shared rejoin writer needs recovery"));
        }
        let private = self
            .physical
            .private_home()
            .cloned()
            .ok_or_else(|| invalid("shared rejoin private home"))?;
        let destination = destination.to_path_buf();
        let holds = shared_holds(self);
        run_blocking(move || {
            let _gate = gate;
            require_no_shared_holds(&holds)?;
            private.capture_files(&destination).map_err(map)
        })
        .await
    }

    /// Select logical positions and hold their exact files under one write barrier.
    pub(crate) async fn freeze_shared_checkpoint<T, E>(
        self: &Arc<Self>,
        id: uuid::Uuid,
        key: &[u8; 32],
        cut: impl Future<Output = Result<Option<T>, E>>,
    ) -> Result<Option<(T, SharedRunCapture)>, E>
    where
        E: From<StorageError>,
    {
        self.require_direct_shared_home()?;
        if id.is_nil()
            || crate::storage::disk_pack::transaction::PendingBatch::current(self).is_some()
        {
            return Err(invalid("shared snapshot capture admission").into());
        }
        let admission = NativeCloudWorkGuard::acquire()?;
        let gate = self.enter_write().await;
        self.require_initialized()?;
        if self.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(invalid("shared snapshot writer needs recovery").into());
        }
        let Some(cut) = cut.await? else {
            return Ok(None);
        };
        let runtime = Arc::clone(self);
        let key = Zeroizing::new(*key);
        let capture = crate::storage::disk_pack::run_blocking(move || {
            let root = shared_hold(&runtime, id);
            if root.try_exists()? {
                RunSource::release_saved(&root)?;
            }
            run_capture::capture_pair(&runtime, &root)?;
            drop(gate);
            open_capture(runtime, root, &key, admission)
        })
        .await?;
        Ok(Some((cut, capture)))
    }

    /// Reopen the retained files after an interrupted upload.
    pub async fn capture_shared_checkpoint_files(
        self: &Arc<Self>,
        id: uuid::Uuid,
        key: &[u8; 32],
        resume: bool,
    ) -> StorageResult<SharedRunCapture> {
        self.require_direct_shared_home()?;
        if id.is_nil() || !resume {
            return Err(invalid("shared snapshot requires a saved file hold"));
        }
        let admission = NativeCloudWorkGuard::acquire()?;
        let runtime = Arc::clone(self);
        let key = Zeroizing::new(*key);
        crate::storage::disk_pack::run_blocking(move || {
            let root = shared_hold(&runtime, id);
            run_capture::require_shared_hold(&root)?;
            open_capture(runtime, root, &key, admission)
        })
        .await
    }

    /// Thaw the frozen generation after the publication has a terminal result.
    pub async fn release_shared_checkpoint_files(
        self: &Arc<Self>,
        id: uuid::Uuid,
    ) -> StorageResult<()> {
        self.require_direct_shared_home()?;
        if id.is_nil() {
            return Err(invalid("shared snapshot file hold identifier"));
        }
        let gate = self.enter_write().await;
        let root = shared_hold(self, id);
        crate::storage::disk_pack::run_blocking(move || {
            let _gate = gate;
            RunSource::release_saved(&root)
        })
        .await
    }
}

fn shared_hold(runtime: &PackRuntime, id: uuid::Uuid) -> PathBuf {
    shared_holds(runtime).join(id.to_string())
}

fn shared_holds(runtime: &PackRuntime) -> PathBuf {
    runtime
        .physical
        .path()
        .join(SHARED_BACKUP_DIR)
        .join(super::HOLDS_DIR)
}

fn require_no_shared_holds(path: &Path) -> StorageResult<()> {
    let mut entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if entries.next().transpose()?.is_some() {
        return Err(invalid("finish the shared snapshot before rejoining"));
    }
    Ok(())
}

fn open_capture(
    runtime: Arc<PackRuntime>,
    root: PathBuf,
    key: &[u8; 32],
    admission: NativeCloudWorkGuard,
) -> StorageResult<SharedRunCapture> {
    Ok(SharedRunCapture {
        source: RunSource::open(runtime, root, key)?,
        _admission: admission,
    })
}

impl SharedRunCapture {
    pub fn profile(&self) -> StorageResult<RunBackupProfile> {
        self.source.profile()
    }

    pub fn files(&self) -> impl Iterator<Item = (&Path, u64)> {
        self.source
            .files
            .iter()
            .map(|file| (file.path.as_path(), file.bytes))
    }

    pub fn destination(
        &self,
        path: &Path,
    ) -> StorageResult<cloud_backup_protocol::v2::Destination> {
        super::run_backup_destination(
            path.to_str()
                .ok_or_else(|| invalid("shared snapshot file path"))?,
        )
    }

    pub fn open_file(&self, path: &Path) -> StorageResult<File> {
        if self
            .source
            .files
            .binary_search_by(|file| file.path.as_path().cmp(path))
            .is_err()
        {
            return Err(invalid("shared snapshot file is outside its inventory"));
        }
        self.source.reader(path)
    }
}
