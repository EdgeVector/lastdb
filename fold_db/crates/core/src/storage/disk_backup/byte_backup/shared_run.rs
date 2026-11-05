//! Hold exact files from a private native home produced by a filtered export.

use super::{invalid, run_capture, run_source::RunSource, PackRuntime, RunBackupProfile};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::error::StorageResult;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

pub struct SharedRunCapture {
    source: RunSource,
    _admission: NativeCloudWorkGuard,
}

impl PackRuntime {
    /// This capability holds the complete native root. Its owner supplies a
    /// private filtered home containing only the recoverable shared state.
    pub async fn capture_shared_checkpoint_files(
        self: &Arc<Self>,
        id: uuid::Uuid,
        key: &[u8; 32],
        resume: bool,
    ) -> StorageResult<SharedRunCapture> {
        if id.is_nil()
            || crate::storage::disk_pack::transaction::PendingBatch::current(self).is_some()
        {
            return Err(invalid("shared checkpoint capture admission"));
        }
        self.flush_native().await?;
        let admission = NativeCloudWorkGuard::acquire()?;
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        let key = Zeroizing::new(*key);
        crate::storage::disk_pack::run_blocking(move || {
            runtime.require_initialized()?;
            crate::storage::disk_pack::cas_delete::require_clear(&runtime.physical)?;
            if runtime.failed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(invalid("shared checkpoint native recovery"));
            }
            let root = run_capture::held_root(&runtime, id);
            if resume {
                run_capture::require_shared_hold(&root)?;
            } else {
                run_capture::capture_pair(&runtime, &root)?;
            }
            drop(gate);
            let source = RunSource::open(runtime, root, &key)?;
            if !resume {
                let (main, cas) = source.reads(crate::storage::disk_pack::budget())?;
                super::super::run_restore::verify_readers_for_profile(
                    &main,
                    &cas,
                    &key,
                    source.profile()?.version,
                )?;
            }
            Ok(SharedRunCapture {
                source,
                _admission: admission,
            })
        })
        .await
    }
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

    pub fn open_file(&self, path: &Path) -> StorageResult<File> {
        if self
            .source
            .files
            .binary_search_by(|file| file.path.as_path().cmp(path))
            .is_err()
        {
            return Err(invalid("shared checkpoint file is outside its inventory"));
        }
        self.source.reader(path)
    }
}
