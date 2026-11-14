//! Capture the matching main and file roots at one durable write boundary.

use crate::storage::disk_pack::{
    admit, cas_delete, map, PackRuntime, CAS_DIR, NODE_MARKER, RUN_DIR,
};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::metadata::Metadata;
use laststore::disk_run::home::RunSnapshot;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The database and file storage captured at one write boundary.
pub struct DatabaseSnapshot {
    /// Last local accepted change included in the frozen files.
    pub accepted_head: u64,
    /// Main storage frontier included in the frozen files.
    pub native_frontier: u64,
    marker: File,
    main: RunSnapshot,
    files: RunSnapshot,
}

impl PackRuntime {
    /// Start a separate author stream on a newly restored device.
    pub async fn adopt_restored_snapshot(self: &Arc<Self>) -> StorageResult<()> {
        let runtime = Arc::clone(self);
        self.replay_accepted(async {
            crate::storage::disk_pack::transaction::scope(
                runtime,
                Metadata::fork_restored_author(self),
            )
            .await
        })
        .await?;
        self.flush_native().await?;
        let frontier = self.physical.current_root().frontier();
        self.note_accepted_snapshot(frontier)
            .map_err(|e| StorageError::invalid(format!("restore snapshot coverage: {e:?}")))
    }

    /// Pin both native publications under the same database write guard.
    pub async fn capture_database_snapshot(self: &Arc<Self>) -> StorageResult<DatabaseSnapshot> {
        let _write = self.enter_write().await;
        if self.requires_recovery() {
            return Err(StorageError::invalid(
                "database snapshot requires a healthy writer",
            ));
        }
        cas_delete::require_clear(&self.physical)?;
        if crate::storage::local_file_ref::has_references(self)? {
            return Err(StorageError::invalid(
                "snapshot cannot include files referenced outside the database",
            ));
        }
        let parent = self
            .physical
            .path()
            .parent()
            .ok_or_else(|| StorageError::invalid("database snapshot root"))?;
        let marker = admit::open_nofollow_read(&parent.join(NODE_MARKER))?;
        Ok(DatabaseSnapshot {
            accepted_head: self.accepted_status().head,
            native_frontier: self.physical.current_root().frontier(),
            marker,
            main: self.physical.home.snapshot().map_err(map)?,
            files: self.physical.cas.snapshot().map_err(map)?,
        })
    }
}

impl DatabaseSnapshot {
    /// List the exact original file paths and sizes.
    pub fn files(&self) -> StorageResult<Vec<(PathBuf, u64)>> {
        let mut files = vec![(NODE_MARKER.into(), self.marker.metadata()?.len())];
        for (name, root) in [(RUN_DIR, &self.main), (CAS_DIR, &self.files)] {
            files.extend(
                root.files()
                    .map_err(map)?
                    .into_iter()
                    .map(|(path, bytes)| (Path::new(name).join(path), bytes)),
            );
        }
        Ok(files)
    }

    /// Open a file held by this capture.
    pub fn open_file(&self, path: &Path) -> StorageResult<File> {
        if path == Path::new(NODE_MARKER) {
            Ok(self.marker.try_clone()?)
        } else if let Ok(relative) = path.strip_prefix(RUN_DIR) {
            self.main.open_file(relative).map_err(map)
        } else if let Ok(relative) = path.strip_prefix(CAS_DIR) {
            self.files.open_file(relative).map_err(map)
        } else {
            Err(StorageError::invalid(
                "database snapshot file is outside its roots",
            ))
        }
    }
}
