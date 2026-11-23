//! Capture the matching main and file roots at one durable write boundary.
//!
//! The capture holds the write gate, so no commit starts or finishes while the
//! roots are pinned and the head rows of the device streams are read. That is
//! what makes the cut exact: every write the roots hold has its head row, and
//! every head row names only writes that the roots hold.

use super::{accepted, prearm};
use crate::storage::disk_pack::{
    admit, cas_delete, map, physical::V2Home, run_blocking, PackRuntime, CAS_DIR, NODE_MARKER,
    RUN_DIR,
};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::metadata::{Metadata, ReplayStreamHead};
use crate::storage::format::METADATA_NAMESPACE;
use lastdb_model::kind_partition::exact_prefix_bounds;
use laststore::disk_run::home::RunSnapshot;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The first snapshot of a home that held data before it began to capture is still to come.
fn awaits_first_snapshot(status: accepted::Status) -> bool {
    !status.gap && status.snapshot_required && status.head == 0
}

/// The row prefix of the head row of one device stream. The rest of the key
/// names the stream.
const HEAD_PREFIX: &str = "signed-replay-head:";
/// A snapshot lists every stream it covers in one signed entry, so the number is bounded.
const MAX_STREAMS: usize = 1024;

/// The database and file storage captured at one write boundary.
pub struct DatabaseSnapshot {
    /// Combined main and file frontier included in the frozen files.
    pub native_frontier: u64,
    /// The head row of every device stream the frozen files hold, by stream name
    /// (`<node key hex>:<device id hex>`). Read at the same boundary as the files.
    pub heads: Vec<(String, ReplayStreamHead)>,
    marker: File,
    main: RunSnapshot,
    files: RunSnapshot,
    _files_stage: Option<super::files::Stage>,
}

impl PackRuntime {
    /// Start a separate author stream on a newly restored device. The installed
    /// files carry the author stream of the device that took the snapshot.
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

    /// Preserve a home that held data before it began to capture writes, as the cut of its
    /// first snapshot. Call it once at boot, before any write is accepted. It does nothing for
    /// a home that captured from its birth, for a restore, and once a write is in the spool.
    ///
    /// The cut is taken under the write gate with the spool empty, so every later write is in
    /// the spool and none is in the cut twice.
    pub async fn begin_first_snapshot(
        self: &Arc<Self>,
        key: zeroize::Zeroizing<[u8; 32]>,
    ) -> StorageResult<()> {
        let Some(log) = &self.accepted else {
            return Ok(());
        };
        let status = log.status();
        if !status.snapshot_required {
            // Nothing is owed. A cut left by a kill between the accepted first snapshot and
            // its removal is removed now.
            return prearm::release(&self.physical);
        }
        if !awaits_first_snapshot(status) || self.fresh_initialization_pending() {
            return Ok(());
        }
        self.flush_native().await?;
        let _write = self.enter_write().await;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let Some(log) = &runtime.accepted else {
                return Ok(());
            };
            if !awaits_first_snapshot(log.status()) {
                return Err(StorageError::invalid(
                    "a write was captured before the first snapshot was preserved",
                ));
            }
            prearm::stage_and_pin(&runtime.physical, log, &key)
        })
        .await
    }

    /// Pin both native publications after the local upload queue drains.
    /// `None` means a local edit still needs to be published in the log.
    pub async fn capture_synced_snapshot(
        self: &Arc<Self>,
        key: zeroize::Zeroizing<[u8; 32]>,
    ) -> StorageResult<Option<DatabaseSnapshot>> {
        let _write = self.enter_write().await;
        if self.requires_recovery() {
            return Err(StorageError::invalid(
                "database snapshot requires a healthy writer",
            ));
        }
        let accepted = self.accepted_status();
        if !self.captures_accepted() || accepted.gap {
            return Err(StorageError::invalid(
                "snapshot needs intact local write capture",
            ));
        }
        if accepted.snapshot_required {
            return Err(StorageError::invalid(
                "the first snapshot of this home comes from its preserved cut",
            ));
        }
        if accepted.acked != accepted.head {
            return Ok(None);
        }
        cas_delete::require_clear(&self.physical)?;
        let heads = self.stream_head_rows().await?;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let _references = crate::storage::local_file_ref::catalog_lock(&runtime)?;
            DatabaseSnapshot::from_roots(&runtime.physical, heads)?
                .with_local_files(&runtime.physical, &key)
                .map(Some)
        })
        .await
    }

    /// Open the files preserved by [`Self::begin_first_snapshot`]. `None` when this home needs
    /// no first snapshot or has lost its cut. The caller must check that the relay holds no
    /// snapshot of this owner yet.
    pub async fn capture_initial_snapshot(
        self: &Arc<Self>,
        key: zeroize::Zeroizing<[u8; 32]>,
    ) -> StorageResult<Option<DatabaseSnapshot>> {
        let _write = self.enter_write().await;
        let status = self.accepted_status();
        if self.requires_recovery() || status.gap || !status.snapshot_required || status.acked != 0
        {
            return Ok(None);
        }
        let runtime = Arc::clone(self);
        let snapshot = run_blocking(move || prearm::open(&runtime.physical, &key)).await?;
        if snapshot
            .as_ref()
            .is_some_and(|cut| cut.native_frontier != status.opened_frontier)
        {
            return Err(StorageError::invalid(
                "initial snapshot does not match the capture frontier",
            ));
        }
        Ok(snapshot)
    }

    /// Remove the local first-snapshot cut after its snapshot entry was accepted.
    pub fn release_initial_snapshot(&self) -> StorageResult<()> {
        if self.accepted_status().snapshot_required {
            return Err(StorageError::invalid("initial snapshot is still required"));
        }
        prearm::release(&self.physical)
    }

    /// Every head row of the store, in key order.
    async fn stream_head_rows(self: &Arc<Self>) -> StorageResult<Vec<(String, ReplayStreamHead)>> {
        let store = self.open_store(METADATA_NAMESPACE).await?;
        let (start, end) = exact_prefix_bounds(HEAD_PREFIX);
        let rows = store
            .scan_range_paged(start.as_bytes(), end.as_bytes(), MAX_STREAMS + 1)
            .await?;
        if rows.len() > MAX_STREAMS {
            return Err(StorageError::invalid(
                "the store holds more device streams than a snapshot can name",
            ));
        }
        rows.into_iter()
            .map(|(key, value)| {
                let key = String::from_utf8(key)
                    .map_err(|_| StorageError::invalid("a stream head row has a bad key"))?;
                let stream = key
                    .strip_prefix(HEAD_PREFIX)
                    .ok_or_else(|| StorageError::invalid("a stream head row has a bad key"))?;
                let head = serde_json::from_slice(&value)
                    .map_err(|_| StorageError::invalid("a stream head row is invalid"))?;
                Ok((stream.to_owned(), head))
            })
            .collect()
    }
}

impl DatabaseSnapshot {
    pub(super) fn from_roots(
        physical: &V2Home,
        heads: Vec<(String, ReplayStreamHead)>,
    ) -> StorageResult<Self> {
        let parent = physical
            .path()
            .parent()
            .ok_or_else(|| StorageError::invalid("database snapshot root"))?;
        let marker = admit::open_nofollow_read(&parent.join(NODE_MARKER))?;
        Ok(Self {
            native_frontier: physical.current_root().frontier(),
            heads,
            marker,
            main: physical.home.snapshot().map_err(map)?,
            files: physical.cas.snapshot().map_err(map)?,
            _files_stage: None,
        })
    }

    pub(super) fn with_local_files(
        mut self,
        physical: &V2Home,
        key: &[u8; 32],
    ) -> StorageResult<Self> {
        if let Some((files, stage)) = super::files::capture(physical, &self.files, key)? {
            self.files = files;
            self._files_stage = Some(stage);
        }
        Ok(self)
    }

    pub(super) fn source_path(&self, data: &Path, relative: &Path) -> PathBuf {
        match (&self._files_stage, relative.strip_prefix(CAS_DIR)) {
            (Some(stage), Ok(file)) => stage.path().join(file),
            _ => data.join(relative),
        }
    }

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
