//! Accepted mutation log, database snapshots, and the native cloud work guard.

pub mod accepted;
pub(in crate::storage) mod prearm;

use crate::storage::error::{StorageError, StorageResult};
use std::sync::atomic::{AtomicBool, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Hold this admission for the complete export or restore and its live readers.
/// Paused durable work retains its files independently of this process guard.
#[must_use = "retain the guard for the complete native cloud operation"]
pub struct NativeCloudWorkGuard {
    _private: (),
}

impl NativeCloudWorkGuard {
    pub fn acquire() -> StorageResult<Self> {
        ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                StorageError::InvalidOperation(
                    "one native cloud export or restore is already active".into(),
                )
            })?;
        Ok(Self { _private: () })
    }
}

impl Drop for NativeCloudWorkGuard {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Release);
    }
}

pub mod snapshot {
    //! Capture the matching main and file roots at one durable write boundary.

    use super::prearm;
    use crate::storage::disk_pack::{
        admit, cas_delete, map, physical::V2Home, run_blocking, PackRuntime, CAS_DIR, NODE_MARKER,
        RUN_DIR,
    };
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::metadata::Metadata;
    use crate::storage::format::METADATA_NAMESPACE;
    use lastdb_model::kind_partition::exact_prefix_bounds;
    use laststore::disk_run::home::RunSnapshot;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// The database and file storage captured at one write boundary.
    pub struct DatabaseSnapshot {
        /// Combined main and file frontier included in the frozen files.
        pub native_frontier: u64,
        /// Whether the frozen files must cover writes from before cloud capture.
        pub needs_initial_coverage: bool,
        /// Whether the files contain no unresolved optimistic log writes.
        pub log_covered: bool,
        marker: File,
        main: RunSnapshot,
        files: RunSnapshot,
    }

    impl PackRuntime {
        /// Start a separate author stream on a newly restored device.
        pub async fn adopt_restored_snapshot(
            self: &Arc<Self>,
            bootstrap: bool,
        ) -> StorageResult<()> {
            let runtime = Arc::clone(self);
            self.replay_accepted(async {
                crate::storage::disk_pack::transaction::scope(
                    runtime,
                    Metadata::fork_restored_author(self),
                )
                .await?;
                if bootstrap {
                    Metadata::clear_restored_bootstrap_pending(self).await?;
                }
                Ok::<_, StorageError>(())
            })
            .await?;
            self.flush_native().await?;
            let frontier = self.physical.current_root().frontier();
            self.note_accepted_snapshot(frontier)
                .map_err(|e| StorageError::invalid(format!("restore snapshot coverage: {e:?}")))
        }

        /// Pin both native publications after the local upload queue drains.
        /// `None` means a local edit still needs to be published in the log.
        pub async fn capture_synced_snapshot(
            self: &Arc<Self>,
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
            if accepted.acked != accepted.head {
                if accepted.snapshot_required {
                    return Err(StorageError::invalid(
                        "initial snapshot cannot include queued signed writes without a log baseline",
                    ));
                }
                return Ok(None);
            }
            cas_delete::require_clear(&self.physical)?;
            if crate::storage::local_file_ref::has_references(self)? {
                return Err(StorageError::invalid(
                    "snapshot cannot include files referenced outside the database",
                ));
            }
            let settled = self.has_settled_replay().await?;
            DatabaseSnapshot::from_roots(&self.physical, accepted.snapshot_required, settled)
                .map(Some)
        }

        /// Open the exact database files preserved before any queued local edit.
        /// The caller must verify the signed personal cloud log still starts at zero.
        pub async fn capture_initial_snapshot(
            self: &Arc<Self>,
            key: zeroize::Zeroizing<[u8; 32]>,
        ) -> StorageResult<Option<DatabaseSnapshot>> {
            let _write = self.enter_write().await;
            let status = self.accepted_status();
            if self.requires_recovery()
                || status.gap
                || !status.snapshot_required
                || status.acked != 0
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

        /// Remove the local first-snapshot checkpoint after its signed publication.
        pub fn release_initial_snapshot(&self) -> StorageResult<()> {
            if self.accepted_status().snapshot_required {
                return Err(StorageError::invalid("initial snapshot is still required"));
            }
            prearm::release(&self.physical)
        }

        async fn has_settled_replay(self: &Arc<Self>) -> StorageResult<bool> {
            let store = self.open_store(METADATA_NAMESPACE).await?;
            for prefix in ["signed-replay-pending:", "signed-replay-deferred:"] {
                let (start, end) = exact_prefix_bounds(prefix);
                if !store
                    .scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
                    .await?
                    .is_empty()
                {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }

    impl DatabaseSnapshot {
        pub(super) fn from_roots(
            physical: &V2Home,
            needs_initial_coverage: bool,
            log_covered: bool,
        ) -> StorageResult<Self> {
            let parent = physical
                .path()
                .parent()
                .ok_or_else(|| StorageError::invalid("database snapshot root"))?;
            let marker = admit::open_nofollow_read(&parent.join(NODE_MARKER))?;
            Ok(Self {
                native_frontier: physical.current_root().frontier(),
                needs_initial_coverage,
                log_covered,
                marker,
                main: physical.home.snapshot().map_err(map)?,
                files: physical.cas.snapshot().map_err(map)?,
            })
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
}

pub mod stopped_sync {
    //! Prove a stopped personal database still matches its prepared recovery state.

    use std::path::Path;
    use std::sync::Arc;

    use laststore::disk_run::home::{ReadBudget, RunRead};
    use serde::{Deserialize, Serialize};

    use super::accepted::{self, Status, UploadIdentity};
    use crate::storage::disk_pack::{cas_delete, map, run_node, PackRuntime, CAS_DIR, RUN_DIR};
    use crate::storage::error::{StorageError, StorageResult};

    /// Durable state of both database roots and of this device's accepted spool.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SyncBoundary {
        main: u64,
        files: u64,
        spool: u64,
        writer: u64,
    }

    impl SyncBoundary {
        /// Device writer sequence already confirmed in the cloud.
        pub fn writer_sequence(&self) -> u64 {
            self.writer
        }
    }

    impl PackRuntime {
        /// Pin the same durable frontier used by native snapshots. All local
        /// mutations through the current spool head must have a cloud confirmation.
        pub async fn sync_boundary(
            self: &Arc<Self>,
            identity: &UploadIdentity,
        ) -> StorageResult<SyncBoundary> {
            let _gate = self.enter_write().await;
            if self.requires_recovery() || crate::storage::local_file_ref::has_references(self)? {
                return Err(unready());
            }
            cas_delete::require_clear(&self.physical)?;
            let roots = self.physical.current_root();
            let Status {
                head,
                acked,
                snapshot_required,
                gap,
                ..
            } = self.accepted_status();
            if gap || snapshot_required || head != acked {
                return Err(unready());
            }
            let confirmed = self
                .accepted_upload_position(identity)
                .map_err(accepted::storage)?;
            let Some(position) = confirmed.filter(|position| position.spool_through == head) else {
                return Err(unready());
            };
            Ok(SyncBoundary {
                main: roots.main.frontier(),
                files: roots.cas.frontier(),
                spool: head,
                writer: position.writer_seq,
            })
        }

        /// Durably take ownership of a never-used writer sequence on a quiet local spool.
        /// The caller must have verified zero from the cloud's immutable accepted entries.
        pub async fn bind_empty_sync_writer(
            self: &Arc<Self>,
            identity: &UploadIdentity,
        ) -> StorageResult<()> {
            let _gate = self.enter_write().await;
            let current = self.accepted_status();
            if self.requires_recovery()
                || current.gap
                || current.snapshot_required
                || current.head != 0
                || current.acked != 0
            {
                return Err(unready());
            }
            self.note_accepted_snapshot(self.physical.current_root().frontier())
                .map_err(accepted::storage)?;
            self.confirm_accepted_upload(
                identity,
                0,
                accepted::UploadPosition {
                    writer_seq: 0,
                    spool_through: 0,
                },
            )
            .map_err(accepted::storage)
        }
    }

    /// Hold both native owner locks until the recovery directory exchange is complete.
    #[must_use = "hold both store locks during the filesystem exchange"]
    pub struct StoppedSync {
        _main: RunRead,
        _files: RunRead,
        pub boundary: SyncBoundary,
    }

    impl StoppedSync {
        /// Read the encrypted committed frontiers and compare the durable spool.
        pub fn open(path: &Path, key: &[u8; 32], identity: &UploadIdentity) -> StorageResult<Self> {
            let budget = ReadBudget::new(8 << 20, 64);
            let main_path = path.join(RUN_DIR);
            let main = RunRead::open_existing(&main_path, &run_node::home_key(key), budget)
                .map_err(map)?;
            let files =
                RunRead::open_existing(&path.join(CAS_DIR), &run_node::cas_key(key), budget)
                    .map_err(map)?;
            require_absent(&main_path.join("CAS_DELETE"))?;
            let confirmed = accepted::stopped_confirmation(&main_path, identity)?;
            let boundary = SyncBoundary {
                main: main.frontier(),
                files: files.frontier(),
                spool: confirmed.spool_through,
                writer: confirmed.writer_seq,
            };
            Ok(Self {
                _main: main,
                _files: files,
                boundary,
            })
        }
    }

    fn require_absent(path: &Path) -> StorageResult<()> {
        match path.symlink_metadata() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
            Ok(_) => Err(unready()),
        }
    }

    fn unready() -> StorageError {
        StorageError::invalid(
            "native sync recovery requires intact storage and a confirmed local spool",
        )
    }
}
