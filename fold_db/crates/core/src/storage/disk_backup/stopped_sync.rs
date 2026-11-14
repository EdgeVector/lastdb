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
        let main =
            RunRead::open_existing(&main_path, &run_node::home_key(key), budget).map_err(map)?;
        let files = RunRead::open_existing(&path.join(CAS_DIR), &run_node::cas_key(key), budget)
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
