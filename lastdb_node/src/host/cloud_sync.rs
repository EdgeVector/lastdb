//! Thread-safe cloud sync status snapshot exposed via `GET /api/status`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CloudSyncStatus {
    pub(crate) connected: bool,
    pub(crate) environment: Option<String>,
    pub(crate) captures_accepted: bool,
    pub(crate) payment_hold: bool,
    pub(crate) last_error: Option<String>,
    /// Last observed synchronization progress by owned personal database ID.
    pub(crate) databases: BTreeMap<String, DatabaseSyncStatus>,
    /// The newest snapshot of the personal database that this node published or installed.
    pub(crate) snapshot: SnapshotStatus,
}

/// Where the snapshot of the personal database stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SnapshotState {
    /// This node knows of no snapshot.
    #[default]
    None,
    /// The first snapshot of a home that held data before it began to sync is being published.
    InProgress,
    /// This node published the snapshot named by the other fields.
    Published,
    /// This node installed the snapshot named by the other fields when it was restored.
    Installed,
    /// A snapshot cannot be published now. `message` says why. The other fields still name the
    /// newest snapshot that this node knows, if there is one.
    Blocked,
}

/// What this node knows about its newest snapshot. It is the object `cloud_sync.snapshot` of
/// `/api/status`, and the record kept on disk when a snapshot is published or installed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotStatus {
    pub(crate) state: SnapshotState,
    /// Unix milliseconds at which the snapshot entry was signed.
    pub(crate) taken_at_ms: Option<u64>,
    /// Files in the snapshot.
    pub(crate) files: Option<u64>,
    /// Total length of the files.
    pub(crate) bytes: Option<u64>,
    /// The origin (scheme, host and port) of the relay that holds the snapshot.
    pub(crate) relay: Option<String>,
    /// The digest of the manifest that the snapshot entry names.
    pub(crate) manifest_sha256: Option<String>,
    /// The last entry of this device's own stream that the snapshot covers (0 when none).
    pub(crate) through_seq: Option<u64>,
    /// The sequence number of the snapshot entry on the snapshot stream.
    pub(crate) snapshot_seq: Option<u64>,
    /// Device streams the snapshot covers.
    pub(crate) streams: Option<u64>,
    /// Why a snapshot is blocked.
    pub(crate) message: Option<String>,
}

/// Separate local upload and download progress for one owned database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DatabaseSyncStatus {
    pub(crate) upload: UploadSyncStatus,
    pub(crate) download: DownloadSyncStatus,
}

/// Positions in this device's durable upload queue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct UploadSyncStatus {
    pub(crate) spool_head: Option<u64>,
    pub(crate) spool_confirmed_through: Option<u64>,
    pub(crate) last_error: Option<String>,
}

/// Local applied tip for signed-history pull (stream writer_seq).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DownloadSyncStatus {
    pub(crate) applied_through: Option<u64>,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct CloudSyncTracker {
    state: Mutex<CloudSyncStatus>,
    wake_notify: Arc<Notify>,
}

impl CloudSyncTracker {
    pub(crate) fn from_boot(home: &Path, captures_accepted: bool) -> Self {
        let (connected, environment, last_error) = read_home_connection(home);
        Self {
            state: Mutex::new(CloudSyncStatus {
                connected,
                environment,
                captures_accepted,
                payment_hold: false,
                last_error,
                databases: BTreeMap::new(),
                snapshot: SnapshotStatus::default(),
            }),
            wake_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn wake(&self) {
        self.wake_notify.notify_one();
    }

    pub(crate) fn record_error(&self, error: impl Into<String>) {
        let message = error.into();
        let mut guard = self.lock();
        if message == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
            guard.payment_hold = true;
        }
        guard.last_error = Some(message);
    }

    /// Record one database's upload position: log length, acknowledged count,
    /// and the error of its last upload attempt (none after a clean one).
    pub(crate) fn record_upload(
        &self,
        database_id: &str,
        head: u64,
        confirmed_through: u64,
        error: Option<String>,
    ) {
        let mut guard = self.lock();
        let upload = &mut guard
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .upload;
        upload.spool_head = Some(head);
        upload.spool_confirmed_through = Some(confirmed_through);
        upload.last_error = error;
    }

    /// The snapshot this node just published or installed.
    pub(crate) fn record_snapshot(&self, snapshot: SnapshotStatus) {
        self.lock().snapshot = snapshot;
    }

    /// Mark the snapshot as `state` (in progress or blocked), keeping the facts of the newest one.
    pub(crate) fn mark_snapshot(&self, state: SnapshotState, message: Option<String>) {
        let mut guard = self.lock();
        guard.snapshot.state = state;
        guard.snapshot.message = message;
    }

    /// Drop the sticky top-level last_error after a successful sync turn.
    pub(crate) fn clear_error(&self) {
        self.lock().last_error = None;
    }

    pub(crate) fn snapshot(&self) -> CloudSyncStatus {
        let mut status = self.lock().clone();
        if status.last_error.is_none() {
            status.last_error = status.databases.values().find_map(|database| {
                database
                    .upload
                    .last_error
                    .as_ref()
                    .or(database.download.last_error.as_ref())
                    .cloned()
            });
        }
        status
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CloudSyncStatus> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn read_home_connection(home: &Path) -> (bool, Option<String>, Option<String>) {
    match crate::cloud::load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let env = saved
                .connection
                .as_ref()
                .map(|c| c.environment.clone())
                .or_else(|| saved.environment().map(|e| e.as_str().to_owned()));
            (true, env, None)
        }
        Ok(None) => (false, None, None),
        Err(err) => (false, None, Some(err)),
    }
}
