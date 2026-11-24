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
    /// What the relay held of the personal database after the last sweep of this node.
    pub(crate) reclaim: ReclaimStatus,
}

/// What the last finished sweep of this node found on the relay: the object `cloud_sync.reclaim`
/// of `/api/status`, and the record kept on disk beside the snapshot record. A sweep drops every
/// stored file that the newest snapshot and the entries after it do not name. Every field is
/// `None` until a sweep of this node finished.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReclaimStatus {
    /// Unix milliseconds at which the sweep ended.
    pub(crate) swept_at_ms: Option<u64>,
    /// The sequence number of the snapshot entry that the sweep kept.
    pub(crate) snapshot_seq: Option<u64>,
    /// Files in the manifest of that snapshot.
    pub(crate) snapshot_files: Option<u64>,
    /// Total length of those files.
    pub(crate) snapshot_bytes: Option<u64>,
    /// Stored files that the manifest names: its files, one for each digest.
    pub(crate) snapshot_blobs: Option<u64>,
    /// Total length of those stored files, as the manifest gives it.
    pub(crate) snapshot_blob_bytes: Option<u64>,
    /// The length of the manifest.
    pub(crate) manifest_bytes: Option<u64>,
    /// Stored files that the entries after the snapshot name and the manifest does not.
    pub(crate) named_files: Option<u64>,
    /// Total length of those files, as the relay lists them.
    pub(crate) named_bytes: Option<u64>,
    /// Files that the manifest or an entry names and the relay did not list.
    pub(crate) missing_files: Option<u64>,
    /// Entries the sweep read: every entry after the positions of the snapshot, the birth and
    /// the snapshot entry.
    pub(crate) entries: Option<u64>,
    /// The length of those entries as JSON.
    pub(crate) entry_bytes: Option<u64>,
    /// Files the relay listed after the sweep.
    pub(crate) cloud_files: Option<u64>,
    /// Total length of those files.
    pub(crate) cloud_bytes: Option<u64>,
    /// Files the relay dropped in the turn that finished the sweep.
    pub(crate) dropped_files: Option<u64>,
    /// Stored bytes the relay released for them.
    pub(crate) released_bytes: Option<u64>,
    /// Why the sweep that is due for the newest snapshot waits. The other fields still describe
    /// the last sweep that finished.
    #[serde(default)]
    pub(crate) message: Option<String>,
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

/// The totals that `cloud_sync.databases.<id>.usage` of `/api/status` shows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CloudUsage {
    /// Signed entries this device sent and the relay accepted.
    pub(crate) entries: u64,
    /// The length of those entries as sent.
    pub(crate) entry_bytes: u64,
    /// Blobs this device uploaded and committed.
    pub(crate) blobs: u64,
    /// The length of those blobs.
    pub(crate) blob_bytes: u64,
    /// Entries the relay dropped for trims this device asked for.
    pub(crate) released_entries: u64,
    /// Stored bytes the relay released for trims this device asked for.
    pub(crate) released_bytes: u64,
    /// Files the relay dropped for sweeps this device asked for (`blob.drop`).
    #[serde(default)]
    pub(crate) released_blobs: u64,
    /// Stored bytes the relay released for those files.
    #[serde(default)]
    pub(crate) released_blob_bytes: u64,
    /// Blobs whose upload began and has not ended: cut off by a stop, and not put again yet. The
    /// relay may hold them. They are not in `blobs`.
    pub(crate) unconfirmed_blobs: u64,
    /// The length of those blobs.
    pub(crate) unconfirmed_blob_bytes: u64,
    /// Unix milliseconds at which the account began. Nothing this device put before is in it.
    pub(crate) since_ms: u64,
    /// True when the account began before this device's first entry on the relay: the first entry
    /// it counted was entry 1 of its stream. False for a home that uploaded before the account
    /// began (an earlier build, or a copy of such a home): its earlier entries and blobs are not
    /// counted, and the totals are not what it has on the relay.
    pub(crate) complete: bool,
}

/// Separate local upload and download progress for one owned database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DatabaseSyncStatus {
    pub(crate) upload: UploadSyncStatus,
    pub(crate) download: DownloadSyncStatus,
    /// What this device put on the relay and the relay accepted (see `log_sync::usage`). None
    /// until it put anything.
    pub(crate) usage: Option<CloudUsage>,
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
                reclaim: ReclaimStatus::default(),
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
        if error.as_deref() == Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE) {
            guard.payment_hold = true;
        }
        let upload = &mut guard
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .upload;
        upload.spool_head = Some(head);
        upload.spool_confirmed_through = Some(confirmed_through);
        upload.last_error = error;
    }

    /// The error of the last read of the other devices' streams of one database, none after
    /// a clean one. A device that is behind history the relay dropped shows it here.
    pub(crate) fn record_download(&self, database_id: &str, error: Option<String>) {
        let mut guard = self.lock();
        if error.as_deref() == Some(crate::cloud::PAYMENT_REQUIRED_MESSAGE) {
            guard.payment_hold = true;
        }
        guard
            .databases
            .entry(database_id.to_owned())
            .or_default()
            .download
            .last_error = error;
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

    /// What the relay held after the sweep that just finished, or that the node kept on disk.
    pub(crate) fn record_reclaim(&self, reclaim: ReclaimStatus) {
        self.lock().reclaim = reclaim;
    }

    /// Say why the sweep that is due waits, or that it waits no longer, keeping the facts of the
    /// last sweep that finished.
    pub(crate) fn mark_reclaim(&self, message: Option<String>) {
        self.lock().reclaim.message = message;
    }

    /// Drop the sticky top-level last_error after a successful sync turn.
    pub(crate) fn clear_error(&self) {
        self.lock().last_error = None;
    }

    pub(crate) fn snapshot(&self) -> CloudSyncStatus {
        self.lock().clone()
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
