//! Status payload types exposed under `status.cloud_sync` on `GET /api/status`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CloudSyncStatus {
    pub(crate) connected: bool,
    pub(crate) environment: Option<String>,
    pub(crate) captures_accepted: bool,
    pub(crate) payment_hold: bool,
    pub(crate) last_error: Option<String>,
    /// Last observed synchronization progress by database ID.
    pub(crate) databases: BTreeMap<String, DatabaseSyncStatus>,
    /// The newest snapshot of the personal database that this node published or installed.
    pub(crate) snapshot: SnapshotStatus,
    /// What the relay held of the personal database after the last sweep of this node.
    pub(crate) reclaim: ReclaimStatus,
    /// The contract revision of the relay this node syncs with, and which features it serves.
    pub(crate) relay: RelayStatus,
}

/// The object `cloud_sync.relay` of `/api/status`: what the relay this node syncs with named on
/// its last answer of a sync turn. Both fields are empty until the relay answered.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct RelayStatus {
    /// The contract revision within protocol `relay_protocol::PROTOCOL` (2 for `1.2`); 0 for a
    /// relay from before revisions, which names none.
    pub(crate) revision: Option<u32>,
    /// Each `relay_protocol::Feature`, and whether this relay serves it.
    pub(crate) features: Vec<FeatureStatus>,
}

/// One feature of [`RelayStatus`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct FeatureStatus {
    pub(crate) name: &'static str,
    /// The revision that first serves it.
    pub(crate) since: u32,
    /// `relay_protocol::supports` for the relay's revision.
    pub(crate) supported: bool,
}

impl RelayStatus {
    /// The status of a relay that named `revision`.
    pub(super) fn of(revision: u32) -> Self {
        let features = relay_protocol::Feature::ALL
            .into_iter()
            .map(|feature| FeatureStatus {
                name: feature.name(),
                since: feature.since(),
                supported: relay_protocol::supports(Some(revision), feature),
            })
            .collect();
        Self {
            revision: Some(revision),
            features,
        }
    }
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

/// Oldest queued upload waiting behind a send failure or relay refusal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct BlockedUploadStatus {
    pub(crate) writer_seq: u64,
    pub(crate) authored_at_ms: Option<u64>,
    pub(crate) reason: String,
}

/// Separate local upload, download, snapshot and cleanup progress for one database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DatabaseSyncStatus {
    pub(crate) upload: UploadSyncStatus,
    pub(crate) download: DownloadSyncStatus,
    /// Snapshot progress for this database when snapshots apply (personal database).
    pub(crate) snapshot: Option<SnapshotStatus>,
    /// Cloud reclaim / cleanup progress for this database when sweeps apply (personal database).
    pub(crate) cleanup: Option<ReclaimStatus>,
    /// What this device put on the relay and the relay accepted (see `log_sync::usage`). None
    /// until it put anything.
    pub(crate) usage: Option<CloudUsage>,
    #[serde(skip)]
    pub(super) owned: bool,
}

/// Positions in this device's durable upload queue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct UploadSyncStatus {
    pub(crate) spool_head: Option<u64>,
    pub(crate) spool_confirmed_through: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) oldest_blocked: Option<BlockedUploadStatus>,
}

/// Local applied tip for signed-history pull (stream writer_seq).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct DownloadSyncStatus {
    pub(crate) applied_through: Option<u64>,
    pub(crate) last_error: Option<String>,
}
