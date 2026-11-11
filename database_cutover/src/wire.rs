//! Evidence files Lane E and Lane I write. This crate checks them and does not fetch them.

use serde::{Deserialize, Serialize};

/// Schema stamp on every cutover file.
pub const SCHEMA: &str = "lastdb.database_cutover/v1";
/// Largest evidence or record file this decision will parse.
pub const MAX_EVIDENCE_BYTES: usize = 65_536;
/// Personal database. One active owner grant. No receiver grant.
pub const KIND_PERSONAL: &str = "personal";
/// Shareable database. `authority_node_key` is the publisher. Members are receive and submit grants.
pub const KIND_SHAREABLE: &str = "shareable";

/// File name inside the evidence directory.
pub const SNAPSHOT_FILE: &str = "snapshot.json";
/// File name inside the evidence directory.
pub const LEGACY_POSITION_FILE: &str = "legacy-position.json";
/// File name inside the evidence directory.
pub const FINAL_WRITES_FILE: &str = "final-writes.json";
/// File name inside the evidence directory.
pub const GRANTS_FILE: &str = "grants.json";
/// File name inside the evidence directory. Lane E writes this.
pub const DAEMON_READY_FILE: &str = "daemon-ready.json";
/// File name inside the evidence directory. Lane I writes this.
pub const DELIVERY_READY_FILE: &str = "delivery-ready.json";

const MAX_MEMBERS: usize = 1024;
const MAX_TIPS: usize = 1024;

/// One writer tip at the final legacy position. Sequence zero means that device has not written.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterTip {
    /// Node key of the writer.
    pub node_key: String,
    /// Device of that node key.
    pub device_id: String,
    /// Last writer sequence, as a JSON integer.
    pub sequence: u64,
}

/// One node-key grant. Bearer material is not a field. Unknown fields are rejected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberGrant {
    /// Node public key.
    pub node_key: String,
    /// `owner`, `receive`, or `submit`. Shareable evidence has no owner row.
    pub capability: String,
    /// `active` or `revoked`. Revoked stays on the member. It does not drop the row.
    pub state: String,
}

/// Fresh full snapshot receipt. Lane E writes it after the snapshot bytes exist.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReceipt {
    /// `fresh_full_snapshot`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database this snapshot covers.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Opaque snapshot pointer. This crate does not open it.
    pub snapshot_ref: String,
    /// Canonical decimal legacy position the snapshot includes.
    pub through_legacy_position: String,
    /// Snapshot object size. Zero only when the position is zero.
    pub byte_count: u64,
    /// The snapshot includes every key of this database.
    pub full: bool,
    /// The snapshot was cut for this migration, not reused from an older backup.
    pub fresh: bool,
    /// The snapshot includes the drained fence.
    pub covers_final_writes: bool,
}

/// Exact final legacy position after the drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPosition {
    /// `legacy_position`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database this position belongs to.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Canonical decimal. The same text as the snapshot and the fence.
    pub position: String,
    /// No legacy append was accepted after this position.
    pub quiescent: bool,
    /// Writer tips at that position.
    pub writer_tips: Vec<WriterTip>,
}

/// Fence and the position observed after draining final writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalWrites {
    /// `final_writes`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database that was fenced.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Canonical decimal where new legacy appends were stopped.
    pub fence_position: String,
    /// Canonical decimal observed after the drain. It must equal the fence.
    pub position_after_drain: String,
    /// Appends accepted after the fence. A request requires zero.
    pub legacy_appends_after_fence: u64,
    /// The fresh snapshot includes `fence_position`.
    pub snapshot_covers_fence: bool,
}

/// Current node-key grants for one database. Lane I copies Lane H's current view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeKeyGrants {
    /// `node_key_grants`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database these grants cover.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Owner node key, or the publisher node key.
    pub authority_node_key: String,
    /// Grant revision. Delivery must cite the same integer.
    pub revision: u64,
    /// Member rows keyed by node key.
    pub members: Vec<MemberGrant>,
}

/// Lane E's promise to honor a future Exemem commit.
/// This file is not that commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonReady {
    /// `daemon_sync_ready`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database the adapter will switch only after Exemem commits.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Adapter revision the operator can compare with the deployed DEV binary.
    pub adapter_revision: String,
    /// Device that is migrating. The second-device proof must name a different device.
    pub migrating_device_id: String,
    /// The adapter will honor a future service commit. This is not that commit.
    pub accepts_cutover_record: bool,
    /// `refused_after_signed_only` after Exemem commits. Not a local switch.
    pub legacy_resume: String,
    /// `honored`.
    pub final_write_fence: String,
}

/// Lane I's promise to close legacy bearer delivery after Exemem commits.
/// This file is not that commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReady {
    /// `delivery_signed_only_ready`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database whose legacy reads I will close after Exemem commits.
    pub database_id: String,
    /// [`KIND_PERSONAL`] or [`KIND_SHAREABLE`].
    pub database_kind: String,
    /// Service revision the operator can compare with the deployed DEV service.
    pub service_revision: String,
    /// Same integer as [`NodeKeyGrants::revision`].
    pub grant_revision: u64,
    /// I will close legacy bearer reads when Exemem commits. This is not that commit.
    pub legacy_bearer_closed: bool,
    /// `drained_or_invalid`.
    pub outstanding_urls: String,
    /// `current_node_key`.
    pub grant_check: String,
}

/// The six receipts, already parsed. [`crate::parse_bundle`] is the parser.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    /// Fresh full snapshot.
    pub snapshot: SnapshotReceipt,
    /// Exact final legacy position.
    pub legacy: LegacyPosition,
    /// Drained fence.
    pub writes: FinalWrites,
    /// Node-key grants.
    pub grants: NodeKeyGrants,
    /// Lane E readiness.
    pub daemon: DaemonReady,
    /// Lane I readiness.
    pub delivery: DeliveryReady,
}

/// Second-device proof. It does not authorize deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecondDeviceProof {
    /// `second_device_proof`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database the device read.
    pub database_id: String,
    /// Device that acked. Not the migrating device.
    pub device_id: String,
    /// Canonical decimal the device acked.
    pub acked_through_legacy_position: String,
    /// Keyed reads on this device matched the migrated database.
    pub keyed_reads_match: bool,
    /// How many keys were compared. Zero only for an empty database.
    pub compared_keys: u64,
    /// The database has no records. Allowed only at legacy position zero.
    pub empty_database: bool,
}

pub(crate) fn within_member_cap(len: usize) -> bool {
    len <= MAX_MEMBERS
}

pub(crate) fn within_tip_cap(len: usize) -> bool {
    len <= MAX_TIPS
}
