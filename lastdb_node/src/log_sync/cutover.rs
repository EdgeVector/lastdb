//! Per-database signed-only gate. A missing record leaves the legacy path alone.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::disk;
use super::registry::{self, Owner};
use super::session;

pub(crate) const SCHEMA: &str = "lastdb.database_cutover/v1";
pub(crate) const ADAPTER_REVISION: &str = "log-sync-1";
const RECORD_LIMIT: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// No registry, or no cutover record. Byte backup stays on its current path.
    Legacy,
    /// This database finished the one signed-only transition.
    SignedOnly(Record),
    /// A record is present and is not the dev signed-only transition.
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub kind: String,
    pub schema: String,
    pub environment: String,
    pub database_id: String,
    pub database_kind: String,
    pub phase: String,
    pub legacy_resume: String,
    pub legacy_retirement: String,
    pub production_migration: bool,
    pub snapshot_ref: String,
    pub final_legacy_position: String,
    pub authority_node_key: String,
    pub grant_revision: u64,
    pub migrating_device_id: String,
    pub evidence_sha256: String,
    pub transition: Transition,
    pub retention: Retention,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Transition {
    pub from: String,
    pub to: String,
    pub at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Retention {
    pub old_data: String,
    pub second_device_ack: bool,
    pub keyed_reads_match: bool,
    pub hold: String,
    pub satisfied_at_ms: Option<u64>,
}

#[derive(Serialize)]
struct DaemonReady {
    kind: &'static str,
    schema: &'static str,
    environment: &'static str,
    database_id: String,
    database_kind: String,
    adapter_revision: &'static str,
    migrating_device_id: String,
    accepts_cutover_record: bool,
    legacy_resume: &'static str,
    final_write_fence: &'static str,
}

pub(crate) fn gate(home: &Path) -> Gate {
    let Some(owner) = registry::owner(home) else {
        return Gate::Legacy;
    };
    match read_record(home, &owner.id) {
        Ok(None) => Gate::Legacy,
        Ok(Some(record)) if accepts(&record, &owner) => Gate::SignedOnly(record),
        Ok(Some(_)) | Err(()) => Gate::Closed,
    }
}

/// True when legacy cloud resume must not run for this home's owner database.
pub fn legacy_resume_forbidden(home: &Path) -> bool {
    !matches!(gate(home), Gate::Legacy)
}

pub(crate) fn record_path(home: &Path, database_id: &str) -> std::path::PathBuf {
    home.join("databases")
        .join(database_id)
        .join("cutover-record.json")
}

/// Write J's daemon-ready evidence once this home has a registry owner.
pub(crate) fn publish_daemon_ready(home: &Path) -> Result<(), String> {
    let Some(owner) = registry::owner(home) else {
        return Ok(());
    };
    if matches!(gate(home), Gate::Closed) {
        return Ok(());
    }
    let device = session::device_id(home)?;
    let ready = DaemonReady {
        kind: "daemon_sync_ready",
        schema: SCHEMA,
        environment: "dev",
        database_id: owner.id.clone(),
        database_kind: owner.kind,
        adapter_revision: ADAPTER_REVISION,
        migrating_device_id: device,
        accepts_cutover_record: true,
        legacy_resume: "refused_after_signed_only",
        final_write_fence: "honored",
    };
    let bytes =
        serde_json::to_vec(&ready).map_err(|_| "daemon-ready encoding failed".to_owned())?;
    let path = home
        .join("databases")
        .join(owner.id)
        .join("daemon-ready.json");
    disk::write_private(&path, &bytes).map_err(|_| "cannot write daemon-ready.json".to_owned())
}

fn read_record(home: &Path, database_id: &str) -> Result<Option<Record>, ()> {
    match disk::read_limited(&record_path(home, database_id), RECORD_LIMIT) {
        Ok(None) => Ok(None),
        Ok(Some(bytes)) => serde_json::from_slice(&bytes).map(Some).map_err(|_| ()),
        Err(_) => Err(()),
    }
}

fn accepts(record: &Record, owner: &Owner) -> bool {
    record.kind == "database_cutover"
        && record.schema == SCHEMA
        && record.environment == "dev"
        && record.database_id == owner.id
        && record.database_kind == owner.kind
        && record.phase == "signed_only"
        && record.legacy_resume == "forbidden"
        && record.legacy_retirement == "forbidden"
        && !record.production_migration
        && record.transition.from == "legacy"
        && record.transition.to == "signed_only"
        && record.retention.old_data == "preserve"
        && record.grant_revision != 0
        && label_ok(&record.authority_node_key)
        && label_ok(&record.migrating_device_id)
        && snapshot_ref_ok(&record.snapshot_ref)
        && hex64(&record.evidence_sha256)
        && matches!(record.retention.hold.as_str(), "required" | "satisfied")
}

pub(crate) fn label_ok(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn snapshot_ref_ok(value: &str) -> bool {
    (1..=1024).contains(&value.len()) && value.chars().all(|ch| !ch.is_control())
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
