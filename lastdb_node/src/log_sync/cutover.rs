//! Exemem is the only signed-only authority for one database.
//!
//! A local file is not that commit. `cutover-record.json`, `cutover-request.json`,
//! and a missing, stale, or unreadable local receipt leave the legacy path running.
//! This process remembers a commit only after `mutation-log/head` returns D's
//! `CutoverReceipt` for this database and realm. If that commit is already
//! verified and the service then gives no current reply, the path stays closed:
//! no legacy resume and no ack. H has not published the service verifying key,
//! so this process does not honor an on-disk copy of the receipt.

use std::path::Path;
use std::sync::Mutex;

use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{input_digest, DecimalU64};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::disk;
use super::registry::{self, Owner};
use super::session;

pub(crate) const SCHEMA: &str = "lastdb.database_cutover/v1";
pub(crate) const ADAPTER_REVISION: &str = "log-sync-1";
const RECEIPT_DOMAIN: &str = "lastdb:share:cutover-receipt:v1";
const RECEIPT_LIMIT: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// No verified service commit. Byte backup stays on its current path.
    Legacy,
    /// Exemem confirmed signed-only for this database on the last reply.
    SignedOnly(Commit),
    /// A verified commit has no current service reply. Do not resume legacy.
    Closed(Commit),
}

/// What the latest mutation-log read said about this database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ServiceView {
    /// The reply is a verified signed-only receipt for this database.
    Commit(Commit),
    /// The service answered and did not confirm signed-only.
    Open,
    /// Transport, server error, or a body that cannot be read.
    Unavailable,
}

/// Live `CutoverReceipt` fields this daemon will drive from.
///
/// `database_kind` comes from the registry, not from the receipt. The receipt
/// binds `database_id`, the realm, and `evidence_sha256`. A file that only
/// claims `signed_only` is not this object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Commit {
    pub database_id: String,
    pub database_kind: String,
    pub environment: String,
    pub grant_revision: u64,
    pub evidence_sha256: String,
}

struct Live {
    commit: Commit,
    closed: bool,
}

static LIVE: Mutex<Option<Live>> = Mutex::new(None);

/// D's receipt. Unknown fields are rejected, so a head cursor is not a commit.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptWire {
    version: Version,
    environment: String,
    database_id: String,
    evidence_sha256: String,
    grant_revision: DecimalU64,
    signature: String,
}

enum Parsed {
    Commit(Commit),
    Open,
    Unavailable,
}

#[derive(Serialize)]
struct DaemonReady {
    kind: &'static str,
    schema: &'static str,
    environment: String,
    database_id: String,
    database_kind: String,
    adapter_revision: &'static str,
    migrating_device_id: String,
    accepts_cutover_record: bool,
    legacy_resume: &'static str,
    final_write_fence: &'static str,
}

pub(crate) fn classify(status: u16, body: &[u8], owner: &Owner, environment: &str) -> ServiceView {
    if status == 404 {
        return ServiceView::Open;
    }
    if !(200..300).contains(&status) || body.is_empty() || body.len() > RECEIPT_LIMIT {
        return ServiceView::Unavailable;
    }
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return ServiceView::Unavailable,
    };
    match commit_of(&value, owner, environment) {
        Parsed::Commit(commit) => ServiceView::Commit(commit),
        Parsed::Open => ServiceView::Open,
        Parsed::Unavailable => ServiceView::Unavailable,
    }
}

pub(crate) fn observe(database_id: &str, view: ServiceView) {
    let mut live = lock();
    match view {
        ServiceView::Commit(commit) if commit.database_id == database_id => {
            *live = Some(Live {
                commit,
                closed: false,
            });
        }
        ServiceView::Open => {
            if live
                .as_ref()
                .is_some_and(|item| item.commit.database_id == database_id)
            {
                *live = None;
            }
        }
        ServiceView::Unavailable => {
            if let Some(item) = live.as_mut() {
                if item.commit.database_id == database_id {
                    item.closed = true;
                }
            }
        }
        ServiceView::Commit(_) => {}
    }
}

pub(crate) fn gate(home: &Path) -> Gate {
    let Some(owner) = registry::owner(home) else {
        return Gate::Legacy;
    };
    match lock().as_ref() {
        Some(item) if item.commit.database_id == owner.id && item.closed => {
            Gate::Closed(item.commit.clone())
        }
        Some(item) if item.commit.database_id == owner.id => Gate::SignedOnly(item.commit.clone()),
        _ => Gate::Legacy,
    }
}

/// True only after Exemem has confirmed signed-only and that fact still holds.
pub fn legacy_resume_forbidden(home: &Path) -> bool {
    !matches!(gate(home), Gate::Legacy)
}

/// The verified commit, including while the latest reply is missing.
pub(crate) fn remembered(home: &Path) -> Option<Commit> {
    match gate(home) {
        Gate::SignedOnly(commit) | Gate::Closed(commit) => Some(commit),
        Gate::Legacy => None,
    }
}

/// Promise that this adapter will honor a future Exemem commit. Not the commit.
pub(crate) fn publish_daemon_ready(home: &Path, environment: &str) -> Result<(), String> {
    if environment != "dev" && environment != "prod" {
        return Err("cloud environment is not dev or prod".into());
    }
    let Some(owner) = registry::owner(home) else {
        return Ok(());
    };
    let ready = DaemonReady {
        kind: "daemon_sync_ready",
        schema: SCHEMA,
        environment: environment.to_owned(),
        database_id: owner.id.clone(),
        database_kind: owner.kind,
        adapter_revision: ADAPTER_REVISION,
        migrating_device_id: session::device_id(home)?,
        accepts_cutover_record: true,
        legacy_resume: "refused_after_signed_only",
        final_write_fence: "honored",
    };
    let bytes =
        serde_json::to_vec(&ready).map_err(|_| "daemon-ready encoding failed".to_owned())?;
    if bytes.len() > RECEIPT_LIMIT {
        return Err("daemon-ready.json exceeds its limit".into());
    }
    let path = home
        .join("databases")
        .join(owner.id)
        .join("daemon-ready.json");
    disk::write_private(&path, &bytes).map_err(|_| "cannot write daemon-ready.json".to_owned())
}

pub(crate) fn label_ok(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn commit_of(value: &Value, owner: &Owner, environment: &str) -> Parsed {
    if !receipt_shaped(value) {
        return Parsed::Open;
    }
    let wire: ReceiptWire = match serde_json::from_value(value.clone()) {
        Ok(wire) => wire,
        Err(_) => return Parsed::Unavailable,
    };
    if wire.environment != environment
        || wire.database_id != owner.id
        || wire.grant_revision.get() == 0
        || !hex64(&wire.evidence_sha256)
        || !hex_len(&wire.signature, 128)
        || receipt_digest(&wire).is_err()
    {
        return Parsed::Unavailable;
    }
    Parsed::Commit(Commit {
        database_id: owner.id.clone(),
        database_kind: owner.kind.clone(),
        environment: environment.to_owned(),
        grant_revision: wire.grant_revision.get(),
        evidence_sha256: wire.evidence_sha256,
    })
}

fn receipt_shaped(value: &Value) -> bool {
    value.get("evidence_sha256").is_some() || value.get("signature").is_some()
}

fn receipt_digest(wire: &ReceiptWire) -> Result<(), ()> {
    input_digest(
        RECEIPT_DOMAIN,
        &(
            &wire.version,
            wire.environment.as_str(),
            wire.database_id.as_str(),
            wire.evidence_sha256.as_str(),
            wire.grant_revision,
        ),
    )
    .map(|_| ())
    .map_err(|_| ())
}

fn lock() -> std::sync::MutexGuard<'static, Option<Live>> {
    LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn hex64(value: &str) -> bool {
    hex_len(value, 64)
}

fn hex_len(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
