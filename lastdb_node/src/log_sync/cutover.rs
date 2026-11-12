//! Exemem is the only signed-only authority, and each database has its own.
//!
//! A local file is not that commit. `cutover-record.json`, `cutover-request.json`,
//! and a missing, stale, or unreadable local receipt leave that database on its
//! current path. This process remembers a commit only after `mutation-log/access`
//! returns `CutoverReceipt` for that database and realm. The receipt has no
//! service signature. A fresh shareable database with a spec is remembered only
//! after `mutation-log/create-database` returns `NativeDatabase` for that id
//! and realm. The same body confirms an equal retry. HTTP 409 does not. If that
//! database was already confirmed and the service then gives no current reply,
//! that database stays closed: no legacy resume and no ack. Other databases
//! are unchanged. A saved receipt is not authority.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

use super::disk;
use super::registry::{self, Database};
use super::session;

pub(crate) const SCHEMA: &str = "lastdb.database_cutover/v1";
pub(crate) const ADAPTER_REVISION: &str = "log-sync-1";
pub(crate) const RECEIPT_LIMIT: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// No verified service reply for this database. Its current path stays.
    Legacy,
    /// Exemem confirmed a cutover receipt for this database on the last reply.
    SignedOnly(Commit),
    /// Exemem confirmed signed create for this fresh named database.
    Native,
    /// A verified reply has no current service answer. Do not resume legacy.
    Closed(Hold),
}

/// Which live reply is remembered while the service is silent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Hold {
    Receipt(Commit),
    Native,
}

/// What the latest `mutation-log/access` body said about this database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ServiceView {
    /// Authenticated `CutoverReceipt` for this database. No service signature.
    Commit(Commit),
    /// Authenticated `NativeDatabase` for this database. No receipt.
    Native,
    /// Authenticated `OpenDatabase`, or a 2xx body that is not a receipt.
    Open,
    /// Transport, server error, or a body that cannot be read.
    ///
    /// HTTP 404 is this arm. It is not an open database. An unconfirmed
    /// database stays on legacy. A confirmed database stays closed.
    Unavailable,
}

/// What the latest signed create said about one fresh named database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeView {
    /// The TLS body is `NativeDatabase` for this id and realm.
    Live,
    /// Transport, server error, or any other body, including HTTP 409.
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

enum Activation {
    Receipt(Commit),
    Native,
}

struct Live {
    activation: Activation,
    closed: bool,
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

pub(crate) fn observe(database_id: &str, view: ServiceView) {
    let mut live = lock();
    match view {
        ServiceView::Commit(commit) if commit.database_id == database_id => {
            live.insert(
                database_id.to_owned(),
                Live {
                    activation: Activation::Receipt(commit),
                    closed: false,
                },
            );
        }
        ServiceView::Native => {
            live.insert(
                database_id.to_owned(),
                Live {
                    activation: Activation::Native,
                    closed: false,
                },
            );
        }
        // An open body clears a receipt only. A native activation stays.
        ServiceView::Open => {
            if matches!(
                live.get(database_id).map(|item| &item.activation),
                Some(Activation::Receipt(_))
            ) {
                live.remove(database_id);
            }
        }
        ServiceView::Unavailable => close_if_present(&mut live, database_id),
        ServiceView::Commit(_) => {}
    }
}

pub(crate) fn observe_native(database_id: &str, view: NativeView) {
    let mut live = lock();
    match view {
        NativeView::Live => {
            live.insert(
                database_id.to_owned(),
                Live {
                    activation: Activation::Native,
                    closed: false,
                },
            );
        }
        NativeView::Unavailable => {
            if let Some(item) = live.get_mut(database_id) {
                if matches!(item.activation, Activation::Native) {
                    item.closed = true;
                }
            }
        }
    }
}

pub(crate) fn gate_for(database_id: &str) -> Gate {
    match lock().get(database_id) {
        Some(item) if item.closed => match &item.activation {
            Activation::Receipt(commit) => Gate::Closed(Hold::Receipt(commit.clone())),
            Activation::Native => Gate::Closed(Hold::Native),
        },
        Some(item) => match &item.activation {
            Activation::Receipt(commit) => Gate::SignedOnly(commit.clone()),
            Activation::Native => Gate::Native,
        },
        None => Gate::Legacy,
    }
}

/// True when the owner store's own service status has left the legacy path.
///
/// Automatic byte backup follows that store only. A sibling's status does not
/// stop it.
pub fn legacy_resume_forbidden(home: &Path) -> bool {
    let Some(owner) = registry::owner(home) else {
        return false;
    };
    !matches!(gate_for(&owner.id), Gate::Legacy)
}

/// Receipt revision remembered for this database, including while the reply is missing.
pub(crate) fn receipt_revision(database_id: &str) -> Option<u64> {
    match gate_for(database_id) {
        Gate::SignedOnly(commit) | Gate::Closed(Hold::Receipt(commit)) => {
            Some(commit.grant_revision)
        }
        Gate::Legacy | Gate::Native | Gate::Closed(Hold::Native) => None,
    }
}

/// Promise that this adapter will honor a future Exemem commit. Not the commit.
pub(crate) fn publish_daemon_ready(
    home: &Path,
    environment: &str,
    database: &Database,
) -> Result<(), String> {
    if database.native {
        return Ok(());
    }
    if environment != "dev" && environment != "prod" {
        return Err("cloud environment is not dev or prod".into());
    }
    let ready = DaemonReady {
        kind: "daemon_sync_ready",
        schema: SCHEMA,
        environment: environment.to_owned(),
        database_id: database.id.clone(),
        database_kind: database.kind.clone(),
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
        .join(&database.id)
        .join("daemon-ready.json");
    disk::write_private(&path, &bytes).map_err(|_| "cannot write daemon-ready.json".to_owned())
}

pub(crate) fn label_ok(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn close_if_present(live: &mut HashMap<String, Live>, database_id: &str) {
    if let Some(item) = live.get_mut(database_id) {
        item.closed = true;
    }
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<String, Live>> {
    static LIVE: OnceLock<Mutex<HashMap<String, Live>>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
