//! Live access and create bodies. Dev and prod use the same objects.
//!
//! `NativeDatabase` and `OpenDatabase` are `state`, `version`, `environment`,
//! and `database_id`. `CutoverReceipt` adds `evidence_sha256` and a non-zero
//! `grant_revision`. Nothing signs these objects. A saved file is not one.

use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{input_digest, DecimalU64};
use serde::Deserialize;
use serde_json::Value;

use super::cutover::{self, Commit, NativeView, ServiceView};
use super::registry::Database;

const RECEIPT_DOMAIN: &str = "lastdb:share:cutover-receipt:v1";

pub(crate) fn classify(
    status: u16,
    body: &[u8],
    database: &Database,
    environment: &str,
) -> ServiceView {
    let Some(value) = read_value(status, body) else {
        return ServiceView::Unavailable;
    };
    view_of(&value, database, environment)
}

/// `NativeDatabase` for this id and realm. Any other 2xx body is not live.
pub(crate) fn native_body(body: &[u8], database_id: &str, environment: &str) -> NativeView {
    let Some(value) = read_value(200, body) else {
        return NativeView::Unavailable;
    };
    match bound(&value, "native", database_id, environment) {
        Some(()) => NativeView::Live,
        None => NativeView::Unavailable,
    }
}

fn read_value(status: u16, body: &[u8]) -> Option<Value> {
    if !(200..300).contains(&status) || body.is_empty() || body.len() > cutover::RECEIPT_LIMIT {
        return None;
    }
    serde_json::from_slice(body).ok()
}

fn view_of(value: &Value, database: &Database, environment: &str) -> ServiceView {
    match value.get("state").and_then(Value::as_str) {
        Some("open") => match bound(value, "open", &database.id, environment) {
            Some(()) => ServiceView::Open,
            None => ServiceView::Unavailable,
        },
        Some("native") => match bound(value, "native", &database.id, environment) {
            Some(()) => ServiceView::Native,
            None => ServiceView::Unavailable,
        },
        Some("committed") => committed(value, database, environment),
        Some(_) => ServiceView::Unavailable,
        None if value.get("evidence_sha256").is_some() => ServiceView::Unavailable,
        None => ServiceView::Open,
    }
}

fn bound(value: &Value, state: &str, database_id: &str, environment: &str) -> Option<()> {
    let wire: BoundWire = serde_json::from_value(value.clone()).ok()?;
    (wire.state == state
        && wire.version == Version::V2
        && wire.environment == environment
        && wire.database_id == database_id)
        .then_some(())
}

fn committed(value: &Value, database: &Database, environment: &str) -> ServiceView {
    let Ok(wire) = serde_json::from_value::<ReceiptWire>(value.clone()) else {
        return ServiceView::Unavailable;
    };
    if wire.state != "committed"
        || wire.environment != environment
        || wire.database_id != database.id
        || wire.grant_revision.get() == 0
        || !hex64(&wire.evidence_sha256)
        || receipt_digest(&wire).is_err()
    {
        return ServiceView::Unavailable;
    }
    ServiceView::Commit(Commit {
        database_id: database.id.clone(),
        database_kind: database.kind.clone(),
        environment: environment.to_owned(),
        grant_revision: wire.grant_revision.get(),
        evidence_sha256: wire.evidence_sha256,
    })
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

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// `NativeDatabase` or `OpenDatabase`. Extra fields, including a signature, fail.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundWire {
    state: String,
    version: Version,
    environment: String,
    database_id: String,
}

/// `CutoverReceipt`. `state` is not in the digest. A signature field fails.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptWire {
    state: String,
    version: Version,
    environment: String,
    database_id: String,
    evidence_sha256: String,
    grant_revision: DecimalU64,
}
