//! Fresh named databases use signed create, then the same sync path.
//!
//! The registry spec is only the create body. A local file does not activate
//! the database. `mutation-log/create-database` does. G e37d7c851 answers
//! `{created:true, kind}` or HTTP 409 `identity_conflict` / `mutation_log.database`
//! when that database already exists. Head does not return the stored spec, so
//! this path does not read a cutover receipt and does not write legacy evidence.

use crate::host::Host;
use cloud_backup_protocol::ErrorCode;
use cloud_backup_protocol::ProtocolError;
use serde::Serialize;

use super::cloud::CloudLog;
use super::cutover::{self, NativeView};
use super::paths;
use super::registry::{Database, Spec};
use super::session;
use super::turn::{self, Pause};

pub(super) async fn turn(host: &Host, database: &Database) -> Pause {
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            cutover::observe_native(&database.id, NativeView::Unavailable);
            return turn::paused_for(&database.id);
        }
    };
    let revision = turn::probe_revision(&host.home, &database.id, &database.kind);
    let log = match CloudLog::connect(host, &database.id, &device, revision).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            cutover::observe_native(&database.id, NativeView::Unavailable);
            return turn::paused_for(&database.id);
        }
    };
    let view = match create_body(&database.id, &database.kind, database.spec.as_ref()) {
        Ok(body) => create_view(&log, body, &database.kind).await,
        Err(()) => NativeView::Unavailable,
    };
    cutover::observe_native(&database.id, view);
    if !matches!(cutover::gate_for(&database.id), cutover::Gate::Native) {
        return turn::paused_for(&database.id);
    }
    turn::open_native(host, database, revision, log, &device).await
}

async fn create_view(log: &CloudLog, body: Vec<u8>, kind: &str) -> NativeView {
    let Ok((status, bytes)) = log.post_status(paths::CREATE, body).await else {
        return NativeView::Unavailable;
    };
    if (200..300).contains(&status) {
        return created(&bytes, kind);
    }
    if status == 409 && already_exists(&bytes) {
        return NativeView::Live;
    }
    NativeView::Unavailable
}

fn created(bytes: &[u8], kind: &str) -> NativeView {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return NativeView::Unavailable;
    };
    if value.get("created") == Some(&serde_json::Value::Bool(true))
        && value.get("kind").and_then(|item| item.as_str()) == Some(kind)
    {
        NativeView::Live
    } else {
        NativeView::Unavailable
    }
}

fn already_exists(bytes: &[u8]) -> bool {
    let Ok(error) = serde_json::from_slice::<ProtocolError>(bytes) else {
        return false;
    };
    error.code == ErrorCode::IdentityConflict && error.field == "mutation_log.database"
}

fn create_body(database_id: &str, kind: &str, spec: Option<&Spec>) -> Result<Vec<u8>, ()> {
    let spec = spec.map(|spec| SpecBody {
        name: &spec.name,
        version: spec.version,
    });
    cloud_backup_protocol::canonical_bytes(&CreateBody {
        database_id,
        kind,
        spec,
    })
    .map_err(|_| ())
}

#[derive(Serialize)]
struct CreateBody<'a> {
    database_id: &'a str,
    kind: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    spec: Option<SpecBody<'a>>,
}

#[derive(Serialize)]
struct SpecBody<'a> {
    name: &'a str,
    version: u32,
}
