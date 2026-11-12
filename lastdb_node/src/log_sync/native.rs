//! A shareable database with a registry spec uses signed create, then sync.
//!
//! The registry spec is the create body. A local file does not activate the
//! database. Activation is the live `NativeDatabase` body from
//! `mutation-log/create-database` for this id and realm. An equal retry
//! returns that same body. `{created:true, kind}` and HTTP 409
//! `identity_conflict` do not activate. This path does not read a receipt
//! and does not write legacy evidence.

use std::sync::Arc;

use crate::host::Host;
use crate::node_read::CanonicalPost;
use serde::Serialize;

use super::cloud::CloudLog;
use super::cutover::{self, NativeView};
use super::http::Proof;
use super::paths;
use super::registry::{Database, Spec};
use super::session;
use super::status;
use super::turn::{self, Pause};

pub(super) async fn turn(host: &Arc<Host>, database: &Database) -> Pause {
    let Some(spec) = database.spec.as_ref() else {
        cutover::observe_native(&database.id, NativeView::Unavailable);
        return turn::paused_for(&database.id);
    };
    if database.kind != "shareable" {
        cutover::observe_native(&database.id, NativeView::Unavailable);
        return turn::paused_for(&database.id);
    }
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            cutover::observe_native(&database.id, NativeView::Unavailable);
            return turn::paused_for(&database.id);
        }
    };
    let revision = turn::probe_revision(&host.home, &database.id, &database.kind);
    let log = match CloudLog::connect(host, &database.id, &device, revision, true).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            cutover::observe_native(&database.id, NativeView::Unavailable);
            return turn::paused_for(&database.id);
        }
    };
    if matches!(cutover::gate_for(&database.id), cutover::Gate::Native) {
        return turn::open_native(host, database, revision, log, &device).await;
    }
    let view = match create_body(&database.id, &database.kind, spec) {
        Ok(body) => create_view(&log, body, &database.id).await,
        Err(()) => NativeView::Unavailable,
    };
    cutover::observe_native(&database.id, view);
    if !matches!(cutover::gate_for(&database.id), cutover::Gate::Native) {
        return turn::paused_for(&database.id);
    }
    turn::open_native(host, database, revision, log, &device).await
}

async fn create_view(log: &CloudLog, body: Vec<u8>, database_id: &str) -> NativeView {
    let Ok((status, bytes)) = log
        .post_status(
            paths::CREATE,
            body,
            Proof::Canonical(CanonicalPost::CreateDatabase),
        )
        .await
    else {
        return NativeView::Unavailable;
    };
    if !(200..300).contains(&status) {
        return NativeView::Unavailable;
    }
    status::native_body(&bytes, database_id, log.environment())
}

fn create_body(database_id: &str, kind: &str, spec: &Spec) -> Result<Vec<u8>, ()> {
    cloud_backup_protocol::canonical_bytes(&CreateBody {
        database_id,
        kind,
        spec: SpecBody {
            name: &spec.name,
            version: spec.version,
        },
    })
    .map_err(|_| ())
}

#[derive(Serialize)]
struct CreateBody<'a> {
    database_id: &'a str,
    kind: &'a str,
    spec: SpecBody<'a>,
}

#[derive(Serialize)]
struct SpecBody<'a> {
    name: &'a str,
    version: u32,
}
