//! Register a database on the signed cloud log, then sync its accepted writes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use crate::host::Host;
use crate::node_read::CanonicalPost;
use serde::Serialize;

use super::cloud::CloudLog;
use super::http::Proof;
use super::paths;
use super::registry::{Database, Spec};
use super::session;
use super::status::NativeView;
use super::turn::{self, Pause};

pub(super) async fn turn(host: &Arc<Host>, database: &Database) -> Pause {
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            return paused(database);
        }
    };
    let revision = turn::probe_revision(&host.home, &database.id, &database.kind);
    let log = match CloudLog::connect(host, &database.id, &device, revision).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            return paused(database);
        }
    };
    if !created(&database.id) && !register(&log, database).await {
        return Pause::Idle;
    }
    turn::open_native(host, database, log, &device).await
}

fn paused(database: &Database) -> Pause {
    if created(&database.id) {
        Pause::Retry
    } else {
        Pause::Idle
    }
}

async fn register(log: &CloudLog, database: &Database) -> bool {
    let Ok(body) = create_body(&database.id, &database.kind, database.spec.as_ref()) else {
        return false;
    };
    if create_view(log, body, &database.id).await != NativeView::Live {
        return false;
    }
    remember(&database.id);
    true
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
    super::status::native_body(&bytes, database_id, log.environment())
}

fn create_body(database_id: &str, kind: &str, spec: Option<&Spec>) -> Result<Vec<u8>, ()> {
    cloud_backup_protocol::canonical_bytes(&CreateBody {
        database_id,
        kind,
        spec: spec.map(|spec| SpecBody {
            name: &spec.name,
            version: spec.version,
        }),
    })
    .map_err(|_| ())
}

fn created(database_id: &str) -> bool {
    confirmed().contains(database_id)
}

fn remember(database_id: &str) {
    confirmed().insert(database_id.to_owned());
}

fn confirmed() -> std::sync::MutexGuard<'static, HashSet<String>> {
    static CREATED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CREATED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
