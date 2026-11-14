//! Register a database on the signed cloud log, then sync its accepted writes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

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

const PAYMENT_HOLD: Duration = Duration::from_secs(300);
const REFUSAL_HOLD: Duration = Duration::from_secs(60);

struct Hold {
    until: Instant,
    pause: Pause,
    message: String,
}

enum CreateOutcome {
    Live,
    PaymentRequired,
    Refused(String),
}

pub(super) async fn turn(host: &Arc<Host>, database: &Database) -> Pause {
    if !super::snapshot::pending(&host.home, &database.id) {
        if let Some(pause) = active_hold(&database.id) {
            return pause;
        }
    }
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            super::snapshot::fail_pending(&host.home, &database.id, &error);
            return Pause::Retry;
        }
    };
    let revision = turn::probe_revision(&host.home, &database.id, &database.kind);
    let log = match CloudLog::connect(host, &database.id, &device, revision).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            super::snapshot::fail_pending(&host.home, &database.id, &error);
            return Pause::Retry;
        }
    };
    if !created(&database.id) {
        match register(&log, database).await {
            CreateOutcome::Live => clear_hold(&database.id),
            CreateOutcome::PaymentRequired => {
                super::snapshot::fail_pending(
                    &host.home,
                    &database.id,
                    crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                );
                return hold_payment(&database.id);
            }
            CreateOutcome::Refused(detail) => {
                super::snapshot::fail_pending(&host.home, &database.id, &detail);
                return hold_backoff(&database.id, &detail);
            }
        }
    }
    turn::open_native(host, database, log, &device).await
}

pub(super) fn hold_payment(database_id: &str) -> Pause {
    set_hold(
        database_id,
        PAYMENT_HOLD,
        Pause::PaymentHold,
        crate::cloud::PAYMENT_REQUIRED_MESSAGE,
    )
}

pub(super) fn hold_backoff(database_id: &str, detail: &str) -> Pause {
    set_hold(database_id, REFUSAL_HOLD, Pause::Backoff, detail)
}

pub(super) fn clear_hold(database_id: &str) {
    holds().remove(database_id);
}

fn active_hold(database_id: &str) -> Option<Pause> {
    let guard = holds();
    let hold = guard.get(database_id)?;
    (Instant::now() < hold.until).then_some(hold.pause)
}

fn set_hold(database_id: &str, duration: Duration, pause: Pause, message: &str) -> Pause {
    let mut guard = holds();
    let already_logged = guard
        .get(database_id)
        .is_some_and(|prev| prev.message == message);
    guard.insert(
        database_id.to_owned(),
        Hold {
            until: Instant::now() + duration,
            pause,
            message: message.to_owned(),
        },
    );
    drop(guard);
    if !already_logged {
        tracing::warn!(
            target: "lastdbd::log_sync",
            database_id = %database_id,
            "{message}"
        );
    }
    pause
}

async fn register(log: &CloudLog, database: &Database) -> CreateOutcome {
    let Ok(body) = create_body(&database.id, &database.kind, database.spec.as_ref()) else {
        return CreateOutcome::Refused("cannot encode cloud database registration".into());
    };
    let outcome = create_view(log, body, &database.id).await;
    if matches!(outcome, CreateOutcome::Live) {
        remember(&database.id);
    }
    outcome
}

async fn create_view(log: &CloudLog, body: Vec<u8>, database_id: &str) -> CreateOutcome {
    let Ok((status, bytes)) = log
        .post_status(
            paths::CREATE,
            body,
            Proof::Canonical(CanonicalPost::CreateDatabase),
        )
        .await
    else {
        return CreateOutcome::Refused("cloud database registration did not complete".into());
    };
    if !(200..300).contains(&status) {
        if crate::cloud::is_payment_subscription_error(status, &bytes) {
            return CreateOutcome::PaymentRequired;
        }
        let detail = serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(&bytes)
            .ok()
            .map_or_else(
                || format!("cloud database registration returned HTTP {status}"),
                |err| {
                    format!(
                        "cloud database registration returned HTTP {status} {:?} ({})",
                        err.code, err.field
                    )
                },
            );
        return CreateOutcome::Refused(detail);
    }
    if super::status::native_body(&bytes, database_id, log.environment()) == NativeView::Live {
        CreateOutcome::Live
    } else {
        CreateOutcome::Refused("cloud database registration returned an invalid reply".into())
    }
}

fn holds() -> std::sync::MutexGuard<'static, HashMap<String, Hold>> {
    static HOLDS: OnceLock<Mutex<HashMap<String, Hold>>> = OnceLock::new();
    HOLDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
