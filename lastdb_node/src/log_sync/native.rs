//! Register a database on the signed cloud log, then sync its accepted writes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::host::owned::RegisteredDatabase as Database;
use crate::host::Host;
use crate::profile::PublicationSpec;
use cloud_backup_protocol::v2::Version;
use serde::{Deserialize, Serialize};

use super::automatic_retention::Pacing;
use super::cloud::CloudLog;
use super::http::{paths, Proof};
use super::session;
use super::turn::{self, Pause};

const PAYMENT_HOLD: Duration = Duration::from_secs(300);
const REFUSAL_HOLD: Duration = Duration::from_secs(60);
const BODY_LIMIT: usize = 65_536;

/// What the latest signed create or access body said about one database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NativeView {
    /// The TLS body is `NativeDatabase` for this id and realm.
    Live,
    /// Transport, server error, or any other body.
    Unavailable,
}

/// `NativeDatabase` for this id and realm. Any other body is not live.
pub(super) fn native_body(body: &[u8], database_id: &str, environment: &str) -> NativeView {
    if body.is_empty() || body.len() > BODY_LIMIT {
        return NativeView::Unavailable;
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct BoundWire {
        state: String,
        version: Version,
        environment: String,
        database_id: String,
    }
    let Ok(wire) = serde_json::from_slice::<BoundWire>(body) else {
        return NativeView::Unavailable;
    };
    if wire.state == "native"
        && wire.version == Version::V2
        && wire.environment == environment
        && wire.database_id == database_id
    {
        NativeView::Live
    } else {
        NativeView::Unavailable
    }
}

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

pub(super) async fn turn(host: &Arc<Host>, database: &Database, cleanup: &mut Pacing) -> Pause {
    if !super::snapshot::pending(&host.home, &database.id) {
        if let Some(pause) = active_hold(&database.id) {
            return pause;
        }
    }
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(error) => {
            host.cloud_sync.record_error(error.clone());
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "device id is unavailable");
            super::snapshot::fail_pending(&host.home, &database.id, &error);
            return Pause::Retry;
        }
    };
    let log = match CloudLog::connect(host, &database.id, &device, 0).await {
        Ok(log) => log,
        Err(error) => {
            host.cloud_sync.record_error(error.clone());
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "cloud log is unavailable");
            super::snapshot::fail_pending(&host.home, &database.id, &error);
            return Pause::Retry;
        }
    };
    if !created(&database.id) {
        match register(&log, database).await {
            CreateOutcome::Live => {
                clear_hold(&database.id);
                host.cloud_sync.clear_payment_hold();
            }
            CreateOutcome::PaymentRequired => {
                host.cloud_sync.record_payment_hold();
                super::snapshot::fail_pending(
                    &host.home,
                    &database.id,
                    crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                );
                return hold_payment(&database.id);
            }
            CreateOutcome::Refused(detail) => {
                host.cloud_sync.record_error(detail.clone());
                super::snapshot::fail_pending(&host.home, &database.id, &detail);
                return hold_backoff(&database.id, &detail);
            }
        }
    }
    turn::open_native(host, database, log, &device, cleanup).await
}

pub(super) fn hold_payment(database_id: &str) -> Pause {
    *payment_hold_until() = Some(Instant::now() + PAYMENT_HOLD);
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

pub(super) fn clear_payment_hold() {
    *payment_hold_until() = None;
    holds().retain(|_, hold| hold.pause != Pause::PaymentHold);
}

pub(super) fn clear_hold(database_id: &str) {
    clear_payment_hold();
    holds().remove(database_id);
}

fn active_hold(database_id: &str) -> Option<Pause> {
    let now = Instant::now();
    if payment_hold_until().is_some_and(|until| now < until) {
        return Some(Pause::PaymentHold);
    }
    let guard = holds();
    let hold = guard.get(database_id)?;
    (now < hold.until).then_some(hold.pause)
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
    let Ok(body) = create_body(&database.id, database.kind, database.spec.as_ref()) else {
        return CreateOutcome::Refused("cannot encode cloud database registration".into());
    };
    let outcome = create_view(log, body, &database.id).await;
    if matches!(outcome, CreateOutcome::Live) {
        remember(&database.id);
    }
    outcome
}

async fn create_view(log: &CloudLog, body: Vec<u8>, database_id: &str) -> CreateOutcome {
    let Ok((status, bytes)) = log.post_status(paths::CREATE, body, Proof::Data).await else {
        return CreateOutcome::Refused("cloud database registration did not complete".into());
    };
    if !(200..300).contains(&status) {
        if crate::cloud::is_payment_subscription_error(status, &bytes) {
            return CreateOutcome::PaymentRequired;
        }
        let prefix = format!("cloud database registration returned HTTP {status}");
        let detail = crate::cloud::describe_protocol_error(&bytes)
            .map_or_else(|| prefix.clone(), |err| format!("{prefix} {err}"));
        return CreateOutcome::Refused(detail);
    }
    if native_body(&bytes, database_id, log.environment()) == NativeView::Live {
        CreateOutcome::Live
    } else {
        CreateOutcome::Refused("cloud database registration returned an invalid reply".into())
    }
}

fn payment_hold_until() -> std::sync::MutexGuard<'static, Option<Instant>> {
    static PAYMENT_UNTIL: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    PAYMENT_UNTIL
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn holds() -> std::sync::MutexGuard<'static, HashMap<String, Hold>> {
    static HOLDS: OnceLock<Mutex<HashMap<String, Hold>>> = OnceLock::new();
    HOLDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn create_body(
    database_id: &str,
    kind: &str,
    spec: Option<&PublicationSpec>,
) -> Result<Vec<u8>, ()> {
    cloud_backup_protocol::canonical_bytes(&CreateBody {
        database_id,
        kind,
        spec,
    })
    .map_err(|_| ())
}

pub(crate) fn database_confirmed(database_id: &str) -> bool {
    created(database_id)
}

fn created(database_id: &str) -> bool {
    confirmed().contains(database_id)
}

fn remember(database_id: &str) {
    confirmed().insert(database_id.to_owned());
    super::cloud_state::note_enrolled();
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
    spec: Option<&'a PublicationSpec>,
}
