//! In-memory publish report for one received database.
//!
//! `pending` is the count of signed log entries past the upload cursor once a
//! path in this process reads the log and records it. The database list reads that
//! record and does not open a store. `pending` is absent until this process
//! has read the log, including after a restart. `lastAttempt` lives only
//! in this process and is empty again after a restart.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Confirmed,
    Refused,
    Unconfirmed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StatusKind {
    Http(u16),
    Missing,
    Word(&'static str),
}

#[derive(Clone)]
struct Attempt {
    at: String,
    outcome: Outcome,
    status: StatusKind,
}

#[derive(Default)]
struct Slot {
    attempt: Option<Attempt>,
    /// `None` until [`note_pending`] has run in this process.
    pending: Option<u64>,
}

/// Record the pending count. The list reads it.
pub(crate) fn note_pending(database_id: &str, pending: u64) {
    slots().entry(database_id.to_owned()).or_default().pending = Some(pending);
}

/// A relay append that stored the full batch.
pub(crate) fn note_confirmed(database_id: &str, pending: u64) {
    note_pending(database_id, pending);
    note(database_id, Outcome::Confirmed, StatusKind::Http(200));
}

/// A relay append that stopped. Prefer an HTTP status when the error names one.
pub(crate) fn note_failure(database_id: &str, reason: &str) {
    let (outcome, status) = classify(reason);
    note(database_id, outcome, status);
}

/// Publish report for one received database. A registration that is not a
/// signed log has no log of ours, so `pending` is 0. Otherwise `pending` is null
/// until [`note_pending`] has run in this process.
pub(crate) fn row(database_id: &str, signed_log: bool) -> Value {
    json!({
        "pending": pending_json(database_id, signed_log),
        "lastAttempt": attempt_json(database_id),
    })
}

fn classify(reason: &str) -> (Outcome, StatusKind) {
    if let Some(code) = http_status(reason) {
        let outcome = if code == 401 || code == 403 {
            Outcome::Refused
        } else {
            Outcome::Unconfirmed
        };
        return (outcome, StatusKind::Http(code));
    }
    if reason.contains("no usable cloud credentials") || reason.contains("no_credentials") {
        return (Outcome::Unconfirmed, StatusKind::Word("no_credentials"));
    }
    (Outcome::Unconfirmed, StatusKind::Missing)
}

fn http_status(reason: &str) -> Option<u16> {
    let marker = "HTTP ";
    let start = reason.find(marker)? + marker.len();
    let digits: String = reason[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn note(database_id: &str, outcome: Outcome, status: StatusKind) {
    slots().entry(database_id.to_owned()).or_default().attempt = Some(Attempt {
        at: now_rfc3339(),
        outcome,
        status,
    });
}

fn pending_json(database_id: &str, signed_log: bool) -> Value {
    if !signed_log {
        return Value::from(0);
    }
    slots()
        .get(database_id)
        .and_then(|slot| slot.pending)
        .map_or(Value::Null, Value::from)
}

fn attempt_json(database_id: &str) -> Value {
    let Some(attempt) = slots()
        .get(database_id)
        .and_then(|slot| slot.attempt.clone())
    else {
        return Value::Null;
    };
    json!({
        "at": attempt.at,
        "outcome": attempt.outcome.as_str(),
        "status": status_json(attempt.status),
    })
}

fn status_json(status: StatusKind) -> Value {
    match status {
        StatusKind::Http(code) => Value::from(code),
        StatusKind::Missing => Value::Null,
        StatusKind::Word(word) => Value::from(word),
    }
}

fn now_rfc3339() -> String {
    let millis = i64::try_from(fold_db::clock::unix_millis()).unwrap_or(0);
    chrono::DateTime::from_timestamp_millis(millis).map_or_else(
        || "1970-01-01T00:00:00Z".to_owned(),
        |time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

fn slots() -> std::sync::MutexGuard<'static, HashMap<String, Slot>> {
    static SLOTS: OnceLock<Mutex<HashMap<String, Slot>>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Refused => "refused",
            Self::Unconfirmed => "unconfirmed",
        }
    }
}
