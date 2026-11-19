//! In-memory publish report for one received database.
//!
//! `pending` is the accepted-write spool: head minus the confirmed prefix.
//! `lastAttempt` lives only in this process and is empty again after a restart.
//! The warn token is the `category` value `PrivateFormat` keeps.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use mutation_log::{AppendBatch, AppendOutcome, LogError};
use serde_json::{json, Value};

use crate::host::Host;
use crate::registered_share::RegisteredShare;

use super::cloud::CloudLog;
use super::wire;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Confirmed,
    Refused,
    Unconfirmed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StatusKind {
    Http(u16),
    AccessUnknown,
    NoCredentials,
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
    noted: bool,
    warned: Option<(Outcome, StatusKind)>,
}

pub(super) enum Step {
    Accepted(Vec<u64>),
    Duplicate,
    Payment,
    Refused,
    Conflict,
    Unconfirmed,
}

pub(super) fn begin(database_id: &str) {
    slots().entry(database_id.to_owned()).or_default().noted = false;
}

pub(super) fn note_access_unknown(database_id: &str) {
    note(database_id, Outcome::Unconfirmed, StatusKind::AccessUnknown);
}

pub(super) fn note_word(database_id: &str, status: &'static str) {
    note(database_id, Outcome::Unconfirmed, StatusKind::Word(status));
}

/// Record a swallowed publish error. A failure noted earlier this turn wins.
/// A `confirmed` note does not: a split batch whose first half was confirmed
/// still stopped, and the report must say so.
pub(super) fn note_failure(database_id: &str, reason: &str) {
    let failed = slots().get(database_id).is_some_and(|slot| {
        slot.noted
            && slot
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.outcome != Outcome::Confirmed)
    });
    if !failed {
        note_word(database_id, failure_status(reason));
    }
}

/// A closed share no longer publishes, so an earlier `confirmed` is not current.
pub(super) fn clear_confirmed(database_id: &str) {
    if let Some(slot) = slots().get_mut(database_id) {
        if slot
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.outcome == Outcome::Confirmed)
        {
            slot.attempt = None;
            slot.warned = None;
        }
    }
}

pub(super) fn note_unreachable(database_id: &str, home: &Path, environment: &str) {
    let status = if credentials_usable(home, environment) {
        StatusKind::Missing
    } else {
        StatusKind::NoCredentials
    };
    note(database_id, Outcome::Unconfirmed, status);
}

/// A turn that stopped with no note. A local cause is named. Any other stop
/// had no answer from the cloud, so the credentials decide the status.
pub(super) fn finish_unnoted(host: &Host, log: &CloudLog, reason: &str) {
    let noted = slots()
        .get(log.database_id())
        .is_some_and(|slot| slot.noted);
    if noted {
        return;
    }
    match reason_word(reason) {
        Some(word) => note_word(log.database_id(), word),
        None => note_unreachable(log.database_id(), &host.home, log.environment()),
    }
}

pub(super) fn classify(
    received: bool,
    database_id: &str,
    status: u16,
    reply: &[u8],
    batch: &AppendBatch,
    through: u64,
    committed: bool,
) -> Step {
    let step = decide(received, status, reply, batch, through, committed);
    if received {
        note(database_id, outcome_of(&step), StatusKind::Http(status));
    }
    step
}

pub(crate) async fn row(host: &Host, share: &RegisteredShare) -> Value {
    json!({
        "pending": pending(host, share).await,
        "lastAttempt": attempt_json(share.id.as_str()),
    })
}

fn decide(
    received: bool,
    status: u16,
    reply: &[u8],
    batch: &AppendBatch,
    through: u64,
    committed: bool,
) -> Step {
    if (200..300).contains(&status) {
        return match wire::append_outcome(reply, batch) {
            Ok(AppendOutcome::Accepted { delivery_seqs }) if !delivery_seqs.is_empty() => {
                Step::Accepted(delivery_seqs)
            }
            Ok(AppendOutcome::Duplicate { next_seq }) if next_seq > through => Step::Duplicate,
            _ => Step::Unconfirmed,
        };
    }
    if crate::cloud::is_payment_subscription_error(status, reply) {
        return Step::Payment;
    }
    if wire::service_error(reply) == Some(LogError::Conflict) {
        return Step::Conflict;
    }
    if received && !committed && refused(status, reply) {
        return Step::Refused;
    }
    Step::Unconfirmed
}

fn refused(status: u16, reply: &[u8]) -> bool {
    status == 401 || status == 403 || wire::service_error(reply) == Some(LogError::Unauthorized)
}

fn outcome_of(step: &Step) -> Outcome {
    match step {
        Step::Accepted(_) | Step::Duplicate => Outcome::Confirmed,
        Step::Refused => Outcome::Refused,
        Step::Payment | Step::Conflict | Step::Unconfirmed => Outcome::Unconfirmed,
    }
}

fn note(database_id: &str, outcome: Outcome, status: StatusKind) {
    let changed = {
        let mut open = slots();
        let slot = open.entry(database_id.to_owned()).or_default();
        slot.noted = true;
        slot.attempt = Some(Attempt {
            at: now_rfc3339(),
            outcome,
            status,
        });
        let pair = (outcome, status);
        if slot.warned == Some(pair) {
            false
        } else {
            slot.warned = Some(pair);
            true
        }
    };
    if changed {
        warn_publish(outcome, status);
    }
}

fn warn_publish(outcome: Outcome, status: StatusKind) {
    // PrivateFormat prints this token as category and drops the message.
    let token = category_token(outcome, status);
    if crate::host::publish_category_allowed(&token) {
        tracing::warn!(target: "lastdbd::log_sync", category = token.as_str(), "publish");
    }
}

fn category_token(outcome: Outcome, status: StatusKind) -> String {
    let name = outcome.as_str();
    match status {
        StatusKind::Http(code) => format!("{name}:{code}"),
        StatusKind::AccessUnknown => format!("{name}:access_unknown"),
        StatusKind::NoCredentials => format!("{name}:no_credentials"),
        StatusKind::Missing => name.to_owned(),
        StatusKind::Word(word) => format!("{name}:{word}"),
    }
}

fn failure_status(reason: &str) -> &'static str {
    reason_word(reason).unwrap_or("upload_unconfirmed")
}

fn reason_word(reason: &str) -> Option<&'static str> {
    Some(match reason {
        "unknown database publication policy" => "policy_unknown",
        "local database or content key unavailable" => "local_store_unavailable",
        "local capture disabled until the enrolled node restarts" => "capture_disabled",
        crate::cloud::PAYMENT_REQUIRED_MESSAGE => "payment_required",
        super::accepted::SEQUENCE_UNKNOWN => "writer_sequence_unknown",
        "could not read stored writes to initialize the local upload position" => "writer_tip",
        "cloud entries disagree with the saved upload identity" => "upload_identity",
        "local upload position is unavailable for this device and cloud scope" => "upload_position",
        "local upload reader stopped" | "the local upload queue reader stopped" => "reader_stopped",
        "local upload writer stopped" => "writer_stopped",
        "upload confirmation could not be recorded locally" => "confirm_failed",
        "refused write was not undone" => "undo_failed",
        "the next local write could not be prepared for upload" => "prepare_failed",
        "the local upload queue could not be read" => "queue_unreadable",
        "invalid local spool position" => "spool_position",
        "invalid local upload batch" => "batch_invalid",
        "signed entry exceeds the request limit" => "entry_too_large",
        _ => return None,
    })
}

fn credentials_usable(home: &Path, environment: &str) -> bool {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) => creds.connection.is_some() && creds.env.as_str() == environment,
        _ => false,
    }
}

async fn pending(host: &Host, share: &RegisteredShare) -> u64 {
    if !share.signed_log {
        return 0;
    }
    let Ok(store) = crate::registered_share::receiver_store(host, &share.id).await else {
        return 0;
    };
    let status = crate::accepted_writes::status(&store.db_ops().native_runtime());
    status.head.saturating_sub(status.acked)
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
        StatusKind::AccessUnknown => Value::from("access_unknown"),
        StatusKind::NoCredentials => Value::from("no_credentials"),
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
