//! Publisher side of write leases: sign a lease when one is due.
//!
//! A database has leases only while its publisher has set a lease length for it.
//! The node signs the first lease at once, a new one when half of the newest
//! is left, and a new one at once after a membership change. Each new lease
//! closes every earlier lease whose `closes_at_ms` has passed, using
//! `closing_streams`. A lease cannot be ended early: there is no call for it.
//!
//! Any device of the publisher that has the length set may sign. Two devices
//! that sign together make two valid leases. A member names the newest, and
//! the counting rule closes a lease with the earliest later lease signed at or
//! after its `closes_at_ms`, a total order by position. Of two such leases only
//! one counts as the closer, so the second adds an entry and changes no result.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use mutation_log::{author_lease, EntryHash, LeaseBody, Role, VerifiedHistory};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use super::history_store::HistoryMap;
use crate::host::owned::RegisteredDatabase;
use crate::host::Host;

const NOT_DUE: &str = "no lease is due";

/// The newest grant that a lease was signed after, by database, in this
/// process. A membership change signs one lease for each new grant. If the
/// covered set still differs from the members afterwards (a grant from a
/// second publisher device that sorts before the lease), nothing signs again
/// until the next grant.
fn noted() -> std::sync::MutexGuard<'static, HashMap<String, EntryHash>> {
    static NOTED: OnceLock<Mutex<HashMap<String, EntryHash>>> = OnceLock::new();
    NOTED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn grant_tip(history: &VerifiedHistory) -> Option<EntryHash> {
    history.latest_grant().map(|tip| tip.entry_hash)
}

/// Sign the leases that are due on the owned shareable databases that have a
/// lease length. Returns the last error, if any.
pub(super) fn renew_owned(
    host: &Host,
    histories: &HistoryMap,
    owned: &[RegisteredDatabase],
) -> Option<String> {
    let mut turn_error = None;
    for database in owned.iter().filter(|row| row.kind == "shareable") {
        let Some(length) = database.lease_length_ms else {
            continue;
        };
        if let Err(error) = renew(host, histories, &database.id, length) {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database.id,
                %error,
                "a write lease could not be signed"
            );
            turn_error = Some(error);
        }
    }
    turn_error
}

fn renew(
    host: &Host,
    histories: &HistoryMap,
    database_id: &str,
    length_ms: u64,
) -> Result<(), String> {
    let identity = chain_author::identity_of(host).map_err(|e| e.reply().message.to_owned())?;
    let due = histories.with_mut(database_id, |history| {
        history.owner() == Some(identity.node_key)
            && is_due(
                history,
                fold_db::clock::unix_millis(),
                length_ms,
                noted().get(database_id).copied(),
            )
    })?;
    if !due {
        return Ok(());
    }
    let target = Target {
        home: &host.home,
        database_id,
        identity: &identity,
    };
    let tip = std::cell::Cell::new(None);
    let signed = chain_author::author_publisher(histories, &target, |ctx, history| {
        // Decided again under the history lock, on the time the entry carries.
        let seen = noted().get(database_id).copied();
        if !is_due(history, ctx.authored_at_ms, length_ms, seen) {
            return Err(ChainError::Conflict(NOT_DUE));
        }
        tip.set(grant_tip(history));
        let body = lease_body(history, ctx.authored_at_ms, length_ms);
        author_lease(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
    });
    match signed {
        Ok(_) => {
            if let Some(tip) = tip.get() {
                noted().insert(database_id.to_owned(), tip);
            }
            Ok(())
        }
        Err(ChainError::Conflict(NOT_DUE)) => Ok(()),
        Err(error) => Err(error.reply().message.to_owned()),
    }
}

/// A lease is due when there is none, when half of the newest is left or
/// less, or when the membership changed after it: a writer it does not cover,
/// or a member it covers that is no longer a writer.
fn is_due(
    history: &VerifiedHistory,
    now_ms: u64,
    length_ms: u64,
    noted_tip: Option<EntryHash>,
) -> bool {
    let Some(newest) = history.latest_lease() else {
        return true;
    };
    if now_ms.saturating_add(length_ms / 2) >= newest.expires_at_ms {
        return true;
    }
    grant_tip(history) != noted_tip
        && history.grants().members().any(|(key, role)| {
            (role == Role::Write) != history.lease_covers(&newest.entry_hash, &key)
        })
}

/// The lease signed at `now_ms`: it expires one length later and may be
/// closed half a length after that. It records the closing points of the
/// earlier leases it closes.
fn lease_body(history: &VerifiedHistory, now_ms: u64, length_ms: u64) -> LeaseBody {
    let expires_at_ms = now_ms.saturating_add(length_ms);
    LeaseBody {
        expires_at_ms,
        closes_at_ms: expires_at_ms.saturating_add(length_ms / 2),
        closed: history.closing_streams(now_ms),
    }
}
