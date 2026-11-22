//! Pull signed entries from the relay and run AcceptEntry.

use std::collections::BTreeSet;

use mutation_log::{AcceptOutcome, Role, SignedEntry, VerifiedHistory};

use super::history_store::{self, HistoryMap};
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;

/// Pull one writer's entries across every device (`device_id` omitted).
pub(super) async fn pull_writer(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    writer_key: &str,
) -> Result<u64, String> {
    let mut cursor: Option<String> = None;
    let mut accepted = 0u64;
    loop {
        let (entries, next) = client
            .read(db.database_id, writer_key, None, 1, cursor.as_deref(), 90)
            .await?;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            if accept_one(histories, db, entry)? {
                accepted += 1;
            }
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    rematerialize(histories, db, accepted).await?;
    Ok(accepted)
}

/// Pull every write/submit grantee (and the publisher) until the set is stable.
pub(super) async fn pull_writers(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    seed: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for key in seed {
        seen.insert(key);
    }
    let mut pending: Vec<String> = seen.iter().cloned().collect();
    while !pending.is_empty() {
        for writer in pending.drain(..) {
            let _ = pull_writer(client, histories, db, &writer).await?;
        }
        for writer in writer_keys(histories, db.database_id) {
            if seen.insert(writer.clone()) {
                pending.push(writer);
            }
        }
    }
    Ok(())
}

/// Hex writer keys: publisher plus active write/submit members.
pub(super) fn writer_keys(histories: &HistoryMap, database_id: &str) -> Vec<String> {
    histories
        .peek_with(database_id, writer_keys_of)
        .unwrap_or_default()
}

fn writer_keys_of(history: &VerifiedHistory) -> Vec<String> {
    let mut keys = BTreeSet::new();
    if let Some(owner) = history.owner() {
        keys.insert(owner.to_hex());
    }
    // Current write/submit holders, plus anyone who ever held those roles
    // (including reduced/revoked members whose earlier writes still count).
    for (key, role) in history.grants().members() {
        if matches!(role, Role::Write | Role::Submit | Role::Revoked)
            || history.grants().was_reduced(&key)
        {
            keys.insert(key.to_hex());
        }
    }
    for (hash, _) in history.publisher_grants() {
        let Some(meta) = history.get_accepted(&hash) else {
            continue;
        };
        let Some(body) = meta.grant_body.as_ref() else {
            continue;
        };
        if !matches!(body.role, Role::Write | Role::Submit) {
            continue;
        }
        if let mutation_log::Grantee::Member(key) = body.grantee {
            keys.insert(key.to_hex());
        }
    }
    keys.into_iter().collect()
}

async fn rematerialize(
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    accepted: u64,
) -> Result<(), String> {
    let retry = histories.needs_materialize_retry(db.database_id);
    if accepted == 0 && !retry {
        return Ok(());
    }
    let history = histories.snapshot(db.database_id)?;
    if history.owner().is_none() {
        return Ok(());
    }
    match materialize::apply_history_records(db.store, &history).await {
        Ok(()) => histories.clear_materialize_retry(db.database_id),
        Err(error) => {
            histories.mark_materialize_retry(db.database_id);
            return Err(error);
        }
    }
    Ok(())
}

fn accept_one(
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    entry: SignedEntry,
) -> Result<bool, String> {
    histories.with_mut(db.database_id, |history| {
        match history.accept_entry(entry) {
            AcceptOutcome::Accept { duplicate, .. } => {
                history_store::persist_outbox(db.home, db.database_id, history.outbox())?;
                Ok(!duplicate)
            }
            AcceptOutcome::Pending { .. } => Ok(false),
            AcceptOutcome::Reject { reason } => {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    ?reason,
                    "relay entry rejected by AcceptEntry"
                );
                Ok(false)
            }
        }
    })?
}
