//! Pull signed entries from the relay and run AcceptEntry.

use std::collections::BTreeSet;

use mutation_log::{AcceptOutcome, DeviceId, NodeKey, Role, SignedEntry, VerifiedHistory};

use super::history_store::HistoryMap;
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;

/// Pull one device stream (`device_id` set). Used for the publisher's known
/// devices so a revoked member can still read through the signed revoke.
///
/// The read starts after the last entry this node holds on that stream, so a
/// sync turn costs what is new and not the whole history.
pub(super) async fn pull_stream(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    writer_key: &str,
    device_id: &str,
) -> Result<u64, String> {
    let from_seq = next_seq(histories, db.database_id, writer_key, device_id);
    pull_pages(client, histories, db, writer_key, Some(device_id), from_seq).await
}

/// Pull one writer's entries across every device (`device_id` omitted). The
/// read starts at the first entry, because it is also how this node finds a
/// device stream it has not seen.
pub(super) async fn pull_writer(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    writer_key: &str,
) -> Result<u64, String> {
    pull_pages(client, histories, db, writer_key, None, 1).await
}

/// The first sequence number this node does not hold on a device stream.
///
/// Only accepted entries count. An entry that waits for a dependency is not
/// kept across a restart, so it is read again from here.
fn next_seq(histories: &HistoryMap, database_id: &str, writer_key: &str, device_id: &str) -> u64 {
    let (Ok(node_key), Ok(device)) = (NodeKey::from_hex(writer_key), DeviceId::from_hex(device_id))
    else {
        return 1;
    };
    let stream = VerifiedHistory::stream(node_key, device);
    histories
        .peek_with(database_id, |history| history.stream_head(&stream))
        .flatten()
        .map_or(1, |head| head.writer_seq + 1)
}

/// Publisher known-device pulls first, then AllDevices for other writers.
///
/// AllDevices HTTP 403 is soft when this caller is already revoked in history
/// (unsigned relay message is not treated as proof of revoke).
pub(super) async fn pull_share_writers(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    caller: NodeKey,
    publisher_hex: &str,
    known_publisher_devices: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let mut devices: BTreeSet<String> = known_publisher_devices.into_iter().collect();
    if let Ok(publisher) = NodeKey::from_hex(publisher_hex) {
        for device in publisher_devices(histories, db.database_id, publisher) {
            devices.insert(device);
        }
    }
    if devices.is_empty() {
        // No observed publisher device yet (pre-birth). Discover via AllDevices;
        // once revoked, soft-fail below keeps this from sticking last_error.
        match pull_writer(client, histories, db, publisher_hex).await {
            Ok(_) => {}
            Err(err)
                if relay_forbidden(&err) && caller_revoked(histories, db.database_id, caller) =>
            {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %db.database_id,
                    %err,
                    "publisher AllDevices refused after signed revoke; continuing"
                );
            }
            Err(err) => return Err(err),
        }
    } else {
        for device in &devices {
            let _ = pull_stream(client, histories, db, publisher_hex, device).await?;
        }
    }
    let mut seen = BTreeSet::new();
    seen.insert(publisher_hex.to_owned());
    let mut pending: Vec<String> = writer_keys(histories, db.database_id)
        .into_iter()
        .filter(|key| key != publisher_hex && seen.insert(key.clone()))
        .collect();
    while !pending.is_empty() {
        for writer in pending.drain(..) {
            match pull_writer(client, histories, db, &writer).await {
                Ok(_) => {}
                Err(err)
                    if relay_forbidden(&err)
                        && caller_revoked(histories, db.database_id, caller) =>
                {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        id = %db.database_id,
                        %writer,
                        %err,
                        "AllDevices pull refused after signed revoke; continuing"
                    );
                }
                Err(err) => return Err(err),
            }
        }
        for writer in writer_keys(histories, db.database_id) {
            if writer == publisher_hex {
                continue;
            }
            if seen.insert(writer.clone()) {
                pending.push(writer);
            }
        }
    }
    Ok(())
}

/// Hex writer keys: publisher plus write/submit (and historical) members.
pub(super) fn writer_keys(histories: &HistoryMap, database_id: &str) -> Vec<String> {
    histories
        .peek_with(database_id, writer_keys_of)
        .unwrap_or_default()
}

fn publisher_devices(histories: &HistoryMap, database_id: &str, publisher: NodeKey) -> Vec<String> {
    histories
        .peek_with(database_id, |history| {
            history
                .stream_heads_of(publisher)
                .into_iter()
                .map(|(device, _)| device.to_hex())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn caller_revoked(histories: &HistoryMap, database_id: &str, caller: NodeKey) -> bool {
    histories
        .peek_with(database_id, |history| {
            history
                .grants()
                .members()
                .any(|(key, role)| key == caller && role == Role::Revoked)
        })
        .unwrap_or(false)
}

fn relay_forbidden(err: &str) -> bool {
    err.contains("HTTP 403")
}

fn writer_keys_of(history: &VerifiedHistory) -> Vec<String> {
    let mut keys = BTreeSet::new();
    if let Some(owner) = history.owner() {
        keys.insert(owner.to_hex());
    }
    // Current write/submit holders and revoked members. Anyone who ever held a
    // proposing role is added below from the publisher grants.
    for (key, role) in history.grants().members() {
        if matches!(role, Role::Write | Role::Submit | Role::Revoked) {
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

async fn pull_pages(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    writer_key: &str,
    device_id: Option<&str>,
    from_seq: u64,
) -> Result<u64, String> {
    let mut cursor: Option<String> = None;
    let mut accepted = 0u64;
    loop {
        let (entries, next) = client
            .read(
                db.database_id,
                writer_key,
                device_id,
                from_seq,
                cursor.as_deref(),
                90,
            )
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
            AcceptOutcome::Accept { duplicate, .. } => Ok(!duplicate),
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
