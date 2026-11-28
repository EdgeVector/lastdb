//! Pull signed entries from the relay and run AcceptEntry.

use std::collections::BTreeSet;

use mutation_log::{AcceptOutcome, DeviceId, NodeKey, Role, SignedEntry, VerifiedHistory};

use super::history_store::HistoryMap;
use super::materialize::{self, DatabaseCtx};
use super::relay::RelayClient;

/// Pull one known publisher device stream starting after the local head.
async fn pull_stream(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    writer_key: &str,
    device_id: &str,
) -> Result<u64, String> {
    let from_seq = next_seq(histories, db.database_id, writer_key, device_id);
    pull_pages(client, histories, db, writer_key, device_id, from_seq).await
}

/// The first sequence number this node does not hold on a device stream.
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

/// Pull publisher streams first, then member streams, and materialize once at
/// the end so cross-device schema + data entries never deadlock.
pub(super) async fn pull_share_writers(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    caller: NodeKey,
    publisher_hex: &str,
    known_publisher_devices: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let mut accepted = pull_publisher(
        client,
        histories,
        db,
        caller,
        publisher_hex,
        known_publisher_devices,
    )
    .await?;
    let mut seen = BTreeSet::new();
    seen.insert(publisher_hex.to_owned());
    let mut pending: Vec<String> = writer_keys(histories, db.database_id)
        .into_iter()
        .filter(|key| key != publisher_hex && seen.insert(key.clone()))
        .collect();
    while !pending.is_empty() {
        for writer in pending.drain(..) {
            accepted += pull_member_writer(client, histories, db, caller, &writer).await?;
        }
        for writer in writer_keys(histories, db.database_id) {
            if writer != publisher_hex && seen.insert(writer.clone()) {
                pending.push(writer);
            }
        }
    }
    rematerialize(client, histories, db, accepted).await
}

async fn pull_publisher(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    caller: NodeKey,
    publisher_hex: &str,
    known_publisher_devices: impl IntoIterator<Item = String>,
) -> Result<u64, String> {
    let mut devices: BTreeSet<String> = known_publisher_devices.into_iter().collect();
    if let Ok(publisher) = NodeKey::from_hex(publisher_hex) {
        for device in publisher_devices(histories, db.database_id, publisher) {
            devices.insert(device);
        }
    }
    let mut accepted = 0u64;
    for device in &devices {
        accepted += pull_stream(client, histories, db, publisher_hex, device).await?;
    }
    match client.streams(db.database_id, publisher_hex).await {
        Ok(heads) => {
            for head in heads {
                let from = next_seq(histories, db.database_id, publisher_hex, &head.device_id);
                if from <= head.head_seq {
                    accepted +=
                        pull_pages(client, histories, db, publisher_hex, &head.device_id, from)
                            .await?;
                }
            }
        }
        Err(err) if relay_forbidden(&err) && caller_revoked(histories, db.database_id, caller) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %db.database_id,
                %err,
                "publisher streams refused after signed revoke; continuing"
            );
        }
        Err(err) => return Err(err),
    }
    Ok(accepted)
}

async fn pull_member_writer(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    caller: NodeKey,
    writer: &str,
) -> Result<u64, String> {
    match client.streams(db.database_id, writer).await {
        Ok(heads) => {
            let mut accepted = 0u64;
            for head in heads {
                let from = next_seq(histories, db.database_id, writer, &head.device_id);
                if from <= head.head_seq {
                    accepted +=
                        pull_pages(client, histories, db, writer, &head.device_id, from).await?;
                }
            }
            Ok(accepted)
        }
        Err(err) if relay_forbidden(&err) && caller_revoked(histories, db.database_id, caller) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %db.database_id,
                %writer,
                %err,
                "member streams refused after signed revoke; continuing"
            );
            Ok(0)
        }
        Err(err) => Err(err),
    }
}

/// Hex writer keys: publisher plus write/submit (and historical) members.
fn writer_keys(histories: &HistoryMap, database_id: &str) -> Vec<String> {
    histories
        .peek_with(database_id, writer_keys_of)
        .unwrap_or_default()
}

fn publisher_devices(histories: &HistoryMap, database_id: &str, publisher: NodeKey) -> Vec<String> {
    histories
        .peek_with(database_id, |history| {
            let mut out: Vec<String> = history
                .stream_heads_of(publisher)
                .into_iter()
                .map(|(device, _)| device.to_hex())
                .collect();
            for tip in history.publisher_heads() {
                out.push(tip.device_id.to_hex());
            }
            out
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
    device_id: &str,
    from_seq: u64,
) -> Result<u64, String> {
    let mut pages = super::stream_reader::StreamReader::new(writer_key, device_id, from_seq, None);
    let mut accepted = 0u64;
    loop {
        pages.fill(db.database_id, client).await?;
        let Some(entry) = pages.pop() else { break };
        if accept_one(histories, db, entry)? {
            accepted += 1;
        }
    }
    Ok(accepted)
}

async fn rematerialize(
    client: &RelayClient,
    histories: &HistoryMap,
    db: &DatabaseCtx<'_>,
    accepted: u64,
) -> Result<(), String> {
    let retry = histories.needs_materialize_retry(db.database_id);
    if accepted == 0 && !retry {
        return Ok(());
    }
    let lock = histories.write_lock(db.database_id);
    let _guard = lock.lock().await;
    let has_work = histories.with_mut(db.database_id, |history| history.owner().is_some())?
        || histories.has_dirty_keys(db.database_id);
    if !has_work {
        return Ok(());
    }
    let sealer = histories.sealer(db.database_id).ok();
    match materialize::rematerialize(
        histories,
        db.database_id,
        db.store,
        db.home,
        Some(client),
        sealer.as_deref(),
    )
    .await
    {
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
