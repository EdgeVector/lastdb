//! Outbox upload to the relay, with publish-state notes for received DBs.

use std::path::Path;

use mutation_log::{entry_hash, SignedEntry};

use crate::host::Host;
use crate::registered_share;

use super::history_store::{self, HistoryMap};
use super::publish_state;
use super::relay::RelayClient;

/// Drain one outbox and record publish state when an upload ran.
pub(super) async fn upload_outbox(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &Path,
    database_id: &str,
) -> Option<String> {
    match drain_outbox(client, histories, home, database_id).await {
        Ok(uploaded) => {
            if uploaded {
                let pending = super::outbox_len(home, database_id);
                publish_state::note_confirmed(database_id, pending);
            }
            None
        }
        Err(err) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database_id,
                %err,
                "outbox upload failed"
            );
            publish_state::note_failure(database_id, &err);
            Some(err)
        }
    }
}

/// Upload every signed-log received outbox; record publish.lastAttempt.
pub(super) async fn upload_received_outboxes(
    host: &Host,
    histories: &HistoryMap,
    client: &RelayClient,
) -> Option<String> {
    let shares = registered_share::list(&host.home).ok()?;
    let mut turn_error = None;
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        if let Some(err) = upload_outbox(client, histories, &host.home, share.id.as_str()).await {
            turn_error = Some(err);
        }
    }
    turn_error
}

/// True when any syncable database still has unacked outbox entries.
pub(super) fn outbox_pending(host: &Host, histories: &HistoryMap) -> bool {
    for database in crate::host::owned::registered_databases(&host.home) {
        if database.kind != "personal" && database.kind != "shareable" {
            continue;
        }
        if database_outbox_pending(histories, &host.home, &database.id) {
            return true;
        }
    }
    if let Ok(shares) = registered_share::list(&host.home) {
        for share in shares {
            if !share.signed_log {
                continue;
            }
            if database_outbox_pending(histories, &host.home, share.id.as_str()) {
                return true;
            }
        }
    }
    false
}

fn database_outbox_pending(histories: &HistoryMap, home: &Path, database_id: &str) -> bool {
    if mutation_log::DatabaseId::from_hex(database_id).is_err() {
        return false;
    }
    let memory = histories
        .peek_with(database_id, |history| !history.outbox().is_empty())
        .unwrap_or(false);
    if memory {
        return true;
    }
    matches!(
        history_store::load_outbox_entries(home, database_id),
        Ok(entries) if !entries.is_empty()
    )
}

/// Upload until this database's outbox is empty (or the batch cap is hit).
/// Returns whether any batch was sent to the relay.
async fn drain_outbox(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &Path,
    database_id: &str,
) -> Result<bool, String> {
    let disk_entries = history_store::load_outbox_entries(home, database_id)?;
    if !disk_entries.is_empty() {
        histories.with_mut(database_id, |history| {
            history_store::requeue_outbox(history, disk_entries)
        })??;
    }
    let mut uploaded = false;
    for _ in 0..64 {
        let batch = histories.with_mut(database_id, |history| {
            history
                .outbox()
                .batch(32)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })?;
        if batch.is_empty() {
            return Ok(uploaded);
        }
        upload_batch(client, histories, home, database_id, &batch).await?;
        uploaded = true;
    }
    Err(format!(
        "outbox for {database_id} still pending after 64 upload batches"
    ))
}

async fn upload_batch(
    client: &RelayClient,
    histories: &HistoryMap,
    home: &Path,
    database_id: &str,
    batch: &[SignedEntry],
) -> Result<(), String> {
    let (stored, duplicate) = client.append(database_id, batch).await?;
    let _ = duplicate;
    let expected = batch.len() as u64;
    if stored < expected {
        return Err(format!(
            "relay log.append stored {stored} of {expected} outbox entries"
        ));
    }
    histories.with_mut(database_id, |history| {
        for entry in batch {
            if let Ok(hash) = entry_hash(entry) {
                history.outbox_mut().ack(&hash);
            }
        }
        history_store::persist_outbox(home, database_id, history.outbox())
    })??;
    Ok(())
}
