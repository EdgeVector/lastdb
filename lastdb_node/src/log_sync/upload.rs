//! Upload the authored log to the relay by cursor, with publish-state notes
//! for received databases.
//!
//! Pending entries are the log entries past `uploaded.json`. The cursor moves
//! only after the relay acknowledges a batch. A crash between the
//! acknowledgement and the cursor write re-sends that batch, and the relay
//! counts identical entries as stored.

use std::path::Path;

use mutation_log::SignedEntry;

use crate::host::Host;
use crate::registered_share;

use super::blobs;
use super::history_store;
use super::publish_state;
use super::relay::RelayClient;

const BATCH: usize = 32;

/// Upload one database's pending entries and record its upload position.
pub(super) async fn upload_log(
    client: &RelayClient,
    host: &Host,
    database_id: &str,
) -> Option<String> {
    let result = drain_log(client, &host.home, database_id).await;
    let (head, confirmed) = history_store::pending(&host.home, database_id, 0)
        .map_or((0, 0), |found| {
            (found.log_len as u64, found.uploaded as u64)
        });
    match result {
        Ok(uploaded) => {
            host.cloud_sync
                .record_upload(database_id, head, confirmed, None);
            if uploaded {
                publish_state::note_confirmed(database_id, head - confirmed);
            }
            None
        }
        Err(err) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database_id,
                %err,
                "log upload failed"
            );
            host.cloud_sync
                .record_upload(database_id, head, confirmed, Some(err.clone()));
            publish_state::note_failure(database_id, &err);
            Some(err)
        }
    }
}

/// Upload every signed-log received database's log; record publish.lastAttempt.
pub(super) async fn upload_received_logs(host: &Host, client: &RelayClient) -> Option<String> {
    let shares = registered_share::list(&host.home).ok()?;
    let mut turn_error = None;
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        if let Some(err) = upload_log(client, host, share.id.as_str()).await {
            turn_error = Some(err);
        }
    }
    turn_error
}

/// True when any syncable database still has log entries the relay has not
/// acknowledged. An unreadable log or cursor counts as pending, so the loop
/// keeps retrying and the error stays visible.
pub(super) fn uploads_pending(host: &Host) -> bool {
    for database in crate::host::owned::registered_databases(&host.home) {
        if database.kind != "personal" && database.kind != "shareable" {
            continue;
        }
        if database_pending(&host.home, &database.id) {
            return true;
        }
    }
    if let Ok(shares) = registered_share::list(&host.home) {
        for share in shares {
            if share.signed_log && database_pending(&host.home, share.id.as_str()) {
                return true;
            }
        }
    }
    false
}

fn database_pending(home: &Path, database_id: &str) -> bool {
    if mutation_log::DatabaseId::from_hex(database_id).is_err() {
        return false;
    }
    !matches!(history_store::pending_count(home, database_id), Ok(0))
}

/// Upload until the cursor reaches the log head. Returns whether any batch
/// was sent to the relay.
async fn drain_log(client: &RelayClient, home: &Path, database_id: &str) -> Result<bool, String> {
    let mut uploaded = false;
    loop {
        let found = history_store::pending(home, database_id, BATCH)?;
        if found.batch.is_empty() {
            return Ok(uploaded);
        }
        let expected = found.batch.len() as u64;
        let stored = append_batch(client, home, database_id, &found.batch).await?;
        if stored < expected {
            return Err(format!(
                "relay log.append stored {stored} of {expected} log entries"
            ));
        }
        history_store::store_uploaded(home, database_id, found.uploaded + found.batch.len())?;
        uploaded = true;
    }
}

/// Append one batch. The blob of a `bundle` entry goes up before its entry, and
/// the relay takes a blob only for a database whose birth it holds. So the
/// entries before a bundle are appended first, then its blob, then the entry
/// with the entries after it up to the next bundle.
async fn append_batch(
    client: &RelayClient,
    home: &Path,
    database_id: &str,
    batch: &[SignedEntry],
) -> Result<u64, String> {
    let mut stored = 0;
    let mut rest = batch;
    while !rest.is_empty() {
        let end = rest
            .iter()
            .skip(1)
            .position(|entry| matches!(entry, SignedEntry::Bundle { .. }))
            .map_or(rest.len(), |at| at + 1);
        let (chunk, tail) = rest.split_at(end);
        if let SignedEntry::Bundle { body, .. } = &chunk[0] {
            let blob = blobs::load(home, database_id, &body.blob_sha256)?
                .ok_or("a bundle entry has no kept blob to upload")?;
            client
                .blob_put(database_id, &body.blob_sha256.to_hex(), &blob)
                .await?;
        }
        stored += client.append(database_id, chunk).await?.0;
        rest = tail;
    }
    Ok(stored)
}
