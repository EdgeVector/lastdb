//! Upload the authored log to the relay by cursor, with publish-state notes
//! for received databases.
//!
//! Pending entries are the log entries past `uploaded.json`. The cursor moves
//! only after the relay acknowledges a batch. A crash between the
//! acknowledgement and the cursor write re-sends that batch, and the relay
//! counts identical entries as stored.

use std::path::Path;

use mutation_log::{SignedEntry, StreamKind};

use crate::host::cloud_sync::BlockedUploadStatus;
use crate::host::Host;
use crate::registered_share;

use super::history_store;
use super::publish_state;
use super::relay::RelayClient;

const BATCH: usize = 32;

/// Upload one database's pending entries and record its upload position.
///
/// `member` is true for a received database. When the relay refuses a
/// member's entries (HTTP 403), the refusal is final for now: the database's
/// sync status names it, the uploads wait (see [`publish_state::uploads_held`])
/// and the turn does not fail, so the other databases keep their pace.
pub(super) async fn upload_log(
    client: &RelayClient,
    host: &Host,
    database_id: &str,
    member: bool,
) -> Option<String> {
    let owned = !member;
    let tip = member.then(|| grant_and_lease_tip(host, database_id));
    if member && publish_state::uploads_held(database_id, tip.as_deref()) {
        if let Ok(found) = history_store::pending(&host.home, database_id, 0) {
            let (head, confirmed) = (found.log_len as u64, found.uploaded as u64);
            publish_state::note_pending(database_id, head.saturating_sub(confirmed));
            host.cloud_sync
                .refresh_upload_queue(database_id, head, confirmed);
        }
        return None;
    }
    if member {
        if let Some(message) = cannot_deposit(host, database_id) {
            // The fact is enough: unsent entries and a role the relay takes
            // nothing from. No request is made, so there is nothing to retry.
            let (head, confirmed, oldest) =
                pending_upload_state(&host.home, database_id, Some(&message));
            publish_state::note_pending(database_id, head.saturating_sub(confirmed));
            host.cloud_sync.record_upload(
                database_id,
                head,
                confirmed,
                Some(message),
                oldest,
                false,
            );
            return None;
        }
    }
    let result = drain_log(client, &host.home, database_id).await;
    match result {
        Ok(uploaded) => {
            let (head, confirmed, _) = pending_upload_state(&host.home, database_id, None);
            let pending = head.saturating_sub(confirmed);
            publish_state::note_pending(database_id, pending);
            host.cloud_sync
                .record_upload(database_id, head, confirmed, None, None, owned);
            if uploaded {
                publish_state::note_confirmed(database_id, pending);
            }
            None
        }
        Err(err) if member && err.contains("HTTP 403") => {
            let message = refusal_message(host, database_id, &err);
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database_id,
                %message,
                "the relay refused this member's entries; uploads wait"
            );
            let (head, confirmed, oldest) =
                pending_upload_state(&host.home, database_id, Some(&message));
            publish_state::note_pending(database_id, head.saturating_sub(confirmed));
            host.cloud_sync.record_upload(
                database_id,
                head,
                confirmed,
                Some(message),
                oldest,
                false,
            );
            publish_state::note_failure(database_id, &err);
            publish_state::hold_uploads(database_id, tip);
            None
        }
        Err(err) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                id = %database_id,
                %err,
                "log upload failed"
            );
            let (head, confirmed, oldest) =
                pending_upload_state(&host.home, database_id, Some(&err));
            publish_state::note_pending(database_id, head.saturating_sub(confirmed));
            host.cloud_sync.record_upload(
                database_id,
                head,
                confirmed,
                Some(err.clone()),
                oldest,
                owned,
            );
            publish_state::note_failure(database_id, &err);
            Some(err)
        }
    }
}

fn pending_upload_state(
    home: &Path,
    database_id: &str,
    reason: Option<&str>,
) -> (u64, u64, Option<BlockedUploadStatus>) {
    let Ok(found) = history_store::pending(home, database_id, 1) else {
        return (0, 0, None);
    };
    let oldest = reason.and_then(|why| {
        let first = found.batch.first()?;
        Some(BlockedUploadStatus {
            writer_seq: first.header().writer_seq,
            authored_at_ms: Some(first.header().authored_at_ms),
            reason: why.to_owned(),
        })
    });
    (found.log_len as u64, found.uploaded as u64, oldest)
}

/// Upload every signed-log received database's log; record publish.lastAttempt.
/// Returns `true` when every signed-log received database uploaded cleanly.
pub(super) async fn upload_received_logs(host: &Host, client: &RelayClient) -> bool {
    let shares = match registered_share::list(&host.home) {
        Ok(shares) => shares,
        Err(err) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %err,
                "received share list failed"
            );
            return false;
        }
    };
    let mut all_ok = true;
    for share in shares {
        if !share.signed_log {
            continue;
        }
        if mutation_log::DatabaseId::from_hex(share.id.as_str()).is_err() {
            continue;
        }
        if upload_log(client, host, share.id.as_str(), true)
            .await
            .is_some()
        {
            all_ok = false;
        }
    }
    all_ok
}

/// True when any owned shareable database (or received share when
/// `include_received` is true) still has unheld log entries past `uploaded.json`.
pub(super) fn shareable_uploads_pending(host: &Host, include_received: bool) -> bool {
    for database in crate::host::owned::registered_databases(&host.home) {
        if database.kind != "shareable" {
            continue;
        }
        if database_pending(&host.home, &database.id) {
            return true;
        }
    }
    if include_received {
        if let Ok(shares) = registered_share::list(&host.home) {
            for share in shares {
                if share.signed_log
                    && database_pending(&host.home, share.id.as_str())
                    && cannot_deposit(host, share.id.as_str()).is_none()
                    && !publish_state::uploads_held(
                        share.id.as_str(),
                        Some(grant_and_lease_tip(host, share.id.as_str()).as_str()),
                    )
                {
                    return true;
                }
            }
        }
    }
    false
}

/// The newest grant and lease the history holds, as one text. A change in
/// either lets a refused member try its uploads again at once.
fn grant_and_lease_tip(host: &Host, database_id: &str) -> String {
    super::histories_for(host)
        .peek_with(database_id, |history| {
            format!(
                "{:?}/{:?}",
                history.latest_grant().map(|tip| tip.entry_hash.to_hex()),
                history
                    .latest_lease()
                    .map(|lease| lease.entry_hash.to_hex())
            )
        })
        .unwrap_or_default()
}

/// The sync status message when this node holds unsent entries of a received
/// database and its role, as its history folds it, is not `write` or `submit`
/// (it was reduced to `read` or removed). The relay takes entries only from a
/// member that may deposit, so these entries stay unsent. It is a fact of the
/// history, so it holds whether the node learned of the change before or
/// after it tried to upload. A role change that comes later is seen on the
/// next turn.
fn cannot_deposit(host: &Host, database_id: &str) -> Option<String> {
    let node = mutation_log::NodeKey::from_bytes(host.keypair.public_key_bytes());
    let known = super::histories_for(host)
        .peek_with(database_id, |history| {
            history.owner().is_some()
                && !matches!(
                    super::received::role_of(history, node),
                    Some(mutation_log::Role::Write | mutation_log::Role::Submit)
                )
        })
        .unwrap_or(false);
    if !known {
        return None;
    }
    let found = history_store::pending(&host.home, database_id, 1).ok()?;
    let first = found.batch.first()?;
    let count = found.log_len - found.uploaded;
    let seq = first.header().writer_seq;
    Some(if count == 1 {
        format!(
            "1 entry of this device stream is not uploaded, from entry {seq}: this node's role is \
             read or removed, and the relay takes entries only from a member that holds write or \
             submit. Entries after it wait behind it. Pulling and other databases are not \
             affected. It is uploaded when the publisher grants this node write or submit again."
        )
    } else {
        format!(
            "{count} entries of this device stream are not uploaded, from entry {seq}: this node's \
             role is read or removed, and the relay takes entries only from a member that holds \
             write or submit. Entries after them wait behind them. Pulling and other databases are \
             not affected. They are uploaded when the publisher grants this node write or submit \
             again."
        )
    })
}

/// What the sync status says when the relay refuses a member's entries. A
/// refused entry blocks the entries after it on the same device stream,
/// because a stream is appended without gaps.
fn refusal_message(host: &Host, database_id: &str, err: &str) -> String {
    let first = history_store::pending(&host.home, database_id, 1)
        .ok()
        .and_then(|found| found.batch.into_iter().next());
    let Some(first) = first else {
        return format!("the relay refused this database's entries; uploads wait ({err})");
    };
    let seq = first.header().writer_seq;
    let direct =
        matches!(&first, SignedEntry::Data { body, .. } if body.stream == StreamKind::Direct);
    if direct {
        format!(
            "the relay refused entry {seq} of this device stream, a direct write signed under \
             a write lease: it takes entries only from a member whose role is write or \
             submit now, and this node's role is read or removed. Entries after it wait \
             behind it. Pulling and other databases are not affected. Uploads are tried again when the grants or leases \
             change, and every 30 seconds. ({err})"
        )
    } else {
        format!(
            "the relay refused entry {seq} of this device stream; entries after it wait behind \
             it. Uploads are tried again when the grants or leases change, and every 30 \
             seconds. ({err})"
        )
    }
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
        let take = chunk_len(database_id, &found.batch)?;
        let chunk = &found.batch[..take];
        let expected = chunk.len() as u64;
        let (stored, duplicate) = client.append(database_id, chunk).await?;
        if stored + duplicate < expected {
            return Err(format!(
                "relay log.append stored {stored} (duplicate {duplicate}) of {expected} log entries"
            ));
        }
        history_store::store_uploaded(home, database_id, found.uploaded + chunk.len())?;
        uploaded = true;
    }
}

fn chunk_len(database_id: &str, batch: &[SignedEntry]) -> Result<usize, String> {
    const MAX_CHUNK_BYTES: usize = 3 * 1024 * 1024;
    let mut used = 32usize.saturating_add(database_id.len());
    let mut count = 0usize;
    for entry in batch {
        let item_len = serde_json::to_vec(entry)
            .map_err(|_| "cannot encode authored log entry".to_owned())?
            .len();
        let next_used = used.saturating_add(item_len).saturating_add(1);
        if next_used > MAX_CHUNK_BYTES {
            if count == 0 {
                return Err(format!(
                    "log entry seq {} exceeds {MAX_CHUNK_BYTES} byte upload limit ({item_len} bytes)",
                    entry.header().writer_seq
                ));
            }
            break;
        }
        used = next_used;
        count += 1;
    }
    Ok(count)
}
