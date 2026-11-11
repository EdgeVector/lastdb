//! Apply a pulled page on the booted store. The caller acks only after this returns.
//!
//! A reset is not installed here. E2 owns snapshot installation through the
//! normal replay path. This module stages bytes when a reset arrives and does
//! not replace the live store. `byte_cloud_restore::native::restore_at` writes
//! an owner byte-backup file set and is not that installer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use fold_db::schema::types::SchemaError;
use mutation_log::{classify_shareable, Entry, NamespaceKind, StoredEntry};
use mutation_log_sync::{Outcome, SyncEvent, SyncState};

use crate::host::Host;

use super::cloud::CloudLog;
use super::cutover;
use super::disk;
use super::payload;

const MARKER_LIMIT: usize = 64;

static SNAPSHOT_WARNED: AtomicBool = AtomicBool::new(false);

pub(crate) enum PagePlan {
    /// Snapshot bytes are staged. Do not ack and do not apply the suffix.
    SnapshotHeld,
    /// The page was handled through this delivery sequence.
    Ack(u64),
    /// Stopped mid-page. Ack only when a delivery sequence was fully handled.
    Hold(Option<u64>),
}

pub(crate) async fn apply_page(
    host: &Host,
    log: &CloudLog,
    state: &mut SyncState,
    outcome: &Outcome,
    database_id: &str,
    shareable: bool,
) -> Result<PagePlan, String> {
    let Outcome::Pulled {
        entries,
        cursor,
        reset,
    } = outcome
    else {
        return Err("pull did not return a page".into());
    };
    if let Some(reset) = reset {
        stage_snapshot(host, log, database_id, reset.snapshot_ref.as_str()).await?;
        if !SNAPSHOT_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                target: "lastdbd::log_sync",
                "snapshot bytes are staged and the live store was not replaced"
            );
        }
        return Ok(PagePlan::SnapshotHeld);
    }
    let mut handled = None;
    for stored in entries {
        if !advance_one(host, log, state, database_id, shareable, stored).await? {
            return Ok(PagePlan::Hold(handled));
        }
        handled = Some(stored.delivery_seq);
    }
    Ok(PagePlan::Ack(*cursor))
}

async fn advance_one(
    host: &Host,
    log: &CloudLog,
    state: &mut SyncState,
    database_id: &str,
    shareable: bool,
    stored: &StoredEntry,
) -> Result<bool, String> {
    let entry = &stored.entry;
    if entry.writer.key.as_str() == log.principal().as_str() {
        return apply_owned(host, database_id, entry).await;
    }
    if !shareable {
        tracing::warn!(
            target: "lastdbd::log_sync",
            delivery_seq = stored.delivery_seq,
            "personal log entry is not from this node key"
        );
        return Ok(false);
    }
    validate_submission(host, log, state, entry).await
}

async fn apply_owned(host: &Host, database_id: &str, entry: &Entry) -> Result<bool, String> {
    let Some(decoded) = payload::decode(&entry.payload) else {
        tracing::warn!(target: "lastdbd::log_sync", "owned log entry is not a canonical record");
        return Ok(false);
    };
    if payload::namespace_of(&decoded.schema).as_ref() != Some(&entry.namespace) {
        tracing::warn!(target: "lastdbd::log_sync", "owned log entry namespace does not match its schema");
        return Ok(false);
    }
    let Some(mutation_id) = safe_id(&decoded.mutation_id) else {
        tracing::warn!(target: "lastdbd::log_sync", "owned log entry has no durable mutation id");
        return Ok(false);
    };
    let marker = marker_path(&host.home, database_id, mutation_id);
    match disk::read_limited(&marker, MARKER_LIMIT) {
        Ok(Some(_)) => return Ok(true),
        Ok(None) => {}
        Err(_) => {
            tracing::warn!(target: "lastdbd::log_sync", "applied marker is unreadable");
            return Ok(false);
        }
    }
    let Some(mutation) = payload::to_mutation(&decoded) else {
        return Ok(false);
    };
    match host
        .db
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
    {
        Ok(_) => match disk::write_private(&marker, b"1") {
            Ok(()) => Ok(true),
            Err(_) => {
                tracing::warn!(target: "lastdbd::log_sync", "applied marker was not stored");
                Ok(false)
            }
        },
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                class = schema_class(&error),
                "log apply stopped before ack"
            );
            Ok(false)
        }
    }
}

async fn validate_submission(
    host: &Host,
    log: &CloudLog,
    state: &mut SyncState,
    entry: &Entry,
) -> Result<bool, String> {
    let Some(decoded) = payload::decode(&entry.payload) else {
        return reject(entry);
    };
    let namespace_matches =
        payload::namespace_of(&decoded.schema).as_ref() == Some(&entry.namespace);
    if !namespace_matches || classify_shareable(&entry.namespace) == NamespaceKind::AppPrivate {
        return reject(entry);
    }
    match host
        .db
        .schema_manager()
        .resolve_schema(&decoded.schema)
        .await
    {
        Ok(Some(_)) => reappend(log, state, entry).await,
        Ok(None) => {
            tracing::warn!(target: "lastdbd::log_sync", "submission schema is not installed");
            Ok(false)
        }
        Err(_) => {
            tracing::warn!(target: "lastdbd::log_sync", "submission schema lookup failed");
            Ok(false)
        }
    }
}

fn reject(entry: &Entry) -> Result<bool, String> {
    tracing::warn!(
        target: "lastdbd::log_sync",
        writer_seq = entry.writer_seq,
        "submission stays unacked because reject is not a route"
    );
    Ok(false)
}

async fn reappend(log: &CloudLog, state: &mut SyncState, entry: &Entry) -> Result<bool, String> {
    let event = SyncEvent::Reappend {
        entry: entry.clone(),
        accepted_at_ms: fold_db::clock::unix_millis(),
    };
    match log.drive_once(state.clone(), event).await {
        Ok((next, Outcome::Appended { .. } | Outcome::Duplicate { .. })) => {
            *state = next;
            Ok(true)
        }
        Ok(_) => {
            tracing::warn!(target: "lastdbd::log_sync", "reappend returned an unexpected outcome");
            Ok(false)
        }
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "reappend failed");
            Ok(false)
        }
    }
}

async fn stage_snapshot(
    host: &Host,
    log: &CloudLog,
    database_id: &str,
    snapshot_ref: &str,
) -> Result<(), String> {
    let bytes = log
        .snapshot_bytes(snapshot_ref)
        .await
        .map_err(|_| "snapshot download failed".to_owned())?;
    let name = fold_db::hex::sha256_hex(snapshot_ref.as_bytes());
    let dir = host
        .home
        .join("log-sync")
        .join(database_id)
        .join("snapshots");
    disk::write_private(&dir.join(&name), &bytes)
        .map_err(|_| "cannot stage snapshot bytes".to_owned())?;
    disk::write_private(&dir.join(format!("{name}.ref")), snapshot_ref.as_bytes())
        .map_err(|_| "cannot stage snapshot ref".to_owned())
}

fn marker_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("applied")
        .join(mutation_id)
}

fn safe_id(mutation_id: &str) -> Option<&str> {
    cutover::label_ok(mutation_id).then_some(mutation_id)
}

fn schema_class(error: &SchemaError) -> &'static str {
    match error {
        SchemaError::NotFound(_) => "not_found",
        SchemaError::Blocked(_) => "blocked",
        SchemaError::ReadResourceLimit { .. } => "read_limit",
        SchemaError::WriteResourceLimit { .. } => "write_limit",
        SchemaError::StorageRead { .. } => "storage_read",
        SchemaError::InvalidField(_) => "invalid_field",
        SchemaError::InvalidData(_) => "invalid_data",
        SchemaError::ReplayBeforeDeleteCutoff { .. } => "replay_cutoff",
        SchemaError::CasConflict { .. } => "cas_conflict",
        SchemaError::RecordTooLarge { .. } => "record_too_large",
        SchemaError::InvalidCursor(_) => "invalid_cursor",
        SchemaError::StorageFull { .. } => "storage_full",
    }
}
