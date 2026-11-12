//! Apply a pulled page on the booted store. The caller acks only after this returns.
//!
//! A named cursor expiry installs through `named_snapshot_restore` and returns
//! no reset. A received page replays that same typed apply and flushes before
//! this returns. This module stages bytes only when an owner reset still
//! arrives. It does not replace the live store and does not edit `registry.json`.
//! `byte_cloud_restore::native::restore_at` writes an owner byte-backup file
//! set and is not the named installer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use fold_db::schema::types::SchemaError;
use mutation_log::{Entry, Incorporation, StoredEntry};
use mutation_log_sync::Outcome;

use crate::host::Host;
use crate::named_snapshot_restore::{DeliveredEntry, IncorporatedFrom, SnapshotEntry};

use super::cloud::CloudLog;
use super::cutover;
use super::disk;
use super::payload;

const MARKER_LIMIT: usize = 64;

static SNAPSHOT_WARNED: AtomicBool = AtomicBool::new(false);
static NAMED_HELD: AtomicBool = AtomicBool::new(false);

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
    outcome: &Outcome,
    database_id: &str,
    shareable: bool,
    publisher: Option<&str>,
) -> Result<PagePlan, String> {
    let Outcome::Pulled {
        entries,
        cursor,
        reset,
    } = outcome
    else {
        return Err("pull did not return a page".into());
    };
    if let Some(publisher) = publisher {
        return received_page(
            host,
            log,
            database_id,
            publisher,
            entries,
            *cursor,
            reset.is_some(),
        )
        .await;
    }
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
        if !advance_one(host, log, database_id, shareable, stored).await? {
            return Ok(PagePlan::Hold(handled));
        }
        handled = Some(stored.delivery_seq);
    }
    Ok(PagePlan::Ack(*cursor))
}

async fn advance_one(
    host: &Host,
    log: &CloudLog,
    database_id: &str,
    shareable: bool,
    stored: &StoredEntry,
) -> Result<bool, String> {
    let entry = &stored.entry;
    if entry.writer.key.as_str() == log.principal().as_str() {
        if &entry.writer.device == log.device() {
            return mark_local(host, database_id, entry);
        }
        if shareable {
            hold_named();
            return Ok(false);
        }
        return apply_owned(host, database_id, entry).await;
    }
    if shareable {
        hold_named();
        return Ok(false);
    }
    tracing::warn!(
        target: "lastdbd::log_sync",
        delivery_seq = stored.delivery_seq,
        "personal log entry is not from this node key"
    );
    Ok(false)
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
        Ok(_) => {
            if let Ok(()) = disk::write_private(&marker, b"1") {
                Ok(true)
            } else {
                tracing::warn!(target: "lastdbd::log_sync", "applied marker was not stored");
                Ok(false)
            }
        }
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

fn mark_local(host: &Host, database_id: &str, entry: &Entry) -> Result<bool, String> {
    let name = format!("{}-{}", entry.writer.device.as_str(), entry.writer_seq);
    if !cutover::label_ok(entry.writer.device.as_str()) {
        return Ok(false);
    }
    let marker = marker_path(&host.home, database_id, &name);
    match disk::read_limited(&marker, MARKER_LIMIT) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => disk::write_private(&marker, b"1")
            .map(|()| true)
            .map_err(|_| "applied marker was not stored".to_owned()),
        Err(_) => Ok(false),
    }
}

async fn received_page(
    host: &Host,
    log: &CloudLog,
    database_id: &str,
    publisher: &str,
    entries: &[StoredEntry],
    cursor: u64,
    reset: bool,
) -> Result<PagePlan, String> {
    if reset {
        return Err("received page still carries a snapshot reset".to_owned());
    }
    let delivered: Vec<DeliveredEntry> = entries.iter().map(delivered_entry).collect();
    let mut files = super::restore::file_source(log.clone());
    crate::named_snapshot_restore::replay_delivered(
        host,
        database_id,
        publisher,
        &delivered,
        &mut files,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(PagePlan::Ack(cursor))
}

fn delivered_entry(stored: &StoredEntry) -> DeliveredEntry {
    let entry = &stored.entry;
    DeliveredEntry {
        delivery_seq: stored.delivery_seq,
        database_id: entry.database_id.as_str().to_owned(),
        entry: SnapshotEntry {
            accepted_at_ms: stored.accepted_at_ms,
            writer_key: entry.writer.key.as_str().to_owned(),
            writer_device: entry.writer.device.as_str().to_owned(),
            writer_seq: entry.writer_seq,
            authored_at_ms: entry.authored_at_ms,
            namespace: entry.namespace.as_str().to_owned(),
            incorporated_from: entry.incorporated_from.as_ref().map(incorporated_from),
            payload_b64: URL_SAFE_NO_PAD.encode(&entry.payload),
        },
    }
}

fn incorporated_from(source: &Incorporation) -> IncorporatedFrom {
    IncorporatedFrom {
        writer_key: source.writer.key.as_str().to_owned(),
        writer_device: source.writer.device.as_str().to_owned(),
        writer_seq: source.writer_seq,
    }
}

fn hold_named() {
    if !NAMED_HELD.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            "named entry stays unacked until the typed payload can be applied"
        );
    }
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
