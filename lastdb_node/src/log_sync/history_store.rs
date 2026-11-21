//! Per-database VerifiedHistory held in memory; outbox persisted to disk.
//!
//! Full history rebuild comes from the relay on restore. Phase 1 does not
//! serialize VerifiedHistory (crate has no serde on that type).

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use mutation_log::{
    entry_hash, AcceptOutcome, DatabaseId, EntryHash, SignedEntry, VerifiedHistory,
};
use serde::{Deserialize, Serialize};

use crate::profile::private_fs as disk;

const OUTBOX_LIMIT: usize = 16 * 1024 * 1024;

/// The histories of one node home, shared by the sync loop and every request
/// handler. Host sits below log_sync in the layer order and cannot hold the
/// map, so the map is looked up by home and created on first use.
pub(crate) fn histories_for(home: &Path) -> Arc<HistoryMap> {
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HistoryMap>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        homes
            .entry(home.to_path_buf())
            .or_insert_with(|| Arc::new(HistoryMap::new(home))),
    )
}

/// Process-wide histories, shared by the sync loop and every request handler.
///
/// A history that is not in memory is rebuilt from `authored.json`, the log of
/// entries this node signed. Entries from other writers return by relay pull.
pub(crate) struct HistoryMap {
    home: PathBuf,
    inner: Mutex<HashMap<String, VerifiedHistory>>,
}

impl HistoryMap {
    pub(crate) fn new(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn with_mut<R>(
        &self,
        database_id: &str,
        f: impl FnOnce(&mut VerifiedHistory) -> R,
    ) -> Result<R, String> {
        let mut map = self.inner.lock().map_err(|_| "history lock poisoned")?;
        let id = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
        let hist = match map.entry(database_id.to_owned()) {
            Entry::Occupied(held) => held.into_mut(),
            Entry::Vacant(slot) => slot.insert(replay_authored(&self.home, id)),
        };
        Ok(f(hist))
    }

    /// Read a history that is already in memory. Never creates one.
    pub(crate) fn peek_with<R>(
        &self,
        database_id: &str,
        f: impl FnOnce(&VerifiedHistory) -> R,
    ) -> Option<R> {
        let map = self.inner.lock().ok()?;
        map.get(database_id).map(f)
    }

    /// Copy of the database's history, so the lock is not held across `.await`.
    pub(super) fn snapshot(&self, database_id: &str) -> Result<VerifiedHistory, String> {
        self.with_mut(database_id, |history| history.clone())
    }

    pub(super) fn insert(&self, history: VerifiedHistory) -> Result<(), String> {
        let id = history.database_id().to_hex();
        let mut map = self.inner.lock().map_err(|_| "history lock poisoned")?;
        map.insert(id, history);
        Ok(())
    }
}

pub(super) fn replay_authored(home: &Path, id: DatabaseId) -> VerifiedHistory {
    let mut history = VerifiedHistory::new(id);
    let database_id = id.to_hex();
    let entries = match load_authored(home, &database_id) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "authored history log is unreadable");
            return load_disk_outbox(home, &database_id, history);
        }
    };
    for entry in entries {
        if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "authored entry was rejected on replay");
        }
    }
    // Authored accept leaves the outbox empty. Rebuild pending uploads from
    // authored rows past the upload-ack cursor (birth/grant/data/…), then any
    // remaining disk outbox rows. Authored order first so birth stays ahead of
    // a later grant that survived alone in outbox.json after a local drop.
    rebuild_outbox_after_replay(home, &database_id, history)
}

pub(super) fn authored_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("authored.json")
}

/// Entries this node signed for the database, in author order.
pub(super) fn load_authored(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    let path = authored_path(home, database_id);
    let Some(bytes) =
        disk::read_limited(&path, OUTBOX_LIMIT).map_err(|_| "cannot read authored history log")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "authored history log is invalid".into())
}

pub(super) fn append_authored(
    home: &Path,
    database_id: &str,
    entry: &SignedEntry,
) -> Result<(), String> {
    let mut entries = load_authored(home, database_id)?;
    entries.push(entry.clone());
    let bytes = serde_json::to_vec(&entries).map_err(|_| "cannot encode authored history log")?;
    disk::write_private(&authored_path(home, database_id), &bytes)
        .map_err(|_| "cannot write authored history log".into())
}

pub(super) fn outbox_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("outbox.json")
}

pub(super) fn upload_ack_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("upload_ack.json")
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct UploadAck {
    /// Count of leading `authored.json` entries confirmed uploaded to the relay.
    acked_through: usize,
}

/// How many leading authored entries have a successful log/append ack.
pub(super) fn load_upload_ack(home: &Path, database_id: &str) -> usize {
    let path = upload_ack_path(home, database_id);
    let Ok(Some(bytes)) = disk::read_limited(&path, 4096) else {
        return 0;
    };
    serde_json::from_slice::<UploadAck>(&bytes).map_or(0, |ack| ack.acked_through)
}

fn store_upload_ack(home: &Path, database_id: &str, acked_through: usize) -> Result<(), String> {
    let bytes = serde_json::to_vec(&UploadAck { acked_through })
        .map_err(|_| "cannot encode upload ack cursor")?;
    disk::write_private(&upload_ack_path(home, database_id), &bytes)
        .map_err(|_| "cannot write upload ack cursor".into())
}

/// After a successful outbox upload, advance the authored upload-ack cursor
/// through every contiguous leading entry included in `batch`.
pub(super) fn advance_upload_ack(
    home: &Path,
    database_id: &str,
    batch: &[SignedEntry],
) -> Result<(), String> {
    let authored = load_authored(home, database_id)?;
    let mut through = load_upload_ack(home, database_id);
    let uploaded = batch_hashes(batch);
    while through < authored.len() {
        let Ok(hash) = entry_hash(&authored[through]) else {
            break;
        };
        if !uploaded.contains(&hash) {
            break;
        }
        through += 1;
    }
    store_upload_ack(home, database_id, through)
}

fn batch_hashes(batch: &[SignedEntry]) -> HashSet<EntryHash> {
    let mut uploaded = HashSet::with_capacity(batch.len());
    for entry in batch {
        if let Ok(hash) = entry_hash(entry) {
            uploaded.insert(hash);
        }
    }
    uploaded
}

pub(super) fn load_outbox_entries(
    home: &Path,
    database_id: &str,
) -> Result<Vec<SignedEntry>, String> {
    let path = outbox_path(home, database_id);
    let Some(bytes) = disk::read_limited(&path, OUTBOX_LIMIT).map_err(|_| "cannot read outbox")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "outbox is invalid".into())
}

pub(super) fn store_outbox_entries(
    home: &Path,
    database_id: &str,
    entries: &[SignedEntry],
) -> Result<(), String> {
    let path = outbox_path(home, database_id);
    let bytes = serde_json::to_vec(entries).map_err(|_| "cannot encode outbox")?;
    disk::write_private(&path, &bytes).map_err(|_| "cannot write outbox".into())
}

/// Write `outbox.json` after restoring any authored rows past the upload-ack
/// cursor. Occupied `with_mut` never reloads disk; without this, a thinner
/// in-memory outbox would wipe unacked birth/grant for the rest of the process.
pub(super) fn persist_outbox(
    home: &Path,
    database_id: &str,
    history: &mut VerifiedHistory,
) -> Result<(), String> {
    requeue_unacked_authored(home, database_id, history);
    let entries: Vec<SignedEntry> = history
        .outbox()
        .batch(usize::MAX)
        .into_iter()
        .cloned()
        .collect();
    store_outbox_entries(home, database_id, &entries)
}

pub(super) fn requeue_outbox(
    history: &mut VerifiedHistory,
    entries: Vec<SignedEntry>,
) -> Result<(), String> {
    for entry in entries {
        let hash = entry_hash(&entry).map_err(|e| e.to_string())?;
        history.outbox_mut().enqueue(hash, entry);
    }
    Ok(())
}

/// Requeue authored entries not yet covered by the upload-ack cursor.
///
/// Every path that appends to `authored.json` also enqueues the outbox
/// (chain_author, mirror, apply), so all authored kinds are outbox-eligible.
fn requeue_unacked_authored(home: &Path, database_id: &str, history: &mut VerifiedHistory) {
    let authored = match load_authored(home, database_id) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %error,
                id = %database_id,
                "authored history log is unreadable while requeueing orphans"
            );
            return;
        }
    };
    let acked = load_upload_ack(home, database_id);
    for entry in authored.into_iter().skip(acked) {
        let Ok(hash) = entry_hash(&entry) else {
            continue;
        };
        history.outbox_mut().enqueue(hash, entry);
    }
}

fn rebuild_outbox_after_replay(
    home: &Path,
    database_id: &str,
    mut history: VerifiedHistory,
) -> VerifiedHistory {
    // Read disk before any persist so bootstrap-only rows are not lost when
    // authored orphans are written back to outbox.json.
    let disk_entries = match load_outbox_entries(home, database_id) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %error,
                id = %database_id,
                "disk outbox is unreadable after authored replay"
            );
            Vec::new()
        }
    };
    requeue_unacked_authored(home, database_id, &mut history);
    if let Err(error) = requeue_outbox(&mut history, disk_entries) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %error,
            id = %database_id,
            "disk outbox could not be requeued after authored replay"
        );
    }
    if !history.outbox().is_empty() {
        if let Err(error) = persist_outbox(home, database_id, &mut history) {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %error,
                id = %database_id,
                "could not persist outbox after requeueing authored orphans"
            );
        }
    }
    history
}

fn load_disk_outbox(
    home: &Path,
    database_id: &str,
    mut history: VerifiedHistory,
) -> VerifiedHistory {
    match load_outbox_entries(home, database_id) {
        Ok(entries) if !entries.is_empty() => {
            if let Err(error) = requeue_outbox(&mut history, entries) {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    %error,
                    id = %database_id,
                    "disk outbox could not be requeued after authored replay"
                );
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %error,
                id = %database_id,
                "disk outbox is unreadable after authored replay"
            );
        }
    }
    history
}
