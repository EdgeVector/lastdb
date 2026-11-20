//! Per-database VerifiedHistory held in memory; outbox persisted to disk.
//!
//! Full history rebuild comes from the relay on restore. Phase 1 does not
//! serialize VerifiedHistory (crate has no serde on that type).

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use mutation_log::{entry_hash, AcceptOutcome, DatabaseId, Outbox, SignedEntry, VerifiedHistory};

use crate::profile::private_fs as disk;

const OUTBOX_LIMIT: usize = 16 * 1024 * 1024;

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
    let entries = match load_authored(home, &id.to_hex()) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "authored history log is unreadable");
            return history;
        }
    };
    for entry in entries {
        if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "authored entry was rejected on replay");
        }
    }
    history
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

pub(super) fn persist_outbox(
    home: &Path,
    database_id: &str,
    outbox: &Outbox,
) -> Result<(), String> {
    let entries: Vec<SignedEntry> = outbox.batch(usize::MAX).into_iter().cloned().collect();
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
