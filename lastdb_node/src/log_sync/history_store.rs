//! Per-database VerifiedHistory held in memory; outbox persisted to disk.
//!
//! Full history rebuild comes from the relay on restore. Phase 1 does not
//! serialize VerifiedHistory (crate has no serde on that type).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use mutation_log::{entry_hash, DatabaseId, Outbox, SignedEntry, VerifiedHistory};

use crate::profile::private_fs as disk;

const OUTBOX_LIMIT: usize = 16 * 1024 * 1024;

pub(super) struct HistoryMap {
    inner: Mutex<HashMap<String, VerifiedHistory>>,
}

impl HistoryMap {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn with_mut<R>(
        &self,
        database_id: &str,
        f: impl FnOnce(&mut VerifiedHistory) -> R,
    ) -> Result<R, String> {
        let mut map = self.inner.lock().map_err(|_| "history lock poisoned")?;
        let id = DatabaseId::from_hex(database_id).map_err(|_| "database_id is not hex32")?;
        let hist = map
            .entry(database_id.to_owned())
            .or_insert_with(|| VerifiedHistory::new(id));
        Ok(f(hist))
    }

    #[allow(dead_code)]
    pub(super) fn get_clone(&self, database_id: &str) -> Option<VerifiedHistory> {
        let map = self.inner.lock().ok()?;
        map.get(database_id).cloned()
    }

    pub(super) fn insert(&self, history: VerifiedHistory) -> Result<(), String> {
        let id = history.database_id().to_hex();
        let mut map = self.inner.lock().map_err(|_| "history lock poisoned")?;
        map.insert(id, history);
        Ok(())
    }
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
