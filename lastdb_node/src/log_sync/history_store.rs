//! Per-database VerifiedHistory held in memory, rebuilt from one durable log.
//!
//! `authored.json` is the log of every entry this node signed, in author
//! order. `uploaded.json` holds one number: how many log entries the relay has
//! acknowledged. Pending uploads are the log entries past that cursor, so no
//! second list exists that could drift from the log.
//!
//! `received.json` holds the accepted entries of every other stream. An entry
//! this node signed can cite another writer's entry, so the authored log alone
//! cannot rebuild the history. With both files a restart rebuilds it fully,
//! without the relay.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use mutation_log::{
    stream_id_of, AcceptOutcome, DatabaseId, SignedEntry, StreamId, VerifiedHistory,
};

use crate::profile::private_fs as disk;

const LOG_LIMIT: usize = 16 * 1024 * 1024;
const CURSOR_LIMIT: usize = 32;

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
/// A history that is not in memory is rebuilt from `authored.json` and
/// `received.json`. A file that cannot be read is an error to the caller; no
/// empty history is created in its place.
pub(crate) struct HistoryMap {
    home: PathBuf,
    inner: Mutex<HashMap<String, VerifiedHistory>>,
    /// How many entries `received.json` holds, per database.
    received: Mutex<HashMap<String, usize>>,
    /// Databases whose last materialize failed; zero-accept pulls retry them.
    materialize_retry: Mutex<HashSet<String>>,
}

impl HistoryMap {
    pub(crate) fn new(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
            inner: Mutex::new(HashMap::new()),
            received: Mutex::new(HashMap::new()),
            materialize_retry: Mutex::new(HashSet::new()),
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
            Entry::Vacant(slot) => {
                let (history, received) = replay(&self.home, id)?;
                self.note_received(database_id, received);
                slot.insert(history)
            }
        };
        Ok(f(hist))
    }

    /// Write the accepted entries of every stream but `own` to `received.json`
    /// when a pull accepted new ones.
    pub(super) fn persist_received(&self, database_id: &str, own: &StreamId) -> Result<(), String> {
        let entries: Vec<SignedEntry> = self.with_mut(database_id, |history| {
            history.accepted_outside(own).into_iter().cloned().collect()
        })?;
        let held = self
            .received
            .lock()
            .map_err(|_| "history lock poisoned")?
            .get(database_id)
            .copied();
        if held == Some(entries.len()) {
            return Ok(());
        }
        store_received(&self.home, database_id, &entries)?;
        self.note_received(database_id, entries.len());
        Ok(())
    }

    fn note_received(&self, database_id: &str, count: usize) {
        if let Ok(mut held) = self.received.lock() {
            held.insert(database_id.to_owned(), count);
        }
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

    pub(super) fn mark_materialize_retry(&self, database_id: &str) {
        if let Ok(mut set) = self.materialize_retry.lock() {
            set.insert(database_id.to_owned());
        }
    }

    pub(super) fn clear_materialize_retry(&self, database_id: &str) {
        if let Ok(mut set) = self.materialize_retry.lock() {
            set.remove(database_id);
        }
    }

    pub(super) fn needs_materialize_retry(&self, database_id: &str) -> bool {
        self.materialize_retry
            .lock()
            .is_ok_and(|set| set.contains(database_id))
    }
}

/// Rebuild a history from the authored log and the received entries, and say
/// how many received entries there were. Entries are replayed in ascending
/// Lamport order, which meets every dependency before its user. A file that
/// cannot be read or parsed is an error.
fn replay(home: &Path, id: DatabaseId) -> Result<(VerifiedHistory, usize), String> {
    let database_id = id.to_hex();
    let received = load_received(home, &database_id)?;
    let count = received.len();
    let mut entries = load_authored(home, &database_id)?;
    entries.extend(received);
    entries.sort_by_key(|entry| entry.header().lamport);
    let mut history = VerifiedHistory::new(id);
    for entry in entries {
        if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "stored entry was rejected on replay");
        }
    }
    Ok((history, count))
}

/// Accept `entry` on a copy of `history`, append it to the log, then swap the
/// copy in. The log is the only durable write. A failure leaves `history` as it
/// was, and a rejected or pending entry is never appended.
///
/// Nothing is signed onto a stream whose tip is not known: while an entry of
/// that stream waits for a dependency, the history's tip is an older one, and a
/// new entry there would reuse a sequence number.
pub(super) fn commit_authored(
    home: &Path,
    history: &mut VerifiedHistory,
    entry: &SignedEntry,
) -> Result<AcceptOutcome, String> {
    if history.pending_on(&stream_id_of(entry)) {
        return Err("this stream has entries that wait for a dependency; sync first".into());
    }
    let database_id = history.database_id().to_hex();
    let mut next = history.clone();
    let outcome = next.accept_entry(entry.clone());
    if matches!(outcome, AcceptOutcome::Accept { .. }) {
        append_authored(home, &database_id, entry)?;
        *history = next;
    }
    Ok(outcome)
}

fn log_dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id)
}

/// Entries this node signed for the database, in author order.
pub(super) fn load_authored(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    let path = log_dir(home, database_id).join("authored.json");
    let Some(bytes) =
        disk::read_limited(&path, LOG_LIMIT).map_err(|_| "cannot read authored history log")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "authored history log is invalid".into())
}

fn append_authored(home: &Path, database_id: &str, entry: &SignedEntry) -> Result<(), String> {
    let mut entries = load_authored(home, database_id)?;
    entries.push(entry.clone());
    let bytes = serde_json::to_vec(&entries).map_err(|_| "cannot encode authored history log")?;
    if bytes.len() > LOG_LIMIT {
        return Err("authored history log is full".into());
    }
    disk::write_private(&log_dir(home, database_id).join("authored.json"), &bytes)
        .map_err(|_| "cannot write authored history log".into())
}

/// Accepted entries of other streams, in ascending Lamport order.
fn load_received(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    let path = log_dir(home, database_id).join("received.json");
    let Some(bytes) =
        disk::read_limited(&path, LOG_LIMIT).map_err(|_| "cannot read received entries")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "received entries are invalid".into())
}

fn store_received(home: &Path, database_id: &str, entries: &[SignedEntry]) -> Result<(), String> {
    let bytes = serde_json::to_vec(entries).map_err(|_| "cannot encode received entries")?;
    if bytes.len() > LOG_LIMIT {
        return Err("received entries are full".into());
    }
    disk::write_private(&log_dir(home, database_id).join("received.json"), &bytes)
        .map_err(|_| "cannot write received entries".into())
}

/// How many log entries the relay has acknowledged. Absent means none.
pub(super) fn load_uploaded(home: &Path, database_id: &str) -> Result<usize, String> {
    let path = log_dir(home, database_id).join("uploaded.json");
    let Some(bytes) =
        disk::read_limited(&path, CURSOR_LIMIT).map_err(|_| "cannot read upload cursor")?
    else {
        return Ok(0);
    };
    std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| "upload cursor is invalid".into())
}

pub(super) fn store_uploaded(home: &Path, database_id: &str, through: usize) -> Result<(), String> {
    let path = log_dir(home, database_id).join("uploaded.json");
    disk::write_private(&path, through.to_string().as_bytes())
        .map_err(|_| "cannot write upload cursor".into())
}

/// The log, the cursor, and the entries past it.
pub(super) struct Pending {
    pub(super) log_len: usize,
    pub(super) uploaded: usize,
    pub(super) batch: Vec<SignedEntry>,
}

/// Up to `limit` log entries past the cursor. A cursor beyond the log is an
/// error, not a reason to upload nothing.
pub(super) fn pending(home: &Path, database_id: &str, limit: usize) -> Result<Pending, String> {
    let log = load_authored(home, database_id)?;
    let uploaded = load_uploaded(home, database_id)?;
    if uploaded > log.len() {
        return Err(format!(
            "upload cursor {uploaded} is past the log length {}",
            log.len()
        ));
    }
    let batch = log.iter().skip(uploaded).take(limit).cloned().collect();
    Ok(Pending {
        log_len: log.len(),
        uploaded,
        batch,
    })
}

/// Entries signed here and not yet acknowledged by the relay.
pub(super) fn pending_count(home: &Path, database_id: &str) -> Result<u64, String> {
    let found = pending(home, database_id, 0)?;
    Ok((found.log_len - found.uploaded) as u64)
}
