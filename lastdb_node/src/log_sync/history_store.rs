//! Per-database VerifiedHistory held in memory, rebuilt from one durable log.
//! It serves the shared databases. The personal database keeps no history: it is
//! signed from the spool and applied through the store (see `personal`).
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
use zeroize::Zeroizing;

use mutation_log::{
    entry_hash, stream_id_of, AcceptOutcome, DatabaseId, EntryHash, EntryKind, SignedEntry,
    StreamId, VerifiedHistory,
};

use crate::host::Host;
use crate::profile::private_fs as disk;

const LOG_LIMIT: usize = 16 * 1024 * 1024;
const CURSOR_LIMIT: usize = 32;

/// The histories of one node home, shared by the sync loop and every request
/// handler. Host sits below log_sync in the layer order and cannot hold the
/// map, so the map is looked up by home and created on first use.
pub(crate) fn histories_for(host: &Host) -> Arc<HistoryMap> {
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HistoryMap>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        homes
            .entry(host.home.clone())
            .or_insert_with(|| Arc::new(HistoryMap::new(host))),
    )
}

/// Process-wide histories, shared by the sync loop and every request handler.
///
/// A history that is not in memory is rebuilt from `authored.json` and
/// `received.json`. A file that cannot be read is an error to the caller; no
/// empty history is created in its place.
pub(crate) struct HistoryMap {
    home: PathBuf,
    node_seed: Zeroizing<[u8; 32]>,
    inner: Mutex<HashMap<String, VerifiedHistory>>,
    /// Exact entry hashes stored in `received.json`, per database.
    received: Mutex<HashMap<String, Vec<EntryHash>>>,
    /// Databases whose last materialize failed; zero-accept pulls retry them.
    materialize_retry: Mutex<HashSet<String>>,
    /// Per-database async lock serializing owner `/v1/write` and proposal incorporation.
    write_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl HistoryMap {
    fn new(host: &Host) -> Self {
        Self {
            home: host.home.clone(),
            node_seed: Zeroizing::new(host.keypair.secret_key_bytes()),
            inner: Mutex::new(HashMap::new()),
            received: Mutex::new(HashMap::new()),
            materialize_retry: Mutex::new(HashSet::new()),
            write_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn write_lock(&self, database_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .write_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            locks
                .entry(database_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
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
                let (history, saved) = self.replay(id)?;
                if let Some(hashes) = saved {
                    self.note_received(database_id, hashes);
                }
                slot.insert(history)
            }
        };
        Ok(f(hist))
    }

    /// Write the accepted entries of every stream but `own` to `received.json`
    /// when the accepted set differs from what `received.json` already holds.
    pub(super) fn persist_received(&self, database_id: &str, own: &StreamId) -> Result<(), String> {
        let entries: Vec<SignedEntry> = self.with_mut(database_id, |history| {
            history.accepted_outside(own).into_iter().cloned().collect()
        })?;
        let hashes: Vec<EntryHash> = entries
            .iter()
            .map(|e| entry_hash(e).map_err(|_| "cannot hash received entry".to_owned()))
            .collect::<Result<_, _>>()?;
        let unchanged = self
            .received
            .lock()
            .map_err(|_| "history lock poisoned")?
            .get(database_id)
            .is_some_and(|held| *held == hashes);
        if unchanged {
            return Ok(());
        }
        store_received(&self.home, database_id, &entries)?;
        self.note_received(database_id, hashes);
        Ok(())
    }

    fn note_received(&self, database_id: &str, hashes: Vec<EntryHash>) {
        if let Ok(mut held) = self.received.lock() {
            held.insert(database_id.to_owned(), hashes);
        }
    }

    /// Read a history held in memory, or lazily replay persisted logs on disk
    /// when present. Never creates an empty history for an unpersisted database.
    pub(crate) fn peek_with<R>(
        &self,
        database_id: &str,
        f: impl FnOnce(&VerifiedHistory) -> R,
    ) -> Option<R> {
        let mut map = self.inner.lock().ok()?;
        if let Some(history) = map.get(database_id) {
            return Some(f(history));
        }
        if !has_persisted_log(&self.home, database_id) {
            return None;
        }
        let id = DatabaseId::from_hex(database_id).ok()?;
        let (history, saved) = self.replay(id).ok()?;
        if let Some(hashes) = saved {
            self.note_received(database_id, hashes);
        }
        let inserted = map.entry(database_id.to_owned()).or_insert(history);
        Some(f(inserted))
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

impl HistoryMap {
    /// Rebuild a history from the authored log and the received entries.
    /// `Birth` replays first, then entries in deterministic `(lamport, node_key,
    /// device_id, writer_seq)` order.
    fn replay(&self, id: DatabaseId) -> Result<(VerifiedHistory, Option<Vec<EntryHash>>), String> {
        let database_id = id.to_hex();
        let received = load_received(&self.home, &database_id)?;
        let saved: Option<Vec<EntryHash>> = received.iter().map(|e| entry_hash(e).ok()).collect();
        let mut entries = load_authored(&self.home, &database_id)?;
        entries.extend(received);
        entries.sort_by_key(|entry| {
            let h = entry.header();
            (
                entry.kind() != EntryKind::Birth,
                h.lamport,
                h.node_key,
                h.device_id,
                h.writer_seq,
            )
        });
        let mut history = super::shared_crypto::history(&self.home, &self.node_seed, id)?;
        for entry in entries {
            if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
                tracing::warn!(target: "lastdbd::log_sync", ?reason, "stored entry was rejected on replay");
            }
        }
        Ok((history, saved))
    }
}

/// Accept `entry` on a copy of `history`, append it to the log, then swap the
/// copy in. The log is the only durable write. A failure leaves `history` as it
/// was, and a rejected or pending entry is never appended.
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
        append_authored_slice(home, &database_id, std::slice::from_ref(entry))?;
        *history = next;
    }
    Ok(outcome)
}

/// Build and accept a batch of authored entries on a copy of `history`, write
/// them to `authored.json` in one atomic write, and swap the copy in.
pub(super) fn commit_authored_batch(
    home: &Path,
    history: &mut VerifiedHistory,
    build: impl FnOnce(&mut VerifiedHistory) -> Result<Vec<SignedEntry>, String>,
) -> Result<(), String> {
    let database_id = history.database_id().to_hex();
    let mut next = history.clone();
    let batch = build(&mut next)?;
    if batch.is_empty() {
        return Ok(());
    }
    append_authored_slice(home, &database_id, &batch)?;
    *history = next;
    Ok(())
}

fn log_dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id)
}

fn has_persisted_log(home: &Path, database_id: &str) -> bool {
    let dir = log_dir(home, database_id);
    dir.join("authored.json").is_file() || dir.join("received.json").is_file()
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

fn append_authored_slice(
    home: &Path,
    database_id: &str,
    slice: &[SignedEntry],
) -> Result<(), String> {
    let mut entries = load_authored(home, database_id)?;
    entries.extend_from_slice(slice);
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
