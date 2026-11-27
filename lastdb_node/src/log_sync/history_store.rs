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

mod outbox;

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use mutation_log::{
    entry_hash, AcceptOutcome, DatabaseId, EntryHash, EntryKind, SignedEntry, StreamId,
    VerifiedHistory,
};
use zeroize::Zeroizing;

use crate::host::Host;
use crate::personal_seal::Sealer;

use super::shared_key;
pub(super) use outbox::{load_uploaded, pending, pending_count, store_uploaded};

/// The histories of one node home, shared by the sync loop and every request
/// handler. Host sits below log_sync in the layer order and cannot hold the
/// map, so the map is looked up by home and created on first use.
pub(crate) fn histories_for(host: &Host) -> Arc<HistoryMap> {
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HistoryMap>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(homes.entry(host.home.clone()).or_insert_with(|| {
        Arc::new(HistoryMap::new(
            &host.home,
            Zeroizing::new(host.keypair.secret_key_bytes()),
        ))
    }))
}

/// Process-wide histories, shared by the sync loop and every request handler.
///
/// A history that is not in memory is rebuilt from `authored.json` and
/// `received.json`. A file that cannot be read is an error to the caller; no
/// empty history is created in its place.
pub(crate) struct HistoryMap {
    home: PathBuf,
    /// Node seed used to derive the publisher's shared database read key.
    node_seed: Zeroizing<[u8; 32]>,
    inner: Mutex<HashMap<String, VerifiedHistory>>,
    /// Cached database read-key sealers, shared between sealing and opening.
    sealers: Mutex<HashMap<String, Arc<Sealer>>>,
    /// Exact entry hashes stored in `received.json`, per database.
    received: Mutex<HashMap<String, Vec<EntryHash>>>,
    /// Databases whose last materialize failed; zero-accept pulls retry them.
    materialize_retry: Mutex<HashSet<String>>,
    /// Keys whose unsigned store write failed to roll back; healed on rematerialize.
    dirty_keys: Mutex<HashMap<String, BTreeSet<(String, String)>>>,
    /// Per-database async lock serializing owner `/v1/write` and proposal incorporation.
    write_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl HistoryMap {
    fn new(home: &Path, node_seed: Zeroizing<[u8; 32]>) -> Self {
        Self {
            home: home.to_path_buf(),
            node_seed,
            inner: Mutex::new(HashMap::new()),
            sealers: Mutex::new(HashMap::new()),
            received: Mutex::new(HashMap::new()),
            materialize_retry: Mutex::new(HashSet::new()),
            dirty_keys: Mutex::new(HashMap::new()),
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

    pub(super) fn sealer(&self, database_id: &str) -> Result<Arc<Sealer>, String> {
        if let Some(cached) = self
            .sealers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(database_id)
            .cloned()
        {
            return Ok(cached);
        }
        let derived = Arc::new(shared_key::sealer_with_seed(
            &self.home,
            &self.node_seed,
            database_id,
        )?);
        let mut guard = self
            .sealers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(Arc::clone(
            guard
                .entry(database_id.to_owned())
                .or_insert_with(|| Arc::clone(&derived)),
        ))
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
        outbox::store_received(&self.home, database_id, &entries)?;
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
        if !outbox::has_persisted_log(&self.home, database_id) {
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

    pub(super) fn mark_dirty_keys(&self, database_id: &str, keys: &[(String, String)]) {
        if keys.is_empty() {
            return;
        }
        if let Ok(mut map) = self.dirty_keys.lock() {
            map.entry(database_id.to_owned())
                .or_default()
                .extend(keys.iter().cloned());
        }
        self.mark_materialize_retry(database_id);
    }

    pub(super) fn has_dirty_keys(&self, database_id: &str) -> bool {
        self.dirty_keys
            .lock()
            .is_ok_and(|map| map.get(database_id).is_some_and(|set| !set.is_empty()))
    }

    pub(super) fn take_dirty_keys(&self, database_id: &str) -> BTreeSet<(String, String)> {
        self.dirty_keys
            .lock()
            .ok()
            .and_then(|mut map| map.remove(database_id))
            .unwrap_or_default()
    }

    pub(super) fn restore_dirty_keys(&self, database_id: &str, keys: BTreeSet<(String, String)>) {
        if keys.is_empty() {
            return;
        }
        if let Ok(mut map) = self.dirty_keys.lock() {
            map.entry(database_id.to_owned()).or_default().extend(keys);
        }
        self.mark_materialize_retry(database_id);
    }

    /// Rebuild a history from the authored log and the received entries.
    /// `Birth` replays first, then entries in deterministic `(lamport, node_key,
    /// device_id, writer_seq)` order.
    fn replay(&self, id: DatabaseId) -> Result<(VerifiedHistory, Option<Vec<EntryHash>>), String> {
        let database_id = id.to_hex();
        let received = outbox::load_received(&self.home, &database_id)?;
        let saved: Option<Vec<EntryHash>> = received.iter().map(|e| entry_hash(e).ok()).collect();
        let mut entries = outbox::cached_authored(&self.home, &database_id)?;
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
        let mut history = VerifiedHistory::new(id);
        let opener: Arc<dyn mutation_log::SealOpener> = self.sealer(&database_id)?;
        history.set_opener(opener);
        for entry in entries {
            if let AcceptOutcome::Reject { reason } = history.accept_entry(entry) {
                tracing::warn!(target: "lastdbd::log_sync", ?reason, "stored entry was rejected on replay");
            }
        }
        Ok((history, saved))
    }
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
    outbox::append_authored_slice(home, &database_id, &batch)?;
    *history = next;
    Ok(())
}
