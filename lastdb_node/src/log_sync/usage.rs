//! What this device put on the relay and the relay accepted: an account kept beside the sync state.
//!
//! `log-sync/<database>/cloud-usage.json` holds cumulative counts and lengths:
//!
//! - entries: every signed entry the relay answered `stored` or `duplicate` to, once each, counted
//!   by the length of its JSON as sent (the length a reference relay stores);
//! - blobs: every blob this device uploaded and committed, counted once, by its length;
//! - released: what the relay said a `log.trim` that this device asked for dropped (`entries_deleted`
//!   and `released_bytes`). A trim also drops entries of the other devices of the owner, so the
//!   released numbers are the relay's account of its own deletions, not a share of this device;
//! - released blobs: what the relay said a `blob.drop` that this device asked for dropped (`dropped`
//!   and `released_bytes`). A sweep drops every stored file that the newest snapshot and the entries
//!   after it do not name, whichever device put it, so these are the relay's own deletions too.
//!
//! The account has a start (`since_ms`). On a home that already uploaded before this file existed
//! (an earlier build), `complete` is false: what it put before is not in the totals, and nothing
//! here says what it holds on the relay. There is no backfill. `complete` is true when the home
//! had no prior uploads when the account began, including a fresh or restored home whose first
//! upload is a snapshot entry above sequence 1.
//!
//! The number is "what this device put and the relay accepted", minus what the trims and the sweeps
//! it requested released. It is not what the account is billed: the entries and blobs of another device are not in
//! it, and a relay may measure stored bytes differently. A status that shows a device shows only that
//! device's file.
//!
//! The file is the one record. It is written whole with a rename after each relay answer:
//!
//! - an entry is counted at most once, by the highest `writer_seq` counted on its stream
//!   (`streams`). The sender keeps a signed batch until the spool confirms it, so after a kill -9 the
//!   same bytes are sent again, the relay answers `duplicate`, and the entry is counted then if it was
//!   not before;
//! - a blob is marked in `pending` right before `stage: "commit"` is sent and unmarked when the
//!   commit succeeds or receives a definitive HTTP 4xx refusal. A blob that `stage: "begin"` says
//!   the relay already holds counts only when `pending` already held it from an earlier cut-off
//!   commit. A blob another device put is not counted again. A blob in `pending` that a
//!   `blob.list` of a sweep lists with its length was committed (the node stopped before it read
//!   the answer, and sent a different segment after): it counts then, before the sweep can drop
//!   it as named by nothing, so its release never counts without its put. A `pending` digest the
//!   same listing does not hold is abandoned without counting: the commit never published it. A
//!   commit counts only while its blob is in `pending`, so a commit that ends after that, or a
//!   second commit of the same blob at the same time, does not count it twice;
//! - a kill -9 in the moment between a relay answer and the write of this file for a snapshot entry
//!   or a trim answer, which nothing sends again, is not repaired.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use mutation_log::SignedEntry;
use serde::{Deserialize, Serialize};

use crate::host::cloud_sync::{CloudSyncStatus, CloudUsage};
use crate::profile::private_fs as disk;

const FILE: &str = "cloud-usage.json";
const LIMIT: usize = 4 * 1024 * 1024;

/// One writer at a time: the lane and the sync turn both put things.
static LOCK: Mutex<()> = Mutex::new(());

#[derive(Default, Serialize, Deserialize)]
struct Saved {
    #[serde(flatten)]
    usage: CloudUsage,
    /// The highest `writer_seq` counted on each device stream, by device id.
    #[serde(default)]
    streams: BTreeMap<String, u64>,
    /// Legacy field from builds that inferred completeness from the first stream sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_after: Option<u64>,
    /// True once `usage.complete` was determined at account creation.
    #[serde(default)]
    complete_decided: bool,
    /// Blobs whose commit began and did not end, by digest, with their length.
    #[serde(default)]
    pending: BTreeMap<String, u64>,
}

/// The account of one home.
#[derive(Clone)]
pub(super) struct Ledger {
    home: PathBuf,
    /// The acknowledged spool position of the home when the ledger was made.
    spool_acked: u64,
}

fn path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join(FILE)
}

fn had_prior_uploads(home: &Path, database_id: &str, spool_acked: u64) -> bool {
    let registered = crate::host::owned::registered_databases(home);
    let is_personal = if registered.is_empty() {
        !home
            .join("log-sync")
            .join(database_id)
            .join("authored.json")
            .is_file()
    } else {
        registered
            .iter()
            .any(|db| !db.native && db.id == database_id)
    };
    if is_personal {
        spool_acked > 0
            || home
                .join("log-sync")
                .join(database_id)
                .join("snapshot-last.json")
                .is_file()
    } else {
        super::history_store::load_uploaded(home, database_id).unwrap_or(0) > 0
    }
}

fn normalize(home: &Path, database_id: &str, spool_acked: u64, saved: &mut Saved) {
    if !saved.complete_decided {
        saved.usage.complete = match saved.started_after {
            Some(after) => after == 0,
            None => !had_prior_uploads(home, database_id, spool_acked) && saved.usage.entries == 0,
        };
        saved.started_after = None;
        saved.complete_decided = true;
    }
}

fn load(home: &Path, database_id: &str, spool_acked: u64) -> Result<Saved, String> {
    match disk::read_limited(&path(home, database_id), LIMIT) {
        Ok(Some(bytes)) => {
            let mut saved: Saved = serde_json::from_slice(&bytes)
                .map_err(|_| "the cloud usage record is invalid".to_owned())?;
            normalize(home, database_id, spool_acked, &mut saved);
            Ok(saved)
        }
        Ok(None) => {
            let mut saved = Saved::default();
            saved.usage.complete = !had_prior_uploads(home, database_id, spool_acked);
            saved.complete_decided = true;
            Ok(saved)
        }
        Err(_) => Err("cannot read the cloud usage record".into()),
    }
}

fn store(home: &Path, database_id: &str, saved: &mut Saved) -> Result<(), String> {
    saved.usage.unconfirmed_blobs = saved.pending.len() as u64;
    saved.usage.unconfirmed_blob_bytes = saved
        .pending
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    saved.started_after = None;
    saved.complete_decided = true;
    if saved.usage.since_ms == 0 {
        saved.usage.since_ms = fold_db::clock::unix_secs().saturating_mul(1000);
    }
    let bytes = serde_json::to_vec(saved).map_err(|_| "cannot encode the cloud usage record")?;
    // An account to show, not a record the sync reads back to decide anything. An ordered
    // write keeps the rename after the data, so a power cut leaves the previous whole file;
    // that is the loss the module text already accepts for a kill -9.
    disk::write_private_with(&path(home, database_id), &bytes, disk::Flush::Ordered)
        .map_err(|_| "cannot save the cloud usage record".to_owned())
}

fn lock() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Ledger {
    pub(super) fn new(home: &Path, spool_acked: u64) -> Self {
        Self {
            home: home.to_owned(),
            spool_acked,
        }
    }

    /// The relay accepted `accepted` of these entries (stored or duplicate). Each counts once, and
    /// only when it accepted all of them. The entries of a log segment count with no length: their
    /// bytes are the segment's blob, which counts as a blob.
    pub(super) fn entries_accepted<E: std::borrow::Borrow<SignedEntry>>(
        &self,
        database_id: &str,
        entries: &[E],
        accepted: u64,
        in_segment: bool,
    ) -> Result<(), String> {
        if accepted != entries.len() as u64 {
            return Ok(());
        }
        let _guard = lock();
        let mut saved = load(&self.home, database_id, self.spool_acked)?;
        let mut changed = false;
        for entry in entries {
            let entry = entry.borrow();
            let header = entry.header();
            let counted = saved
                .streams
                .entry(header.device_id.to_string())
                .or_insert(0);
            if header.writer_seq <= *counted {
                continue;
            }
            *counted = header.writer_seq;
            let entry_len = if in_segment {
                0
            } else {
                serde_json::to_vec(entry)
                    .map_err(|_| "cannot measure an entry")?
                    .len() as u64
            };
            saved.usage.entries = saved.usage.entries.saturating_add(1);
            saved.usage.entry_bytes = saved.usage.entry_bytes.saturating_add(entry_len);
            changed = true;
        }
        if changed {
            store(&self.home, database_id, &mut saved)?;
        }
        Ok(())
    }

    /// If `pending` holds `sha256` from an earlier cut-off commit and `begin` now says the relay
    /// holds the blob, move it from `pending` into the confirmed blob totals.
    pub(super) fn complete_pending_blob(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<(), String> {
        let _guard = lock();
        let Some(raw) = disk::read_limited(&path(&self.home, database_id), LIMIT)
            .map_err(|_| "cannot read the cloud usage record".to_owned())?
        else {
            return Ok(());
        };
        let mut saved: Saved = serde_json::from_slice(&raw)
            .map_err(|_| "the cloud usage record is invalid".to_owned())?;
        normalize(&self.home, database_id, self.spool_acked, &mut saved);
        if saved.pending.remove(sha256).is_none() {
            return Ok(());
        }
        saved.usage.blobs = saved.usage.blobs.saturating_add(1);
        saved.usage.blob_bytes = saved.usage.blob_bytes.saturating_add(bytes);
        store(&self.home, database_id, &mut saved)
    }

    /// Record that `stage: "commit"` is about to be sent for the blob `sha256` of `bytes`.
    pub(super) fn blob_begins(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<(), String> {
        let _guard = lock();
        let mut saved = load(&self.home, database_id, self.spool_acked)?;
        if !saved.pending.contains_key(sha256) {
            saved.pending.insert(sha256.to_owned(), bytes);
            store(&self.home, database_id, &mut saved)?;
        }
        Ok(())
    }

    /// Finish a pending blob commit: count it when committed, or clear `pending` on 4xx refusal.
    pub(super) fn blob_ends(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
        counted: bool,
    ) -> Result<(), String> {
        let _guard = lock();
        let mut saved = load(&self.home, database_id, self.spool_acked)?;
        if saved.pending.remove(sha256).is_none() {
            // A listing confirmed it already, or another commit of the same blob ended first.
            return Ok(());
        }
        if counted {
            saved.usage.blobs = saved.usage.blobs.saturating_add(1);
            saved.usage.blob_bytes = saved.usage.blob_bytes.saturating_add(bytes);
        }
        store(&self.home, database_id, &mut saved)
    }

    /// The relay answered a trim (`entries_deleted`, `released_bytes`).
    pub(super) fn released(
        &self,
        database_id: &str,
        answer: &relay_protocol::LogTrimAnswer,
    ) -> Result<(), String> {
        let (entries, bytes) = (answer.entries_deleted, answer.released_bytes);
        if entries == 0 && bytes == 0 {
            return Ok(());
        }
        let _guard = lock();
        let mut saved = load(&self.home, database_id, self.spool_acked)?;
        saved.usage.released_entries = saved.usage.released_entries.saturating_add(entries);
        saved.usage.released_bytes = saved.usage.released_bytes.saturating_add(bytes);
        store(&self.home, database_id, &mut saved)
    }

    /// A `blob.list` of the relay during a sweep (after the send step): every blob in `pending`
    /// that it lists with the same length was committed, so it moves into the confirmed totals.
    /// Every other `pending` digest is abandoned without counting: the commit never published a
    /// listed blob (cut off before the relay claimed it, or the claim has no content yet).
    pub(super) fn confirm_listed(
        &self,
        database_id: &str,
        listed: &BTreeMap<String, u64>,
    ) -> Result<(), String> {
        let _guard = lock();
        let Some(raw) = disk::read_limited(&path(&self.home, database_id), LIMIT)
            .map_err(|_| "cannot read the cloud usage record".to_owned())?
        else {
            return Ok(());
        };
        let mut saved: Saved = serde_json::from_slice(&raw)
            .map_err(|_| "the cloud usage record is invalid".to_owned())?;
        normalize(&self.home, database_id, self.spool_acked, &mut saved);
        if saved.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut saved.pending);
        let mut changed = false;
        for (digest, bytes) in pending {
            if listed.get(digest.as_str()) == Some(&bytes) {
                saved.usage.blobs = saved.usage.blobs.saturating_add(1);
                saved.usage.blob_bytes = saved.usage.blob_bytes.saturating_add(bytes);
                changed = true;
            } else {
                // Not listed: the commit did not land as a held blob. Drop the cut-off mark.
                changed = true;
            }
        }
        if changed {
            store(&self.home, database_id, &mut saved)?;
        }
        Ok(())
    }

    /// The relay answered a `blob.drop`: it dropped `blobs` stored files of `bytes` in all.
    pub(super) fn blobs_released(
        &self,
        database_id: &str,
        blobs: u64,
        bytes: u64,
    ) -> Result<(), String> {
        if blobs == 0 && bytes == 0 {
            return Ok(());
        }
        let _guard = lock();
        let mut saved = load(&self.home, database_id, self.spool_acked)?;
        saved.usage.released_blobs = saved.usage.released_blobs.saturating_add(blobs);
        saved.usage.released_blob_bytes = saved.usage.released_blob_bytes.saturating_add(bytes);
        store(&self.home, database_id, &mut saved)
    }
}

/// The totals of one database in `home`, if this device put anything.
fn read(home: &Path, database_id: &str, spool_acked: u64) -> Option<CloudUsage> {
    let bytes = disk::read_limited(&path(home, database_id), LIMIT).ok()??;
    let mut saved = serde_json::from_slice::<Saved>(&bytes).ok()?;
    normalize(home, database_id, spool_acked, &mut saved);
    Some(saved.usage)
}

/// Put the totals of every database of `home` into the status of the node.
pub(crate) fn fill(home: &Path, spool_acked: u64, status: &mut CloudSyncStatus) {
    let Ok(dirs) = std::fs::read_dir(home.join("log-sync")) else {
        return;
    };
    for dir in dirs.flatten() {
        let Some(id) = dir.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(usage) = read(home, &id, spool_acked) {
            status.databases.entry(id).or_default().usage = Some(usage);
        }
    }
}
