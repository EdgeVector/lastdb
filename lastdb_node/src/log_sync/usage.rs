//! What this device put on the relay and the relay accepted: an account kept beside the sync state.
//!
//! `log-sync/<database>/cloud-usage.json` holds cumulative counts and lengths:
//!
//! - entries: every signed entry the relay answered `stored` or `duplicate` to, once each, counted
//!   by the length of its JSON as sent (the length a reference relay stores);
//! - blobs: every blob this device uploaded and committed, counted once, by its length;
//! - released: what the relay said a `log.trim` that this device asked for dropped (`entries_deleted`
//!   and `released_bytes`). A trim also drops entries of the other devices of the owner, so the
//!   released numbers are the relay's account of its own deletions, not a share of this device.
//!
//! The number is "what this device put and the relay accepted", minus what trims it requested
//! released. It is not what the account is billed: the entries and blobs of another device are not in
//! it, and a relay may measure stored bytes differently. A status that shows a device shows only that
//! device's file.
//!
//! The file is the one record. It is written whole with a rename after each relay answer:
//!
//! - an entry is counted at most once, by the highest `writer_seq` counted on its stream
//!   (`streams`). The sender keeps a signed batch until the spool confirms it, so after a kill -9 the
//!   same bytes are sent again, the relay answers `duplicate`, and the entry is counted then if it was
//!   not before;
//! - a blob is marked in `pending` before its upload begins and unmarked when the upload commits. A
//!   blob that the relay already holds counts only when it was marked before this try: an earlier
//!   try that was cut off after the commit. A blob another upload put is not counted again;
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
    /// Blobs whose upload began and did not end, by digest, with their length.
    #[serde(default)]
    pending: BTreeMap<String, u64>,
}

/// The account of one home.
#[derive(Clone)]
pub(super) struct Ledger {
    home: PathBuf,
}

fn path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join(FILE)
}

fn load(home: &Path, database_id: &str) -> Result<Saved, String> {
    match disk::read_limited(&path(home, database_id), LIMIT) {
        Ok(Some(bytes)) => {
            serde_json::from_slice(&bytes).map_err(|_| "the cloud usage record is invalid".into())
        }
        Ok(None) => Ok(Saved::default()),
        Err(_) => Err("cannot read the cloud usage record".into()),
    }
}

fn store(home: &Path, database_id: &str, saved: &mut Saved) -> Result<(), String> {
    saved.usage.unconfirmed_blobs = saved.pending.len() as u64;
    saved.usage.unconfirmed_blob_bytes = saved.pending.values().sum();
    let bytes = serde_json::to_vec(saved).map_err(|_| "cannot encode the cloud usage record")?;
    disk::write_private(&path(home, database_id), &bytes)
        .map_err(|_| "cannot save the cloud usage record".to_owned())
}

fn lock() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Ledger {
    pub(super) fn new(home: &Path) -> Self {
        Self {
            home: home.to_owned(),
        }
    }

    /// The relay accepted these entries. Each is counted once.
    pub(super) fn entries_accepted(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<(), String> {
        let _guard = lock();
        let mut saved = load(&self.home, database_id)?;
        let mut changed = false;
        for entry in entries {
            let header = entry.header();
            let counted = saved
                .streams
                .entry(header.device_id.to_string())
                .or_insert(0);
            if header.writer_seq <= *counted {
                continue;
            }
            *counted = header.writer_seq;
            saved.usage.entries += 1;
            saved.usage.entry_bytes += serde_json::to_vec(entry)
                .map_err(|_| "cannot measure an entry")?
                .len() as u64;
            changed = true;
        }
        if changed {
            store(&self.home, database_id, &mut saved)?;
        }
        Ok(())
    }

    /// An upload of the blob `sha256` of `bytes` is about to begin. True when an earlier try of it
    /// was cut off before it finished.
    pub(super) fn blob_begins(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<bool, String> {
        let _guard = lock();
        let mut saved = load(&self.home, database_id)?;
        if saved.pending.contains_key(sha256) {
            return Ok(true);
        }
        saved.pending.insert(sha256.to_owned(), bytes);
        store(&self.home, database_id, &mut saved)?;
        Ok(false)
    }

    /// The blob is on the relay. `counted` is true when this device put it.
    pub(super) fn blob_ends(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
        counted: bool,
    ) -> Result<(), String> {
        let _guard = lock();
        let mut saved = load(&self.home, database_id)?;
        saved.pending.remove(sha256);
        if counted {
            saved.usage.blobs += 1;
            saved.usage.blob_bytes += bytes;
        }
        store(&self.home, database_id, &mut saved)
    }

    /// The relay answered a trim with what it dropped.
    pub(super) fn released(
        &self,
        database_id: &str,
        entries: u64,
        bytes: u64,
    ) -> Result<(), String> {
        if entries == 0 && bytes == 0 {
            return Ok(());
        }
        let _guard = lock();
        let mut saved = load(&self.home, database_id)?;
        saved.usage.released_entries += entries;
        saved.usage.released_bytes += bytes;
        store(&self.home, database_id, &mut saved)
    }
}

/// The totals of one database in `home`, if this device put anything.
fn read(home: &Path, database_id: &str) -> Option<CloudUsage> {
    let bytes = disk::read_limited(&path(home, database_id), LIMIT).ok()??;
    serde_json::from_slice::<Saved>(&bytes)
        .ok()
        .map(|saved| saved.usage)
}

/// Put the totals of every database of `home` into the status of the node.
pub(crate) fn fill(home: &Path, status: &mut CloudSyncStatus) {
    let Ok(dirs) = std::fs::read_dir(home.join("log-sync")) else {
        return;
    };
    for dir in dirs.flatten() {
        let Some(id) = dir.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(usage) = read(home, &id) {
            status.databases.entry(id).or_default().usage = Some(usage);
        }
    }
}
