//! In-memory authored outbox, upload cursor, and received-entry persistence.
//!
//! Each database's authored entries and `uploaded.json` cursor are loaded once
//! from disk and kept in memory so `/v1/write`, proposal submission, lease
//! re-signing, and upload checks never re-read or re-parse `authored.json`.
//! Writes still update `authored.json` and `uploaded.json` atomically on disk;
//! if `rename` succeeds before directory sync fails, in-memory state still
//! advances with the renamed file so the node can never sign a duplicate
//! `writer_seq` (equivocation) on the next write.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use mutation_log::SignedEntry;

use crate::profile::private_fs as disk;

const LOG_LIMIT: usize = 16 * 1024 * 1024;
const CURSOR_LIMIT: usize = 32;

struct Outbox {
    authored: Vec<SignedEntry>,
    uploaded: usize,
}

type HomeOutboxes = Mutex<HashMap<String, Outbox>>;

/// The log, the cursor, and the entries past it.
pub(in crate::log_sync) struct Pending {
    pub(in crate::log_sync) log_len: usize,
    pub(in crate::log_sync) uploaded: usize,
    pub(in crate::log_sync) batch: Vec<SignedEntry>,
}

fn outboxes_for(home: &Path) -> Arc<HomeOutboxes> {
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HomeOutboxes>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        homes
            .entry(home.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new()))),
    )
}

fn with_outbox<R>(
    home: &Path,
    database_id: &str,
    f: impl FnOnce(&mut Outbox) -> Result<R, String>,
) -> Result<R, String> {
    let map = outboxes_for(home);
    let mut guard = map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let slot = match guard.entry(database_id.to_owned()) {
        Entry::Occupied(held) => held.into_mut(),
        Entry::Vacant(vacant) => {
            let authored = load_authored_disk(home, database_id)?;
            let uploaded = load_uploaded_disk(home, database_id)?;
            vacant.insert(Outbox { authored, uploaded })
        }
    };
    f(slot)
}

fn log_dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id)
}

pub(super) fn has_persisted_log(home: &Path, database_id: &str) -> bool {
    let dir = log_dir(home, database_id);
    dir.join("authored.json").is_file() || dir.join("received.json").is_file()
}

pub(super) fn cached_authored(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    with_outbox(home, database_id, |slot| Ok(slot.authored.clone()))
}

pub(super) fn append_authored_slice(
    home: &Path,
    database_id: &str,
    slice: &[SignedEntry],
) -> Result<(), String> {
    if slice.is_empty() {
        return Ok(());
    }
    with_outbox(home, database_id, |slot| {
        let mut next = Vec::with_capacity(slot.authored.len() + slice.len());
        next.extend_from_slice(&slot.authored);
        next.extend_from_slice(slice);
        let bytes = serde_json::to_vec(&next).map_err(|_| "cannot encode authored history log")?;
        if bytes.len() > LOG_LIMIT {
            return Err("authored history log is full".into());
        }
        let path = log_dir(home, database_id).join("authored.json");
        match disk::write_private_with(&path, &bytes, disk::Flush::Ordered) {
            Ok(()) => {
                slot.authored = next;
                Ok(())
            }
            Err(error) => {
                if load_authored_disk(home, database_id)
                    .is_ok_and(|on_disk| on_disk.len() == next.len())
                {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        id = %database_id,
                        %error,
                        "authored log rename completed before directory sync failed; advancing in-memory state"
                    );
                    slot.authored = next;
                    return Ok(());
                }
                Err("cannot write authored history log".into())
            }
        }
    })
}

fn load_authored_disk(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    let path = log_dir(home, database_id).join("authored.json");
    let Some(bytes) =
        disk::read_limited(&path, LOG_LIMIT).map_err(|_| "cannot read authored history log")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "authored history log is invalid".into())
}

/// Accepted entries of other streams, in ascending Lamport order.
pub(super) fn load_received(home: &Path, database_id: &str) -> Result<Vec<SignedEntry>, String> {
    let path = log_dir(home, database_id).join("received.json");
    let Some(bytes) =
        disk::read_limited(&path, LOG_LIMIT).map_err(|_| "cannot read received entries")?
    else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| "received entries are invalid".into())
}

pub(super) fn store_received(
    home: &Path,
    database_id: &str,
    entries: &[SignedEntry],
) -> Result<(), String> {
    let bytes = serde_json::to_vec(entries).map_err(|_| "cannot encode received entries")?;
    if bytes.len() > LOG_LIMIT {
        return Err("received entries are full".into());
    }
    disk::write_private_with(
        &log_dir(home, database_id).join("received.json"),
        &bytes,
        disk::Flush::Ordered,
    )
    .map_err(|_| "cannot write received entries".into())
}

fn load_uploaded_disk(home: &Path, database_id: &str) -> Result<usize, String> {
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

/// How many log entries the relay has acknowledged. Absent means none.
pub(in crate::log_sync) fn load_uploaded(home: &Path, database_id: &str) -> Result<usize, String> {
    let map = outboxes_for(home);
    if let Some(uploaded) = map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(database_id)
        .map(|slot| slot.uploaded)
    {
        return Ok(uploaded);
    }
    load_uploaded_disk(home, database_id)
}

pub(in crate::log_sync) fn store_uploaded(
    home: &Path,
    database_id: &str,
    through: usize,
) -> Result<(), String> {
    with_outbox(home, database_id, |slot| {
        let path = log_dir(home, database_id).join("uploaded.json");
        // A cursor that a power cut sets back only sends entries again, and the relay answers
        // `duplicate` for the same bytes. An ordered write keeps the file whole.
        match disk::write_private_with(&path, through.to_string().as_bytes(), disk::Flush::Ordered)
        {
            Ok(()) => {
                slot.uploaded = through;
                Ok(())
            }
            Err(_) if load_uploaded_disk(home, database_id) == Ok(through) => {
                slot.uploaded = through;
                Ok(())
            }
            Err(_) => Err("cannot write upload cursor".into()),
        }
    })
}

/// Up to `limit` log entries past the cursor. A cursor beyond the log is an
/// error, not a reason to upload nothing.
pub(in crate::log_sync) fn pending(
    home: &Path,
    database_id: &str,
    limit: usize,
) -> Result<Pending, String> {
    with_outbox(home, database_id, |slot| {
        if slot.uploaded > slot.authored.len() {
            return Err(format!(
                "upload cursor {} is past the log length {}",
                slot.uploaded,
                slot.authored.len()
            ));
        }
        let batch = slot
            .authored
            .iter()
            .skip(slot.uploaded)
            .take(limit)
            .cloned()
            .collect();
        Ok(Pending {
            log_len: slot.authored.len(),
            uploaded: slot.uploaded,
            batch,
        })
    })
}

/// Entries signed here and not yet acknowledged by the relay.
pub(in crate::log_sync) fn pending_count(home: &Path, database_id: &str) -> Result<u64, String> {
    with_outbox(home, database_id, |slot| {
        if slot.uploaded > slot.authored.len() {
            return Err(format!(
                "upload cursor {} is past the log length {}",
                slot.uploaded,
                slot.authored.len()
            ));
        }
        Ok((slot.authored.len() - slot.uploaded) as u64)
    })
}
