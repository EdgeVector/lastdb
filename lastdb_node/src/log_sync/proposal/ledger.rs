//! Durable idempotency ledger (`proposals.json`) and per-database submit lock.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use mutation_log::EntryHash;
use serde::{Deserialize, Serialize};

use super::ContributionError;
use crate::profile::private_fs as disk;

const LEDGER_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub(in crate::log_sync) struct Submitted {
    pub(in crate::log_sync) writer_seq: u64,
    pub(in crate::log_sync) entry_hash: EntryHash,
    pub(in crate::log_sync) body_sha256: String,
}

pub(in crate::log_sync) type Ledger = BTreeMap<String, Submitted>;

fn ledger_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("proposals.json")
}

pub(in crate::log_sync) fn load_ledger(
    home: &Path,
    database_id: &str,
) -> Result<Ledger, ContributionError> {
    let path = ledger_path(home, database_id);
    match disk::read_limited(&path, LEDGER_LIMIT) {
        Ok(Some(bytes)) => {
            serde_json::from_slice(&bytes).map_err(|_| ContributionError::Unavailable)
        }
        Ok(None) => Ok(Ledger::new()),
        Err(_) => Err(ContributionError::Unavailable),
    }
}

pub(in crate::log_sync) fn store_ledger(
    home: &Path,
    database_id: &str,
    ledger: &Ledger,
) -> Result<(), ContributionError> {
    let bytes = serde_json::to_vec(ledger).map_err(|_| ContributionError::Unavailable)?;
    if bytes.len() > LEDGER_LIMIT {
        return Err(ContributionError::Unavailable);
    }
    disk::write_private(&ledger_path(home, database_id), &bytes)
        .map_err(|_| ContributionError::Unavailable)
}

pub(super) fn fits_ledger(
    ledger: &Ledger,
    key: &str,
    digest: &str,
) -> Result<(), ContributionError> {
    let mut probe = ledger.clone();
    probe.insert(
        key.to_owned(),
        Submitted {
            writer_seq: u64::MAX,
            entry_hash: EntryHash::ZERO,
            body_sha256: digest.to_owned(),
        },
    );
    let bytes = serde_json::to_vec(&probe).map_err(|_| ContributionError::Unavailable)?;
    if bytes.len() > LEDGER_LIMIT {
        return Err(ContributionError::Unavailable);
    }
    Ok(())
}

pub(super) fn submit_lock(home: &Path, database_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    type Key = (PathBuf, String);
    static LOCKS: OnceLock<Mutex<HashMap<Key, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let mut map = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        map.entry((home.to_path_buf(), database_id.to_owned()))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
    )
}
