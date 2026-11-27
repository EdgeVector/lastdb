//! Resolve the read key for an owned or received shared database.

use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use mutation_log::{DatabaseId, VerifiedHistory};
use zeroize::Zeroizing;

use crate::host::{owned, Host};
use crate::personal_seal::Sealer;
use crate::share_cache::{Lock, ShareCache, ShareCacheError};
use crate::shared_seal;

pub(super) fn open_accepted(
    histories: &super::history_store::HistoryMap,
    database_id: &str,
    entry: &mutation_log::SignedEntry,
) -> Result<(VerifiedHistory, mutation_log::SignedEntry), String> {
    let history = histories.snapshot(database_id)?;
    let hash = mutation_log::entry_hash(entry).map_err(|e| e.to_string())?;
    let opened = history
        .opened_entry(&hash)
        .ok_or("entry content did not open")?;
    Ok((history, opened))
}

pub(super) fn for_host(host: &Host, database_id: &str) -> Result<Sealer, String> {
    let seed = Zeroizing::new(host.keypair.secret_key_bytes());
    for_home(&host.home, &seed, database_id)
}

pub(super) fn history(
    home: &Path,
    node_seed: &[u8; 32],
    id: DatabaseId,
) -> Result<VerifiedHistory, String> {
    let sealer = for_home(home, node_seed, &id.to_hex())?;
    Ok(VerifiedHistory::new(id).with_opener(Arc::new(shared_seal::Opener(sealer))))
}

fn for_home(home: &Path, node_seed: &[u8; 32], database_id: &str) -> Result<Sealer, String> {
    let id = Id::try_from(database_id.to_owned()).map_err(|e| e.to_string())?;
    let key = match received_key(home, &id)? {
        Some(key) => key,
        None => owned::content_encryption_key(node_seed, database_id)?,
    };
    shared_seal::for_key(database_id, key)
}

fn received_key(home: &Path, id: &Id) -> Result<Option<Zeroizing<[u8; 32]>>, String> {
    let Some(cache) = ShareCache::open_managed(home, false).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let entry = match cache.entry(id, false) {
        Ok(entry) => entry,
        Err(ShareCacheError::Unavailable) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let _pointer = entry.lock(Lock::PointerRead).map_err(|e| e.to_string())?;
    let (registration, _) = entry.load().map_err(|e| e.to_string())?;
    registration
        .map(|registration| registration.link_key().map_err(|e| e.to_string()))
        .transpose()
}
