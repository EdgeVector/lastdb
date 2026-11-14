//! Replay one received log page into the registered receiver store.
//!
//! The page uses the same envelope decode and record apply as a snapshot.
//! Its attempt file is `page-attempt.json`, separate from the snapshot
//! journal. The store flushes before this returns. The caller then acks.

use std::path::{Path, PathBuf};

use fold_db::fold_db_core::FoldDB;
use serde::{Deserialize, Serialize};

use crate::log_envelope::DatabaseContentKey;
use crate::Host;

use super::apply;
use super::decode;
use super::journal;
use super::payload;
use super::types::{SnapshotEntry, SnapshotFileSource};
use super::RestoreError;

const ATTEMPT_LIMIT: usize = 4096;
const DETAIL: &str = "log page";

/// One live log entry, already accepted, ready for the snapshot decoder.
pub(crate) struct DeliveredEntry {
    pub(crate) delivery_seq: u64,
    pub(crate) database_id: String,
    pub(crate) entry: SnapshotEntry,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageAttempt {
    version: u32,
    database_id: String,
    delivery_seq: u64,
    payload_sha256: String,
}

pub(super) async fn replay(
    host: &Host,
    database_id: &str,
    publisher: &str,
    delivered: &[DeliveredEntry],
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let target = super::resolve_target(host, database_id)?;
    match &target.place {
        super::Place::Received => authorize(host, database_id, publisher)?,
        super::Place::Owned(_)
            if target.private && publisher == hex::encode(host.keypair.public_key_bytes()) => {}
        super::Place::Owned(_) => return Err(RestoreError::Scope("database store")),
    }
    check_page(database_id, publisher, delivered)?;
    let store = super::open_store(host, database_id, &target).await?;
    let decoder = target.decoder(database_id);
    let replay = async {
        for item in delivered {
            apply_item(&store, decoder, &host.home, item, files).await?;
        }
        store
            .flush()
            .await
            .map_err(|_| RestoreError::Durable("database flush"))
    };
    store
        .db_ops()
        .native_runtime()
        .replay_accepted(replay)
        .await
}

fn authorize(host: &Host, database_id: &str, publisher: &str) -> Result<(), RestoreError> {
    let share = crate::registered_share::resolve(&host.home, database_id)
        .map_err(|_| RestoreError::Scope("database registry"))?
        .ok_or(RestoreError::Scope("named database"))?;
    if publisher.is_empty()
        || !share.signed_log
        || share.id.as_str() != database_id
        || share.publisher.as_str() != publisher
    {
        return Err(RestoreError::Scope("database store"));
    }
    Ok(())
}

pub(super) fn check_page(
    database_id: &str,
    publisher: &str,
    delivered: &[DeliveredEntry],
) -> Result<(), RestoreError> {
    let mut previous = 0u64;
    for item in delivered {
        if item.delivery_seq <= previous || item.database_id != database_id {
            return Err(RestoreError::Entry("snapshot entry"));
        }
        if item.entry.writer_key != publisher {
            return Err(RestoreError::Scope("database store"));
        }
        previous = item.delivery_seq;
    }
    Ok(())
}

async fn apply_item(
    store: &FoldDB,
    decoder: decode::EntryDecoder<'_>,
    home: &Path,
    item: &DeliveredEntry,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let database_id = decoder.database_id;
    let prepared = decoder.open(&item.entry)?;
    if let journal::Marker::Applied =
        journal::read_marker(home, database_id, &prepared.id, &prepared.hash)?
    {
        return release_applied(home, database_id, item.delivery_seq, &prepared.hash);
    }
    let resumed = begin_attempt(home, database_id, item.delivery_seq, &prepared.hash)?;
    if apply_change(
        store,
        decoder.key,
        database_id,
        &item.entry,
        files,
        prepared.change,
        resumed,
    )
    .await?
    {
        clear_attempt(home, database_id)?;
        return Err(RestoreError::Conflict(DETAIL));
    }
    journal::write_marker(home, database_id, &prepared.id, &prepared.hash)?;
    clear_attempt(home, database_id)
}

async fn apply_change(
    store: &FoldDB,
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    files: &mut (dyn SnapshotFileSource + Send),
    change: payload::Change,
    resumed: bool,
) -> Result<bool, RestoreError> {
    match change {
        payload::Change::Schema(change) => {
            apply::apply_schema(store, *change).await?;
            Ok(false)
        }
        payload::Change::Record(mut change) => {
            apply::stage_file(store, key, database_id, entry, change.as_mut(), files).await?;
            if apply::bind_expected(store, &mut change.mutation, resumed).await? {
                return first_conflict(apply::write_record(store, change.mutation, resumed).await);
            }
            Ok(false)
        }
    }
}

fn first_conflict(result: Result<(), RestoreError>) -> Result<bool, RestoreError> {
    match result {
        Ok(()) => Ok(false),
        Err(RestoreError::Conflict(_)) => Ok(true),
        Err(error) => Err(error),
    }
}

fn release_applied(
    home: &Path,
    database_id: &str,
    delivery_seq: u64,
    hash: &str,
) -> Result<(), RestoreError> {
    let Some(attempt) = load_attempt(home, database_id)? else {
        return Ok(());
    };
    if attempt.delivery_seq == delivery_seq && attempt.payload_sha256 == hash {
        return clear_attempt(home, database_id);
    }
    if attempt.delivery_seq == delivery_seq {
        return Err(RestoreError::Conflict(DETAIL));
    }
    Ok(())
}

fn begin_attempt(
    home: &Path,
    database_id: &str,
    delivery_seq: u64,
    hash: &str,
) -> Result<bool, RestoreError> {
    let Some(attempt) = load_attempt(home, database_id)? else {
        save_attempt(home, database_id, delivery_seq, hash)?;
        return Ok(false);
    };
    if attempt.delivery_seq == delivery_seq && attempt.payload_sha256 == hash {
        return Ok(true);
    }
    Err(RestoreError::Conflict(DETAIL))
}

fn load_attempt(home: &Path, database_id: &str) -> Result<Option<PageAttempt>, RestoreError> {
    let path = attempt_path(home, database_id)?;
    let Some(bytes) = journal::read_bytes(&path, ATTEMPT_LIMIT, DETAIL)? else {
        return Ok(None);
    };
    let attempt: PageAttempt =
        serde_json::from_slice(&bytes).map_err(|_| RestoreError::Durable(DETAIL))?;
    if attempt.version == 1 && attempt.database_id == database_id {
        Ok(Some(attempt))
    } else {
        Err(RestoreError::Conflict(DETAIL))
    }
}

fn save_attempt(
    home: &Path,
    database_id: &str,
    delivery_seq: u64,
    hash: &str,
) -> Result<(), RestoreError> {
    let attempt = PageAttempt {
        version: 1,
        database_id: database_id.to_owned(),
        delivery_seq,
        payload_sha256: hash.to_owned(),
    };
    let bytes = serde_json::to_vec(&attempt).map_err(|_| RestoreError::Durable(DETAIL))?;
    if bytes.len() > ATTEMPT_LIMIT {
        return Err(RestoreError::Durable(DETAIL));
    }
    journal::write_private(&attempt_path(home, database_id)?, &bytes, DETAIL)
}

fn clear_attempt(home: &Path, database_id: &str) -> Result<(), RestoreError> {
    journal::remove_private(&attempt_path(home, database_id)?, DETAIL)
}

fn attempt_path(home: &Path, database_id: &str) -> Result<PathBuf, RestoreError> {
    Ok(journal::database_dir(home, database_id)?.join("page-attempt.json"))
}
