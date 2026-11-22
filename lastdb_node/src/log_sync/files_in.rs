//! The files of records that other devices signed.
//!
//! When a pull accepts an entry whose record names a file, this device fetches
//! the sealed blob from the relay before it applies the record, and keeps the
//! blob beside the log. Applying then opens the blob, checks the file against
//! the digest and size that the record names, and puts the file into the store.
//! The fetch is eager on purpose: a restored home that needs the relay to read a
//! file is not a restored home.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use signed_history_materialize::{
    declare_history_schemas, read_state, FileRef, FileSource, NoFiles, RecordValue,
};

use super::blobs;
use super::history_store::HistoryMap;
use super::relay::RelayClient;
use super::sealed::Sealer;

/// Opens the file blobs that this node keeps for one database.
pub(super) struct SealedFiles {
    home: PathBuf,
    database_id: String,
    sealer: Arc<Sealer>,
}

impl FileSource for SealedFiles {
    fn open(&self, file: &FileRef) -> Result<Vec<u8>, String> {
        let sealed = blobs::load(&self.home, &self.database_id, &file.blob)?
            .ok_or("the blob of a file is not on this node")?;
        let plain = self
            .sealer
            .open_file(&sealed, file.sha256.as_bytes())
            .ok_or("a file blob does not open")?;
        if u64::try_from(plain.len()).ok() != Some(file.size_bytes)
            || blobs::digest(&plain) != file.sha256
        {
            return Err("a file blob differs from the file its record names".into());
        }
        Ok(plain)
    }
}

/// The source of files for `database_id`: the sealed blobs of the personal
/// database, and no files for any other database.
pub(super) fn files_for(
    histories: &HistoryMap,
    home: &Path,
    database_id: &str,
) -> Box<dyn FileSource> {
    match histories.sealer_of(database_id) {
        Some(sealer) => Box::new(SealedFiles {
            home: home.to_path_buf(),
            database_id: database_id.to_owned(),
            sealer,
        }),
        None => Box::new(NoFiles),
    }
}

/// Fetch and keep the blob of every winning file that the store does not hold
/// and this node has not kept.
pub(super) async fn fetch_missing(
    home: &Path,
    histories: &HistoryMap,
    client: &RelayClient,
    store: &FoldDB,
    database_id: &str,
) -> Result<(), String> {
    let history = histories.snapshot(database_id)?;
    let mut declared = false;
    for ((namespace, record_key), winner) in history.records() {
        if winner.value.is_empty() {
            continue;
        }
        let Some(file) = RecordValue::decode(&winner.value)?.file else {
            continue;
        };
        if !declared {
            declare_history_schemas(store, &history).await?;
            declared = true;
        }
        let held = read_state(store, namespace, record_key)
            .await?
            .and_then(|state| state.file)
            .is_some_and(|marker| marker.sha256 == file.sha256.to_hex());
        if held || blobs::load(home, database_id, &file.blob)?.is_some() {
            continue;
        }
        let sealed = client
            .blob_get(database_id, &file.blob.to_hex(), None)
            .await?;
        blobs::save(home, database_id, &file.blob, &sealed)?;
    }
    Ok(())
}
