//! Share identity stored beside the download journal.
//!
//! A whole-database share remembers which mutation wrote a record revision.
//! The creator of a record is `created_by` on the record itself.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::fold_db_core::mutation_manager::{RecordWriteLog, RecordWriteNote};
use fold_db::schema::SchemaError;
use mutation_log::DatabaseId;

use crate::profile::private_fs as disk;

const MARKER_LIMIT: usize = 512;

type PlannedWrite = (PathBuf, Option<Vec<u8>>);

pub(crate) struct IdentityLog {
    home: PathBuf,
    database_id: String,
}

pub(crate) fn record_log(home: &Path, database_id: &str) -> Arc<dyn RecordWriteLog> {
    Arc::new(IdentityLog {
        home: home.to_path_buf(),
        database_id: database_id.to_owned(),
    })
}

impl RecordWriteLog for IdentityLog {
    fn prepare(
        &self,
        notes: &[RecordWriteNote],
    ) -> Result<Box<dyn FnOnce() -> Result<(), String> + Send>, SchemaError> {
        let writes = plan(self, notes)?;
        Ok(Box::new(move || apply_writes(&writes)))
    }
}

fn plan(log: &IdentityLog, notes: &[RecordWriteNote]) -> Result<Vec<PlannedWrite>, SchemaError> {
    let mut mutation: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
    for note in notes {
        mutation.insert(
            note.address.clone(),
            note.after_revision
                .as_ref()
                .map(|revision| marker(revision, &note.mutation_id))
                .transpose()?,
        );
    }
    let mut writes = Vec::new();
    for (address, body) in mutation {
        writes.push((mutation_path(&log.home, &log.database_id, &address)?, body));
    }
    Ok(writes)
}

fn apply_writes(writes: &[PlannedWrite]) -> Result<(), String> {
    for (path, body) in writes {
        match body {
            Some(bytes) => disk::write_private(path, bytes)
                .map_err(|_| "record identity was not stored".to_owned())?,
            None => disk::remove_private(path)
                .map_err(|_| "record identity was not stored".to_owned())?,
        }
    }
    Ok(())
}

fn marker(revision: &str, value: &str) -> Result<Vec<u8>, SchemaError> {
    let text = format!("{revision}:{value}");
    if text.len() > MARKER_LIMIT || revision.is_empty() || value.is_empty() {
        return Err(SchemaError::InvalidData(
            "invalid record identity marker".into(),
        ));
    }
    Ok(text.into_bytes())
}

fn mutation_path(home: &Path, database_id: &str, address: &str) -> Result<PathBuf, SchemaError> {
    named(home, database_id, "mutation", address)
}

fn named(
    home: &Path,
    database_id: &str,
    kind: &str,
    address: &str,
) -> Result<PathBuf, SchemaError> {
    let root = root(home, database_id).map_err(SchemaError::InvalidData)?;
    Ok(root
        .join(kind)
        .join(fold_db::hex::sha256_hex(address.as_bytes())))
}

fn root(home: &Path, database_id: &str) -> Result<PathBuf, String> {
    // Owned database ids are Hex32 (db verifying key), not cloud UUID Ids.
    let id = DatabaseId::from_hex(database_id).map_err(|_| "database id was refused".to_owned())?;
    if id.to_hex() != database_id {
        return Err("database id was refused".into());
    }
    Ok(home
        .join("log-sync")
        .join(database_id)
        .join("share-identity"))
}
