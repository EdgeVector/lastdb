//! Share identity stored beside the download journal.
//!
//! A whole-database share remembers the publisher mutation and, for a standard
//! comment, the verified creator. The record store does not keep those markers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::mutation_manager::{RecordWriteLog, RecordWriteNote};
use fold_db::schema::SchemaError;
use fold_db::STANDARD_COMMENT_SCHEMA;

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

pub(crate) fn receipt_recorded(
    home: &Path,
    database_id: &str,
    id: &[u8; 32],
) -> Result<bool, String> {
    let path = receipt_path(home, database_id, id)?;
    disk::read_limited(&path, 8)
        .map(|found| found.is_some())
        .map_err(|_| "member write receipt is unavailable".to_owned())
}

pub(crate) fn store_receipt(home: &Path, database_id: &str, id: &[u8; 32]) -> Result<(), String> {
    let path = receipt_path(home, database_id, id)?;
    if receipt_recorded(home, database_id, id)? {
        return Ok(());
    }
    disk::write_private(&path, b"1").map_err(|_| "member write receipt was not stored".to_owned())
}

pub(crate) struct LocalIdentity {
    pub(crate) local_revision: String,
    pub(crate) mutation_id: String,
    pub(crate) comment_creator: Option<String>,
}

pub(crate) fn local_identity(
    home: &Path,
    database_id: &str,
    address: &str,
    revision: &str,
    comment: bool,
) -> Result<Option<LocalIdentity>, String> {
    let mutation = mutation_path(home, database_id, address).map_err(|error| error.to_string())?;
    let Some(mutation_id) = marker_at(&mutation, revision)? else {
        return Ok(None);
    };
    let comment_creator = if comment {
        let creator =
            creator_path(home, database_id, address).map_err(|error| error.to_string())?;
        marker_at(&creator, revision)?
    } else {
        None
    };
    Ok(Some(LocalIdentity {
        local_revision: revision.to_owned(),
        mutation_id,
        comment_creator,
    }))
}

fn plan(log: &IdentityLog, notes: &[RecordWriteNote]) -> Result<Vec<PlannedWrite>, SchemaError> {
    let mut mutation: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
    let mut creator: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
    for note in notes {
        mutation.insert(
            note.address.clone(),
            note.after_revision
                .as_ref()
                .map(|revision| marker(revision, &note.mutation_id))
                .transpose()?,
        );
        if note.schema_name != STANDARD_COMMENT_SCHEMA {
            continue;
        }
        let previous = match creator.get(&note.address) {
            Some(planned) => planned.clone(),
            None => read_marker(&creator_path(&log.home, &log.database_id, &note.address)?)?,
        };
        let carried = match (&note.before_revision, previous.as_deref()) {
            (Some(before), Some(bytes)) => value_at(bytes, before)?,
            _ => note.created_by.clone(),
        };
        let next = match &note.after_revision {
            Some(revision) => carried
                .map(|creator| marker(revision, &creator))
                .transpose()?,
            None => None,
        };
        if next.is_some() || previous.is_some() {
            creator.insert(note.address.clone(), next);
        }
    }
    let mut writes = Vec::new();
    for (address, body) in mutation {
        writes.push((mutation_path(&log.home, &log.database_id, &address)?, body));
    }
    for (address, body) in creator {
        writes.push((creator_path(&log.home, &log.database_id, &address)?, body));
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

fn marker_at(path: &Path, revision: &str) -> Result<Option<String>, String> {
    match read_marker(path).map_err(|error| error.to_string())? {
        Some(bytes) => value_at(&bytes, revision).map_err(|error| error.to_string()),
        None => Ok(None),
    }
}

fn value_at(bytes: &[u8], revision: &str) -> Result<Option<String>, SchemaError> {
    let marker = std::str::from_utf8(bytes)
        .map_err(|_| SchemaError::InvalidData("invalid record identity marker".into()))?;
    let (saved, value) = marker
        .split_once(':')
        .ok_or_else(|| SchemaError::InvalidData("invalid record identity marker".into()))?;
    Ok((saved == revision).then(|| value.to_owned()))
}

fn read_marker(path: &Path) -> Result<Option<Vec<u8>>, SchemaError> {
    disk::read_limited(path, MARKER_LIMIT).map_err(|_| SchemaError::StorageRead {
        detail: "record identity could not be read".into(),
    })
}

fn receipt_path(home: &Path, database_id: &str, id: &[u8; 32]) -> Result<PathBuf, String> {
    Ok(root(home, database_id)?
        .join("receipt")
        .join(fold_db::hex::hex_lower(id)))
}

fn mutation_path(home: &Path, database_id: &str, address: &str) -> Result<PathBuf, SchemaError> {
    named(home, database_id, "mutation", address)
}

fn creator_path(home: &Path, database_id: &str, address: &str) -> Result<PathBuf, SchemaError> {
    named(home, database_id, "creator", address)
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
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| "database id was refused".to_owned())?;
    if id.as_str() != database_id {
        return Err("database id was refused".into());
    }
    Ok(home
        .join("log-sync")
        .join(database_id)
        .join("share-identity"))
}
