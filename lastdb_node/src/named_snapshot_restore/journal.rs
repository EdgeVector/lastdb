//! Durable install cursor under `<home>/log-sync/<database>/`.
//!
//! A crash retries the same chain. The cursor moves only after the record
//! marker is on disk. `complete` is set only after the store flush.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use cloud_backup_protocol::Id;
use serde::{Deserialize, Serialize};

use super::chain::Link;
use super::decode;
use super::types::SnapshotInstall;
use super::RestoreError;

const JOURNAL_LIMIT: usize = 4 * 1024 * 1024;
const MARKER_LIMIT: usize = 64;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    pub version: u32,
    pub database_id: String,
    pub tip_ref: String,
    pub through_delivery_seq: u64,
    pub applied_refs: Vec<String>,
    pub next_part: u32,
    pub next_entry: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<Attempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_ref: Option<String>,
    pub complete: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attempt {
    pub part: u32,
    pub entry: u32,
    pub payload_sha256: String,
}

pub(super) enum Resume {
    Done,
    Continue { retarget: bool },
}

pub(super) enum Marker {
    Absent,
    Applied,
}

impl Journal {
    pub(super) fn fresh(database_id: &str, tip: &str) -> Self {
        Self {
            version: 1,
            database_id: database_id.to_owned(),
            tip_ref: tip.to_owned(),
            through_delivery_seq: 0,
            applied_refs: Vec::new(),
            next_part: 0,
            next_entry: 0,
            attempt: None,
            current_ref: None,
            complete: false,
        }
    }
}

pub(super) fn load(home: &Path, database_id: &str) -> Result<Option<Journal>, RestoreError> {
    let path = journal_path(home, database_id)?;
    let Some(bytes) = read_bytes(&path, JOURNAL_LIMIT, "snapshot journal")? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Durable("snapshot journal"))
}

pub(super) fn save(home: &Path, journal: &Journal) -> Result<(), RestoreError> {
    let path = journal_path(home, &journal.database_id)?;
    let bytes =
        serde_json::to_vec(journal).map_err(|_| RestoreError::Durable("snapshot journal"))?;
    if bytes.len() > JOURNAL_LIMIT {
        return Err(RestoreError::Durable("snapshot journal"));
    }
    write_private(&path, &bytes, "snapshot journal")
}

pub(super) fn plan(
    journal: &Journal,
    request: &SnapshotInstall,
    chain: &[Link],
) -> Result<Resume, RestoreError> {
    if journal.version != 1 || journal.database_id != request.database_id {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    if !is_prefix(&journal.applied_refs, chain) {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    let object = journal.applied_refs.len();
    if object == chain.len() {
        return plan_finished(journal, request, chain);
    }
    let kind = cursor_kind(journal, &chain[object])?;
    plan_open(journal, request, &kind)
}

pub(super) fn read_marker(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    hash: &str,
) -> Result<Marker, RestoreError> {
    let path = marker_path(home, database_id, mutation_id)?;
    // A body of `1` is E's existing applied marker and counts as this payload.
    match read_bytes(&path, MARKER_LIMIT, "applied marker")? {
        None => Ok(Marker::Absent),
        Some(bytes) if bytes.as_slice() == b"1" || bytes == hash.as_bytes() => Ok(Marker::Applied),
        Some(_) => Err(RestoreError::Conflict("applied marker")),
    }
}

pub(super) fn write_marker(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    hash: &str,
) -> Result<(), RestoreError> {
    let path = marker_path(home, database_id, mutation_id)?;
    write_private(&path, hash.as_bytes(), "applied marker")
}

fn plan_finished(
    journal: &Journal,
    request: &SnapshotInstall,
    chain: &[Link],
) -> Result<Resume, RestoreError> {
    if !clean_tail(journal) {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    let through = chain
        .last()
        .map(|link| link.manifest.through_delivery_seq)
        .ok_or(RestoreError::Chain("snapshot chain"))?;
    if journal.complete
        && journal.tip_ref == request.snapshot_ref
        && journal.through_delivery_seq == through
    {
        return Ok(Resume::Done);
    }
    if journal.complete {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    Ok(Resume::Continue {
        retarget: journal.tip_ref != request.snapshot_ref,
    })
}

fn plan_open(
    journal: &Journal,
    request: &SnapshotInstall,
    kind: &Kind,
) -> Result<Resume, RestoreError> {
    match kind {
        Kind::Boundary => Ok(Resume::Continue {
            retarget: journal.complete || journal.tip_ref != request.snapshot_ref,
        }),
        Kind::Inside if !journal.complete && journal.tip_ref == request.snapshot_ref => {
            Ok(Resume::Continue { retarget: false })
        }
        Kind::Inside => Err(RestoreError::Conflict("snapshot journal")),
    }
}

fn cursor_kind(journal: &Journal, link: &Link) -> Result<Kind, RestoreError> {
    if at_start(journal) {
        return boundary_ref(journal, link);
    }
    inside(journal, link)
}

fn at_start(journal: &Journal) -> bool {
    journal.next_part == 0 && journal.next_entry == 0 && journal.attempt.is_none()
}

fn boundary_ref(journal: &Journal, link: &Link) -> Result<Kind, RestoreError> {
    match journal.current_ref.as_deref() {
        None => Ok(Kind::Boundary),
        Some(refer) if refer == link.manifest.snapshot_ref => Ok(Kind::Boundary),
        Some(_) => Err(RestoreError::Conflict("snapshot journal")),
    }
}

fn inside(journal: &Journal, link: &Link) -> Result<Kind, RestoreError> {
    if journal.current_ref.as_deref() != Some(link.manifest.snapshot_ref.as_str()) {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    let parts = link.parts.len();
    let part = journal.next_part as usize;
    if part == parts && journal.next_entry == 0 && journal.attempt.is_none() {
        return Ok(Kind::Inside);
    }
    if part >= parts {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    let entries = link.parts[part].entries.len();
    let entry = journal.next_entry as usize;
    if entries == 0 {
        return empty_part(journal, entry);
    }
    if entry >= entries {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    if let Some(attempt) = &journal.attempt {
        if attempt.part != journal.next_part || attempt.entry != journal.next_entry {
            return Err(RestoreError::Conflict("snapshot journal"));
        }
    }
    Ok(Kind::Inside)
}

fn empty_part(journal: &Journal, entry: usize) -> Result<Kind, RestoreError> {
    if entry == 0 && journal.attempt.is_none() {
        Ok(Kind::Inside)
    } else {
        Err(RestoreError::Conflict("snapshot journal"))
    }
}

fn clean_tail(journal: &Journal) -> bool {
    journal.next_part == 0
        && journal.next_entry == 0
        && journal.attempt.is_none()
        && journal.current_ref.is_none()
}

fn is_prefix(applied: &[String], chain: &[Link]) -> bool {
    applied.len() <= chain.len()
        && applied
            .iter()
            .zip(chain)
            .all(|(refer, link)| refer == &link.manifest.snapshot_ref)
}

enum Kind {
    Boundary,
    Inside,
}

fn journal_path(home: &Path, database_id: &str) -> Result<PathBuf, RestoreError> {
    Ok(database_dir(home, database_id)?.join("snapshot-restore.json"))
}

fn marker_path(home: &Path, database_id: &str, mutation_id: &str) -> Result<PathBuf, RestoreError> {
    if !decode::mutation_label(mutation_id) {
        return Err(RestoreError::Entry("mutation id"));
    }
    Ok(database_dir(home, database_id)?
        .join("applied")
        .join(mutation_id))
}

fn database_dir(home: &Path, database_id: &str) -> Result<PathBuf, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    if id.as_str() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    Ok(home.join("log-sync").join(database_id))
}

fn read_bytes(
    path: &Path,
    limit: usize,
    detail: &'static str,
) -> Result<Option<Vec<u8>>, RestoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RestoreError::Durable(detail)),
    };
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.len() > limit as u64
    {
        return Err(class_for(detail));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| RestoreError::Durable(detail))?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RestoreError::Durable(detail))?;
    if bytes.len() > limit {
        return Err(class_for(detail));
    }
    Ok(Some(bytes))
}

fn class_for(detail: &'static str) -> RestoreError {
    if detail == "applied marker" {
        RestoreError::Conflict(detail)
    } else {
        RestoreError::Durable(detail)
    }
}

fn write_private(path: &Path, bytes: &[u8], detail: &'static str) -> Result<(), RestoreError> {
    let parent = path.parent().ok_or(RestoreError::Durable(detail))?;
    fs::create_dir_all(parent).map_err(|_| RestoreError::Durable(detail))?;
    let temporary = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)
            .map_err(|_| RestoreError::Durable(detail))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| RestoreError::Durable(detail))?;
        file.write_all(bytes)
            .map_err(|_| RestoreError::Durable(detail))?;
        file.sync_all().map_err(|_| RestoreError::Durable(detail))?;
    }
    fs::rename(&temporary, path).map_err(|_| RestoreError::Durable(detail))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RestoreError::Durable(detail))?;
    Ok(())
}
