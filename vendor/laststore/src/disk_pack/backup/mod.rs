//! Explicit local backups of complete immutable native disk generations.
//! The bundle contains opaque stored bytes, not node credentials or cloud grants.

pub mod export;
mod files;
mod manifest;
mod prepare;
mod validate;

pub use prepare::{prepare_referenced_backup, PreparedBackup, ReservedBackup};
pub use validate::{prepare_snapshot_directories, validate_snapshot, ValidatedSnapshot};

use super::home::DiskHome;
use super::lifetime::{DurableReference, ReferencePurpose};
use super::{corrupt, Current};
use crate::Result;
use std::collections::BTreeMap;
use std::path::Path;

/// Dated immutable boundary included by a completed local backup or restore.
#[derive(Clone, Debug)]
pub struct BackupReceipt {
    /// Complete immutable root selected for the bundle.
    pub generation: u64,
    /// Last complete mutation batch in that root.
    pub frontier: u64,
    /// Exact dependency and snapshot metadata file count.
    pub files: u64,
    /// Total bytes in those files, excluding the manifest.
    pub bytes: u64,
}

/// Retain a complete selected snapshot, then stream a
/// new bundle. Existing destinations and a live external writer are refused.
pub fn create_backup(home: &DiskHome, destination: &Path) -> Result<BackupReceipt> {
    let writer = home.snapshot()?;
    let current = writer.current().clone();
    let reference = DurableReference::create(home.path(), &current, ReferencePurpose::Backup, 0)?;
    let prepared = prepare_referenced_backup(home, reference, destination)?;
    let reserved = prepared.reserve(&writer, &super::maintenance::SpacePolicy::default())?;
    drop(writer);
    reserved.create()
}

pub(super) fn export(
    source: &Path,
    staging: &Path,
    current: &Current,
    inventory: Vec<super::lifetime::GenerationFile>,
) -> Result<BackupReceipt> {
    let mut entries = Vec::with_capacity(inventory.len() + 1);
    for file in inventory {
        entries.push(files::copy(source, staging, &file.path, file.bytes, None)?);
    }
    entries.push(files::write(staging, "CURRENT", &current.encode()?)?);
    snapshot_directories(staging)?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest = manifest::Manifest {
        generation: current.generation,
        frontier: current.frontier,
        entries,
    };
    manifest.write(staging)?;
    files::sync_tree(
        staging,
        manifest.entries.iter().map(|entry| entry.path.as_path()),
    )?;
    Ok(manifest.receipt())
}

/// Restore a complete verified bundle into a new root. The source home and its
/// mutable writer state are not required. No existing home is overwritten.
pub fn restore_backup(backup: &Path, destination: &Path) -> Result<BackupReceipt> {
    let manifest = manifest::Manifest::read(backup)?;
    let destination = files::destination(destination)?;
    let backup = std::fs::canonicalize(backup)?;
    if destination.starts_with(&backup) {
        return Err(corrupt("restore destination is inside its backup"));
    }
    files::reserve_output(
        &destination,
        manifest.output_bytes()?,
        super::maintenance::SpacePolicy::default().free_space_reserve_bytes,
    )?;
    let staging = files::staging(&destination)?;
    for entry in &manifest.entries {
        files::copy(
            &backup,
            &staging,
            &entry.path,
            entry.bytes,
            Some(&entry.digest),
        )?;
    }
    let validated = validate_snapshot(&staging)?;
    let current = &validated.current;
    if current.generation != manifest.generation || current.frontier != manifest.frontier {
        return Err(corrupt("backup CURRENT differs from manifest boundary"));
    }
    snapshot_directories(&staging)?;
    let expected: BTreeMap<_, _> = validated
        .files
        .into_iter()
        .map(|file| (file.path, file.bytes))
        .collect();
    let actual: BTreeMap<_, _> = manifest
        .entries
        .iter()
        .map(|entry| (entry.path.clone(), entry.bytes))
        .collect();
    if expected != actual {
        return Err(corrupt(
            "backup files differ from exact generation inventory",
        ));
    }
    files::sync_tree(
        &staging,
        manifest.entries.iter().map(|entry| entry.path.as_path()),
    )?;
    files::publish(&staging, &destination)?;
    Ok(manifest.receipt())
}

fn snapshot_directories(root: &Path) -> Result<()> {
    for name in ["directory", "locator", "packs"] {
        let path = root.join(name);
        std::fs::create_dir_all(&path)?;
        crate::durability::sync_directory(&std::fs::File::open(path)?)?;
    }
    Ok(())
}
