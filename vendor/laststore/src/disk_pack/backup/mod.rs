//! Explicit local backups of complete immutable native disk generations.
//! The bundle contains opaque stored bytes, not node credentials or cloud grants.

mod files;
mod manifest;

use super::home::DiskHome;
use super::journal::JournalTail;
use super::lifetime::{generation_inventory, DurableReference, ReferencePurpose};
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

/// Checkpoint complete pending batches, retain the selected files, then stream a
/// new bundle. Existing destinations and a live external writer are refused.
pub fn create_backup(home: &DiskHome, destination: &Path) -> Result<BackupReceipt> {
    let writer = home.journal()?;
    let current = writer.current().clone();
    let reference = DurableReference::create(home.path(), &current, ReferencePurpose::Backup, 0)?;
    let hold = reference.hold()?;
    let inventory = hold.inventory()?;
    drop(writer);
    let staging = files::staging(destination)?;
    let result = export(home.path(), &staging, &current, inventory.files);
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(crate::Error::Config(format!(
                "backup incomplete at {}; retained reference {}: {error}",
                staging.display(),
                reference.info().identity
            )))
        }
    };
    files::publish(&staging, destination)?;
    drop(hold);
    reference.release()?;
    Ok(receipt)
}

fn export(
    source: &Path,
    staging: &Path,
    current: &Current,
    inventory: Vec<super::lifetime::GenerationFile>,
) -> Result<BackupReceipt> {
    let mut entries = Vec::with_capacity(inventory.len() + 2);
    for file in inventory {
        entries.push(files::copy(source, staging, &file.path, file.bytes, None)?);
    }
    entries.push(files::write(staging, "CURRENT", &current.encode()?)?);
    entries.push(files::write(
        staging,
        "JOURNAL_TAIL",
        &snapshot_tail(current)?.encode()?,
    )?);
    snapshot_directories(staging, current)?;
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
/// write journal are not required. No existing home is overwritten.
pub fn restore_backup(backup: &Path, destination: &Path) -> Result<BackupReceipt> {
    let manifest = manifest::Manifest::read(backup)?;
    let staging = files::staging(destination)?;
    for entry in &manifest.entries {
        files::copy(
            backup,
            &staging,
            &entry.path,
            entry.bytes,
            Some(&entry.digest),
        )?;
    }
    std::fs::create_dir_all(staging.join("journal"))?;
    let home = DiskHome::open(&staging)?;
    let mut current_file = std::fs::File::open(staging.join("CURRENT"))?;
    let mut bytes = [0; 4096];
    std::io::Read::read_exact(&mut current_file, &mut bytes)?;
    let current = Current::decode(&bytes)?;
    if current.generation != manifest.generation || current.frontier != manifest.frontier {
        return Err(corrupt("backup CURRENT differs from manifest boundary"));
    }
    snapshot_directories(&staging, &current)?;
    let inventory = generation_inventory(home.path(), &current)?;
    let mut expected: BTreeMap<_, _> = inventory
        .files
        .into_iter()
        .map(|file| (file.path, file.bytes))
        .collect();
    expected.insert("CURRENT".into(), 4096);
    expected.insert("JOURNAL_TAIL".into(), 4096);
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
    JournalTail::read(&staging)?.validate_for_snapshot(&current)?;
    files::sync_tree(
        &staging,
        manifest.entries.iter().map(|entry| entry.path.as_path()),
    )?;
    drop(home);
    files::publish(&staging, destination)?;
    Ok(manifest.receipt())
}

fn snapshot_tail(current: &Current) -> Result<JournalTail> {
    if current.frontier != current.journal_frontier {
        return Err(corrupt(
            "backup requires a complete readable journal checkpoint",
        ));
    }
    Ok(JournalTail {
        stream: current.journal_stream,
        first_batch: current
            .frontier
            .checked_add(1)
            .ok_or_else(|| corrupt("backup frontier exhausted"))?,
        last_batch: current.frontier,
        encoded_bytes: 0,
        published_ms: current.published_ms,
        last_marker_digest: [0; 32],
        predecessor: None,
    })
}

fn snapshot_directories(root: &Path, current: &Current) -> Result<()> {
    let journal = root.join("journal");
    let stream = journal.join(current.journal_stream.to_string());
    std::fs::create_dir_all(&stream)?;
    crate::durability::sync_directory(&std::fs::File::open(stream)?)?;
    crate::durability::sync_directory(&std::fs::File::open(journal)?)
}

impl JournalTail {
    fn validate_for_snapshot(&self, current: &Current) -> Result<()> {
        if *self != snapshot_tail(current)? {
            return Err(corrupt(
                "backup tail differs from complete snapshot boundary",
            ));
        }
        Ok(())
    }
}
