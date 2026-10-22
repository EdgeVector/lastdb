//! Reuse the native decoder before a cloud importer exposes its owned output.
use super::{files, snapshot_directories};
use crate::disk_pack::home::DiskHome;
use crate::disk_pack::journal::JournalTail;
use crate::disk_pack::lifetime::{generation_inventory, GenerationFile};
use crate::disk_pack::Current;
use crate::Result;
use std::io::Read;
use std::path::Path;
use uuid::Uuid;

/// The exact native inventory, including generated snapshot boundary files.
/// This is a format/inventory proof, not an account-key or file-blob proof.
pub struct ValidatedSnapshot {
    /// Exact decoded CURRENT selected by this validation.
    pub current: Current,
    /// Immutable native home identity.
    pub home_id: Uuid,
    /// Exact native dependencies plus CURRENT and the empty JOURNAL_TAIL.
    pub files: Vec<GenerationFile>,
}

/// Validate an owned, inactive complete root without copying or publishing it.
/// The caller verifies every expected file digest and excludes concurrent writers.
pub fn validate_snapshot(root: &Path) -> Result<ValidatedSnapshot> {
    let mut input = files::open(root, Path::new("CURRENT"))?;
    if input.metadata()?.len() != 4096 {
        return Err(crate::disk_pack::corrupt("snapshot CURRENT length"));
    }
    let mut bytes = [0; 4096];
    input.read_exact(&mut bytes)?;
    let current = Current::decode(&bytes)?;
    let home = DiskHome::open(root)?;
    JournalTail::read(root)?.validate_for_snapshot(&current)?;
    let mut inventory = generation_inventory(root, &current)?.files;
    for name in ["CURRENT", "JOURNAL_TAIL"] {
        inventory.push(GenerationFile {
            path: name.into(),
            bytes: 4096,
            creation_generation: current.generation,
        });
    }
    Ok(ValidatedSnapshot {
        current,
        home_id: home.descriptor().identity,
        files: inventory,
    })
}

/// Create only the native empty writer directories after validation.
/// The caller owns this inactive root and retains the exact validated CURRENT.
pub fn prepare_snapshot_directories(root: &Path, current: &Current) -> Result<()> {
    current.encode()?;
    snapshot_directories(root, current)
}
