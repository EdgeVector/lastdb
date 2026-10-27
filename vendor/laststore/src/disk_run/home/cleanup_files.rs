//! Retry removal of obsolete active-store files without a second owner.

use super::{RunHome, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use uuid::Uuid;

/// Result of one exact active run-directory cleanup. Backup hardlinks are
/// independent names and are never inspected or removed by this operation.
#[derive(Debug, Clone, Copy)]
pub struct RunCleanupFiles {
    /// Replaced run bytes whose old roots still hold their files.
    pub held_bytes: u64,
    /// Obsolete run or temporary files removed on this call.
    pub removed_files: u64,
}

// CURRENT permits at most 32*4096 tiered, 4096 merging and 4096 base runs.
// Leave room for reader-held generations; refuse excess work, never certify
// an unexamined suffix. This is a bounded physical directory, not a data query.
const MAX_ENTRIES: usize = 1_000_000;

impl RunHome {
    /// Remove unnamed files only after all active writers/mergers finish.
    /// Held old roots protect their files. Every unlink error is returned,
    /// including errors that an earlier last-holder drop could not report.
    pub fn cleanup_files(&self) -> Result<RunCleanupFiles> {
        let _merge = self
            .merge
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _commit = self
            .commit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _publish = self
            .publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.refuse_if_poisoned()?;
        let current = self.held();
        let held: Vec<_> = {
            let mut retired = self
                .retired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            retired.retain(|run| run.strong_count() > 0);
            if retired.len() > MAX_ENTRIES {
                return Err(Error::WriteCapacity(
                    "cleanup held-file bound exceeded".into(),
                ));
            }
            retired
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect()
        };
        let protected: HashSet<_> = current
            .newest_first()
            .chain(held.iter())
            .map(|run| run.meta.run_id)
            .collect();
        let mut removed_files = 0;
        for (index, entry) in fs::read_dir(&self.runs)?.enumerate() {
            if index >= MAX_ENTRIES {
                return Err(Error::WriteCapacity(
                    "cleanup run-directory entry bound exceeded".into(),
                ));
            }
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let obsolete = name
                .strip_suffix(RUN_SUFFIX)
                .and_then(|id| Uuid::parse_str(id).ok())
                .is_some_and(|id| !protected.contains(&id));
            if obsolete || (name.starts_with('.') && name.ends_with(".tmp")) {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed_files += 1,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        // This also orders unlinks that last-holder drops performed earlier.
        sync_directory(&File::open(&self.runs)?)?;
        for run in current.newest_first() {
            let meta = fs::symlink_metadata(&run.path)?;
            if !meta.is_file() || meta.len() != run.meta.file_bytes {
                return Err(Error::Corrupt(
                    "cleanup found an invalid current run file".into(),
                ));
            }
        }
        Ok(RunCleanupFiles {
            held_bytes: held.iter().map(|run| run.meta.file_bytes).sum(),
            removed_files,
        })
    }
}
