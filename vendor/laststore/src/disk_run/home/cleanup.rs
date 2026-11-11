//! Explicit base compaction, authenticated cleanup checkpoints, and obsolete-file removal.

use super::commit::MAX_MERGE_INPUTS;
use super::manifest::Manifest;
use super::{RunHome, CAS_DELETE as CAS_NAME, NODE_CLEANUP as NAME};
use crate::disk_run::RUN_SUFFIX;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame_bounded, encode_frame, FrameHeader};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"LDRCLN1\0";
const MAX_BYTES: usize = 16 * 1024;
const CAS_MAGIC: &[u8; 8] = b"LDRCAS1\0";
/// Maximum encrypted cross-home deletion intent payload.
pub const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 1_000_000;

/// Result of one exact active run-directory cleanup.
#[derive(Debug, Clone, Copy)]
pub struct RunCleanupFiles {
    /// Replaced run bytes whose old roots still hold their files.
    pub held_bytes: u64,
    /// Obsolete run or temporary files removed on this call.
    pub removed_files: u64,
}

impl RunHome {
    /// Explicit cleanup uses the same replacement admission as automatic merges.
    pub fn cleanup_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.cleanup_step_inner(policy, false)
    }

    /// Whether the current files contain no known or uncounted hidden versions.
    pub fn snapshot_has_clean_runs(&self) -> bool {
        clean_runs(&self.held())
    }

    /// Remove hidden versions below the ordinary merge thresholds before a new cloud snapshot.
    pub fn snapshot_cleanup_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.cleanup_step_inner(policy, true)
    }

    fn cleanup_step_inner(&self, policy: &SpacePolicy, snapshot: bool) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        if snapshot {
            self.count_hidden(&held)?;
            if clean_runs(&held) {
                return Ok(false);
            }
        }
        let start = if held.merging.is_empty() {
            let tiered: Vec<_> = held.tiered().cloned().collect();
            if tiered.is_empty() {
                return Ok(false);
            }
            let first = tiered.len().saturating_sub(MAX_MERGE_INPUTS);
            Some(tiered[first..].to_vec())
        } else {
            None
        };
        self.base_step(start, policy).map(|()| true)
    }

    /// Read the node's private cleanup checkpoint.
    pub fn load_cleanup_checkpoint(&self) -> Result<Option<Vec<u8>>> {
        self.load_checkpoint(NAME, MAGIC, MAX_BYTES)
    }

    /// Read the authenticated cross-home deletion intent, if one remains.
    pub fn load_cas_delete_checkpoint(&self) -> Result<Option<Vec<u8>>> {
        self.load_checkpoint(CAS_NAME, CAS_MAGIC, MAX_CAS_DELETE_CHECKPOINT_BYTES)
    }

    fn load_checkpoint(
        &self,
        name: &str,
        magic: &[u8; 8],
        maximum: usize,
    ) -> Result<Option<Vec<u8>>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(self.root.join(name))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.mode() & 0o077 != 0
            || meta.nlink() != 1
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.len() > (maximum + 4096) as u64
        {
            return Err(invalid("invalid cleanup checkpoint file"));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((maximum + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let frame = decode_frame_bounded(&self.key, &bytes, maximum + 24)?;
        if frame.payload.get(..8) != Some(magic.as_slice())
            || frame.payload.get(8..24) != Some(self.identity.as_bytes().as_slice())
        {
            return Err(invalid(
                "cleanup checkpoint belongs to another role or home",
            ));
        }
        Ok(Some(frame.payload[24..].to_vec()))
    }

    /// Durably replace the bounded node-cleanup checkpoint.
    pub fn save_cleanup_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        self.save_checkpoint(NAME, MAGIC, MAX_BYTES, bytes)
    }

    /// Save intent before either home changes. No body or encryption key enters it.
    pub fn save_cas_delete_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        self.save_checkpoint(CAS_NAME, CAS_MAGIC, MAX_CAS_DELETE_CHECKPOINT_BYTES, bytes)
    }

    /// Remove only the authenticated intent after its exact frontier completes.
    pub fn clear_cas_delete_checkpoint(&self) -> Result<()> {
        if self.load_cas_delete_checkpoint()?.is_some() {
            fs::remove_file(self.root.join(CAS_NAME))?;
            sync_directory(&File::open(&self.root)?)?;
        }
        Ok(())
    }

    fn save_checkpoint(
        &self,
        name: &str,
        magic: &[u8; 8],
        maximum: usize,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.len() > maximum {
            return Err(invalid("cleanup checkpoint exceeds its byte bound"));
        }
        let _publish = self
            .publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.refuse_if_poisoned()?;
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let mut payload = magic.to_vec();
        payload.extend_from_slice(self.identity.as_bytes());
        payload.extend_from_slice(bytes);
        let frame = encode_frame(&self.key, header, &payload)?;
        let pending = self.root.join(format!(".{name}.{}.tmp", header.chunk_uuid));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&pending)?;
            file.write_all(&frame)?;
            sync_dirty_file(&file)?;
            fs::rename(&pending, self.root.join(name))?;
            sync_directory(&File::open(&self.root)?)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&pending);
        }
        result
    }

    /// Remove unnamed files only after all active writers/mergers finish.
    pub fn cleanup_files(&self) -> Result<RunCleanupFiles> {
        let _merge = self
            .merge
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _commit = self.commit_lock()?;
        let _publish = self.publish_lock()?;
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

fn clean_runs(held: &Manifest) -> bool {
    held.merging.is_empty() && held.tiered().all(|run| run.hidden() == Some(0))
}

fn invalid(message: &str) -> Error {
    Error::Corrupt(message.into())
}
