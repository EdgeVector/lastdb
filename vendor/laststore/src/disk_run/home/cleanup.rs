//! Explicit base compaction, hidden-version counting, cleanup checkpoints, and file removal.

use super::commit::MAX_MERGE_INPUTS;
use super::manifest::{Manifest, RunFile};
use super::{is_removable_run, write_private_file, RunHome, CAS_DELETE, NODE_CLEANUP};
use crate::disk_run::RunReader;
use crate::durability::sync_directory;
use crate::frame::{decode_frame_bounded, encode_frame, FrameHeader};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::Arc;
use uuid::Uuid;

/// Maximum encrypted cross-home deletion intent payload.
pub const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 1_000_000;
const COUNT_KEYS: usize = 4096;

/// Private authenticated checkpoint slot inside a run home.
#[derive(Debug, Clone, Copy)]
pub enum CheckpointKind {
    /// Bounded node-cleanup progress receipt.
    NodeCleanup,
    /// Cross-home CAS deletion intent.
    CasDelete,
}

impl CheckpointKind {
    fn spec(self) -> (&'static str, &'static [u8; 8], usize) {
        match self {
            Self::NodeCleanup => (NODE_CLEANUP, b"LDRCLN1\0", 16 * 1024),
            Self::CasDelete => (CAS_DELETE, b"LDRCAS1\0", MAX_CAS_DELETE_CHECKPOINT_BYTES),
        }
    }
}

/// Result of one exact active run-directory cleanup.
#[derive(Debug, Clone, Copy)]
pub struct RunCleanupFiles {
    /// Replaced run bytes whose old roots still hold their files.
    pub held_bytes: u64,
    /// Obsolete run or temporary files removed on this call.
    pub removed_files: u64,
}

impl RunHome {
    pub(super) fn count_hidden(&self, held: &Manifest) -> Result<()> {
        let runs: Vec<&Arc<RunFile>> = held.newest_first().collect();
        for (at, run) in runs.iter().enumerate().take(held.tiered().count()) {
            if run.hidden().is_none() {
                run.set_hidden(self.hidden_bytes(run, &runs[at + 1..])?);
            }
        }
        Ok(())
    }

    fn hidden_bytes(&self, run: &RunFile, older: &[&Arc<RunFile>]) -> Result<u64> {
        let older: Vec<&Arc<RunFile>> = older
            .iter()
            .copied()
            .filter(|o| o.meta.meets(&run.meta))
            .collect();
        if older.is_empty() {
            return Ok(0);
        }
        let reader = RunReader::open(&run.path, &self.key)?;
        let mut readers: Vec<Option<RunReader>> = (0..older.len()).map(|_| None).collect();
        let (mut records, mut hidden) = (reader.iter(), 0u64);
        loop {
            let keys: Vec<Vec<u8>> = records
                .by_ref()
                .take(COUNT_KEYS)
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_>>()?;
            let Some(first) = keys.first() else {
                return Ok(hidden);
            };
            for (slot, other) in older.iter().enumerate() {
                if other.meta.last_key < *first {
                    readers[slot] = None;
                }
            }
            hidden = hidden.saturating_add(self.older_bytes(&keys, &older, &mut readers)?);
        }
    }

    fn older_bytes(
        &self,
        keys: &[Vec<u8>],
        older: &[&Arc<RunFile>],
        readers: &mut [Option<RunReader>],
    ) -> Result<u64> {
        let (mut open, mut bytes) = ((0..keys.len()).collect::<Vec<_>>(), 0u64);
        for (slot, run) in older.iter().enumerate() {
            let lookups: Vec<&[u8]> = open
                .iter()
                .filter_map(|&at| run.meta.covers(&keys[at]).then_some(keys[at].as_slice()))
                .collect();
            if !lookups.is_empty() {
                let reader = match &mut readers[slot] {
                    Some(r) => r,
                    slot_ref => slot_ref.insert(RunReader::open(&run.path, &self.key)?),
                };
                let mut hits = reader.record_bytes_many(&lookups)?.into_iter();
                open.retain(|&at| {
                    if !run.meta.covers(&keys[at]) {
                        return true;
                    }
                    match hits.next().expect("aligned") {
                        Some(size) => {
                            bytes = bytes.saturating_add(size);
                            false
                        }
                        None => true,
                    }
                });
            }
            if keys.last().is_some_and(|last| run.meta.last_key <= *last) {
                readers[slot] = None;
            }
            if open.is_empty() {
                break;
            }
        }
        Ok(bytes)
    }

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
            Some(tiered[tiered.len().saturating_sub(MAX_MERGE_INPUTS)..].to_vec())
        } else {
            None
        };
        self.base_step(start, policy).map(|()| true)
    }

    /// Read one authenticated private checkpoint, if present.
    pub fn load_checkpoint(&self, kind: CheckpointKind) -> Result<Option<Vec<u8>>> {
        let (name, magic, maximum) = kind.spec();
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(self.root.join(name))
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.mode() & 0o077 != 0
            || meta.nlink() != 1
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.len() > (maximum + 4096) as u64
        {
            return Err(Error::Corrupt("invalid cleanup checkpoint file".into()));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((maximum + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let frame = decode_frame_bounded(&self.key, &bytes, maximum + 24)?;
        if frame.payload.get(..8) != Some(magic.as_slice())
            || frame.payload.get(8..24) != Some(self.identity.as_bytes().as_slice())
        {
            return Err(Error::Corrupt(
                "cleanup checkpoint belongs to another role or home".into(),
            ));
        }
        Ok(Some(frame.payload[24..].to_vec()))
    }

    /// Durably replace one authenticated private checkpoint.
    pub fn save_checkpoint(&self, kind: CheckpointKind, bytes: &[u8]) -> Result<()> {
        let (name, magic, maximum) = kind.spec();
        if bytes.len() > maximum {
            return Err(Error::Corrupt(
                "cleanup checkpoint exceeds its byte bound".into(),
            ));
        }
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let chunk_uuid = Uuid::new_v4();
        let header = FrameHeader {
            chunk_uuid,
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let mut payload = Vec::with_capacity(24 + bytes.len());
        payload.extend_from_slice(magic);
        payload.extend_from_slice(self.identity.as_bytes());
        payload.extend_from_slice(bytes);
        let pending = self.root.join(format!(".{name}.{chunk_uuid}.tmp"));
        write_private_file(&pending, &encode_frame(&self.key, header, &payload)?)?;
        fs::rename(&pending, self.root.join(name))
            .map_err(Error::from)
            .and_then(|()| sync_directory(&self.root))
            .inspect_err(|_| {
                let _ = fs::remove_file(&pending);
            })
    }

    /// Remove an authenticated private checkpoint after verifying its ownership.
    pub fn clear_checkpoint(&self, kind: CheckpointKind) -> Result<()> {
        if self.load_checkpoint(kind)?.is_some() {
            fs::remove_file(self.root.join(kind.spec().0))?;
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    /// Remove unnamed files only after all active writers/mergers finish.
    pub fn cleanup_files(&self) -> Result<RunCleanupFiles> {
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let (_commit, _publish) = (self.commit_lock()?, self.publish_lock()?);
        self.refuse_if_poisoned()?;
        let current = self.held();
        let held: Vec<_> = {
            let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
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
            .chain(&held)
            .map(|r| r.meta.run_id)
            .collect();
        let mut removed_files = 0;
        for (index, entry) in fs::read_dir(&self.runs)?.enumerate() {
            if index >= MAX_ENTRIES {
                return Err(Error::WriteCapacity(
                    "cleanup run-directory entry bound exceeded".into(),
                ));
            }
            let entry = entry?;
            if is_removable_run(&entry.file_name().to_string_lossy(), &protected) {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed_files += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        sync_directory(&self.runs)?;
        for run in current.newest_first() {
            let meta = fs::symlink_metadata(&run.path)?;
            if !meta.is_file() || meta.len() != run.meta.file_bytes {
                return Err(Error::Corrupt(
                    "cleanup found an invalid current run file".into(),
                ));
            }
        }
        Ok(RunCleanupFiles {
            held_bytes: held.iter().map(|r| r.meta.file_bytes).sum(),
            removed_files,
        })
    }
}

fn clean_runs(held: &Manifest) -> bool {
    held.merging.is_empty() && held.tiered().all(|run| run.hidden() == Some(0))
}
