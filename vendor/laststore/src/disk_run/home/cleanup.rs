//! Explicit base compaction, hidden-version counting, checkpoints, commit replay, and file removal.

use super::commit::{tail, RunCommit};
use super::manifest::{Manifest, RunFile};
use super::{
    capture::{link_synced, private_parent},
    invalid, is_removable_run, open_nofollow, write_private_file, RunHome, CAS_DELETE,
    NODE_CLEANUP,
};
use crate::disk_run::{Found, MetadataMemo, RunReader, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame_bounded, encode_frame};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
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

impl RunHome {
    /// Restore-only import. Original encrypted records and tombstones remain unchanged.
    pub fn replay_commit_file(&self, source: &Path, before: u64, after: u64) -> Result<RunCommit> {
        if before.checked_add(1) != Some(after) {
            return Err(invalid("log frontier step"));
        }
        private_parent(source)?;
        let orig = fs::symlink_metadata(source)?;
        if !orig.is_file() || orig.uid() != euid() || orig.mode() & 0o022 != 0 {
            return Err(invalid("log source file"));
        }
        let working = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        let run =
            RunReader::open_bounded(source, &self.key, working, 0)?.verified_summary(source)?;
        let (run_id, records, run_bytes) = (run.run_id, run.records, run.file_bytes);
        let (_commit, _publish) = (self.commit_lock()?, self.publish_lock()?);
        self.refuse_if_poisoned()?;
        let held = self.held();
        if held.frontier == after {
            let ok = held
                .newest_first()
                .find(|f| f.meta.run_id == run_id)
                .ok_or_else(|| invalid("replayed log run is absent"))?;
            return (ok.meta == run && equal_files(&ok.path, source)?)
                .then(|| RunCommit::from_manifest(run_id, records, run_bytes, held))
                .ok_or_else(|| invalid("replayed log bytes differ"));
        }
        if held.frontier != before || held.newest_first().any(|f| f.meta.run_id == run_id) {
            return Err(invalid("log replay frontier or run identity"));
        }
        let target = self.runs.join(format!("{run_id}{RUN_SUFFIX}"));
        if !target.try_exists()? {
            link_synced(source, &target, run_bytes)?;
        } else if !equal_files(&target, source)? {
            return Err(invalid("log staged run differs"));
        }
        let file = RunFile::new(&self.runs, run);
        let res = self
            .stage_new_run(&file)
            .and_then(|(n, p)| self.publish_staged(n, &p));
        if res.is_err() && !self.is_poisoned() {
            file.retire();
        }
        Ok(RunCommit::from_manifest(run_id, records, run_bytes, res?))
    }

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
        let older: Vec<_> = older
            .iter()
            .copied()
            .filter(|o| o.meta.meets(&run.meta))
            .collect();
        if older.is_empty() {
            return Ok(0);
        }
        let reader = Arc::new(RunReader::open(&run.path, &self.key)?);
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
            let mut open: Vec<usize> = (0..keys.len()).collect();
            for (slot, o) in older.iter().enumerate() {
                if o.meta.last_key < *first {
                    readers[slot] = None;
                    continue;
                }
                let lookups: Vec<&[u8]> = open
                    .iter()
                    .filter_map(|&at| o.meta.covers(&keys[at]).then_some(keys[at].as_slice()))
                    .collect();
                if !lookups.is_empty() {
                    let r = match &mut readers[slot] {
                        Some(r) => r,
                        None => readers[slot].insert(RunReader::open(&o.path, &self.key)?),
                    };
                    let mut hits = r
                        .get_many_with(&lookups, &mut MetadataMemo::new(0))?
                        .into_iter();
                    open.retain(|&at| {
                        let Some(found) = o
                            .meta
                            .covers(&keys[at])
                            .then(|| hits.next().unwrap())
                            .flatten()
                        else {
                            return true;
                        };
                        let vlen = if let Found::Value(v) = found {
                            v.len()
                        } else {
                            0
                        };
                        hidden = hidden.saturating_add((keys[at].len() + vlen) as u64);
                        false
                    });
                }
                if keys.last().is_some_and(|last| o.meta.last_key <= *last) {
                    readers[slot] = None;
                }
                if open.is_empty() {
                    break;
                }
            }
        }
    }

    /// Whether the current files contain no known or uncounted hidden versions.
    pub fn snapshot_has_clean_runs(&self) -> bool {
        clean_runs(&self.held())
    }

    /// Remove hidden versions below the ordinary merge thresholds before a new cloud snapshot.
    pub fn snapshot_cleanup_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.cleanup_step(policy, true)
    }

    /// Run one explicit base-compaction step under `policy`, optionally stopping once snapshot-clean.
    pub fn cleanup_step(&self, policy: &SpacePolicy, snapshot: bool) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        if snapshot {
            self.count_hidden(&held)?;
            if clean_runs(&held) {
                return Ok(false);
            }
        }
        if !held.merging.is_empty() {
            return self.base_step(None, policy).map(|()| true);
        }
        let tiered: Vec<_> = held.tiered().cloned().collect();
        (!tiered.is_empty())
            .then(|| self.base_step(Some(tail(&tiered)), policy))
            .transpose()
            .map(|r| r.is_some())
    }

    /// Read one authenticated private checkpoint, if present.
    pub fn load_checkpoint(&self, kind: CheckpointKind) -> Result<Option<Vec<u8>>> {
        let (name, magic, maximum) = kind.spec();
        let mut file = match open_nofollow(&self.root.join(name)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let m = file.metadata()?;
        if !m.is_file()
            || m.mode() & 0o077 != 0
            || m.nlink() != 1
            || m.uid() != euid()
            || m.len() > (maximum + 4096) as u64
        {
            return Err(Error::Corrupt("invalid cleanup checkpoint file".into()));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((maximum + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let payload = decode_frame_bounded(&self.key, &bytes, maximum + 24)?;
        if payload.len() < 24
            || &payload[..8] != magic
            || &payload[8..24] != self.identity.as_bytes()
        {
            return Err(Error::Corrupt(
                "cleanup checkpoint belongs to another role or home".into(),
            ));
        }
        Ok(Some(payload[24..].to_vec()))
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
        let id = Uuid::new_v4();
        let payload = [magic.as_slice(), self.identity.as_bytes(), bytes].concat();
        let pending = self.root.join(format!(".{name}.{id}.tmp"));
        write_private_file(&pending, &encode_frame(&self.key, id, 0, &payload)?)?;
        fs::rename(&pending, self.root.join(name))
            .map_err(Error::from)
            .and_then(|()| sync_directory(&self.root))
            .inspect_err(|_| drop(fs::remove_file(&pending)))
    }

    /// Remove an authenticated private checkpoint after verifying its ownership.
    pub fn clear_checkpoint(&self, kind: CheckpointKind) -> Result<()> {
        if self.load_checkpoint(kind)?.is_some() {
            fs::remove_file(self.root.join(kind.spec().0))?;
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    /// Remove unnamed files after writers/mergers finish, returning `(held_bytes, removed_files)`.
    pub fn cleanup_files(&self) -> Result<(u64, u64)> {
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let (_commit, _publish) = (self.commit_lock()?, self.publish_lock()?);
        self.refuse_if_poisoned()?;
        let current = self.held();
        let held: Vec<_> = {
            let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
            retired.retain(|r| r.strong_count() > 0);
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
        let keep: HashSet<_> = current
            .newest_first()
            .chain(&held)
            .map(|r| r.meta.run_id)
            .collect();
        let mut removed = 0;
        for (idx, entry) in fs::read_dir(&self.runs)?.enumerate() {
            if idx >= MAX_ENTRIES {
                return Err(Error::WriteCapacity(
                    "cleanup run-directory entry bound exceeded".into(),
                ));
            }
            let entry = entry?;
            if is_removable_run(&entry.file_name().to_string_lossy(), &keep) {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed += 1,
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
        Ok((held.iter().map(|r| r.meta.file_bytes).sum(), removed))
    }
}

pub(super) fn euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

fn clean_runs(held: &Manifest) -> bool {
    held.merging.is_empty() && held.tiered().all(|r| r.hidden() == Some(0))
}

fn equal_files(left: &Path, right: &Path) -> Result<bool> {
    let (ml, mr) = (fs::symlink_metadata(left)?, fs::symlink_metadata(right)?);
    if !ml.is_file() || !mr.is_file() {
        return Err(invalid("log regular file"));
    }
    if ml.len() != mr.len() {
        return Ok(false);
    }
    let (mut l, mut r, mut a, mut b) =
        (File::open(left)?, File::open(right)?, [0; 8192], [0; 8192]);
    loop {
        let n = l.read(&mut a)?;
        if n == 0 || {
            r.read_exact(&mut b[..n])?;
            a[..n] != b[..n]
        } {
            return Ok(n == 0);
        }
    }
}
