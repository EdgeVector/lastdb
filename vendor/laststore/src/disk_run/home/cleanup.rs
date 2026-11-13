//! Commit retention, replay, hidden-version counting, checkpoints, and run-file cleanup.

use super::commit::{tail, RunCommit};
use super::manifest::{Manifest, RunFile};
use super::{
    invalid, open_nofollow, sweep_runs, unpoison, write_private_file, RunHome, CAS_DELETE,
    NODE_CLEANUP,
};
use crate::disk_run::codec::corrupt;
use crate::disk_run::{Found, RunMeta, RunReader, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame, encode_frame};
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
pub const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 << 20;
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

fn euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

fn private_parent(path: &Path) -> Result<()> {
    let p = path.parent().ok_or_else(|| invalid("log file parent"))?;
    let m = fs::symlink_metadata(p)?;
    let perm = m.is_dir() && m.uid() == euid() && m.mode() & 0o077 == 0;
    let ok = path.is_absolute() && p.canonicalize()? == p && perm;
    ok.then_some(())
        .ok_or_else(|| invalid("log private directory"))
}

fn link_synced(source: &Path, target: &Path, bytes: u64) -> Result<()> {
    let b = fs::symlink_metadata(source)?;
    if !b.is_file() || b.len() != bytes {
        return Err(invalid("log retained inode differs"));
    }
    fs::hard_link(source, target)?;
    let a = fs::symlink_metadata(target)?;
    if !a.is_file() || (b.dev(), b.ino(), bytes) != (a.dev(), a.ino(), a.len()) {
        return Err(invalid("log retained inode differs"));
    }
    File::open(target)?.sync_all()?;
    sync_directory(target.parent().ok_or_else(|| invalid("log link parent"))?)
}

fn verified_summary(path: &Path, key: &[u8; 32]) -> Result<RunMeta> {
    let adm = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
    let r = RunReader::open_bounded(path, key, adm, 0)?;
    let mut meta = RunMeta(
        r.4 .0,
        [0, 0, 0, 0, fs::metadata(path)?.len()],
        Vec::new(),
        Vec::new(),
    );
    let mut blocks = 0u64;
    for num in 0..r.5 .1.len() {
        let index = r.read_index(num, None, Some(adm))?;
        for blk in &index.0 {
            let mut b = r.open_block(blk, &index, adm / 4, meta.3.capacity() as u64)?;
            while let Some(live) = b.next(Some(&blk.0))? {
                if meta.1[0] > 0 && meta.3 >= b.1 {
                    return Err(corrupt("imported mutation run key order"));
                }
                meta.note_record(&b.1, live.then_some(b.3 .0))?;
            }
            blocks += 1;
        }
    }
    if meta.1[0] == 0 {
        return Err(invalid("disk run home: an empty run has no place"));
    }
    let exp_first = r.5 .1.first().map(|i| i.0.as_slice());
    let ok = (meta.1[0], blocks) == (r.4 .3, u64::from(r.4 .5))
        && Some(meta.2.as_slice()) == exp_first
        && meta.3 == r.5 .2;
    ok.then_some(meta)
        .ok_or_else(|| corrupt("imported mutation run complete summary"))
}

impl RunCommit {
    /// The held root protects the new run from compaction until this link is durable.
    pub fn retain_file(&self, destination: &Path) -> Result<()> {
        private_parent(destination)?;
        let f = self
            .root
            .newest_first()
            .find(|f| f.0 .0 == self.run_id)
            .ok_or_else(|| invalid("commit run absent"))?;
        (f.0 .1[4] == self.run_bytes)
            .then(|| link_synced(&f.1, destination, self.run_bytes))
            .ok_or_else(|| invalid("commit run length"))?
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
        let run = verified_summary(source, &self.key)?;
        let (run_id, run_bytes) = (run.0, run.1[4]);
        let (_commit, _publish) = (self.commit_lock()?, self.publish_lock()?);
        self.refuse_if_poisoned()?;
        let held = self.held();
        if held.frontier == after {
            let ok = held
                .newest_first()
                .find(|f| f.0 .0 == run_id)
                .ok_or_else(|| invalid("replayed log run is absent"))?;
            return (ok.0 == run && equal_files(&ok.1, source)?)
                .then(|| RunCommit::new(run_id, run_bytes, held))
                .ok_or_else(|| invalid("replayed log bytes differ"));
        }
        if held.frontier != before || held.newest_first().any(|f| f.0 .0 == run_id) {
            return Err(invalid("log replay frontier or run identity"));
        }
        let target = self.paths.1.join(format!("{run_id}{RUN_SUFFIX}"));
        if !target.try_exists()? {
            link_synced(source, &target, run_bytes)?;
        } else if !equal_files(&target, source)? {
            return Err(invalid("log staged run differs"));
        }
        let file = RunFile::new(&self.paths.1, run);
        let res = self
            .stage_new_run(&file)
            .and_then(|(n, p)| self.publish_staged(n, &p));
        if res.is_err() && !self.is_poisoned() {
            file.retire();
        }
        Ok(RunCommit::new(run_id, run_bytes, res?))
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
        let (rk0, rk1) = (&run.0 .2, &run.0 .3);
        let older: Vec<_> = older
            .iter()
            .copied()
            .filter(|o| o.0 .2 <= *rk1 && *rk0 <= o.0 .3)
            .collect();
        if older.is_empty() {
            return Ok(0);
        }
        let reader = Arc::new(RunReader::open(&run.1, &self.key)?);
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
                if o.0 .3 < *first {
                    readers[slot] = None;
                    continue;
                }
                let lookups: Vec<&[u8]> = open
                    .iter()
                    .filter_map(|&at| o.0.covers(&keys[at]).then_some(keys[at].as_slice()))
                    .collect();
                if !lookups.is_empty() {
                    if readers[slot].is_none() {
                        readers[slot] = Some(RunReader::open(&o.1, &self.key)?);
                    }
                    let r = readers[slot].as_ref().unwrap();
                    let mut hits = r
                        .get_many_with(&lookups, |rd, n, sc| {
                            rd.read_index(n, Some(sc), None).map(Arc::new)
                        })?
                        .into_iter();
                    open.retain(|&at| {
                        let Some(found) =
                            o.0.covers(&keys[at])
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
                if keys.last().is_some_and(|last| o.0 .3 <= *last) {
                    readers[slot] = None;
                }
                if open.is_empty() {
                    break;
                }
            }
        }
    }

    /// Run one explicit base-compaction step under `policy`.
    pub fn cleanup_step(&self, policy: &SpacePolicy) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = unpoison(&self.locks.2);
        let held = self.held();
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
        let mut file = match open_nofollow(&self.paths.0.join(name)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            r => r?,
        };
        let m = file.metadata()?;
        if !(m.is_file() && m.mode() & 0o077 == 0 && m.nlink() == 1 && m.uid() == euid())
            || m.len() > (maximum + 4096) as u64
        {
            return Err(Error::Corrupt("invalid cleanup checkpoint file".into()));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((maximum + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let payload = decode_frame(&self.key, &bytes, maximum + 24, (None, None))?.2;
        let ok = payload.len() >= 24
            && &payload[..8] == magic
            && &payload[8..24] == self.identity.as_bytes();
        let err = || Error::Corrupt("cleanup checkpoint belongs to another role or home".into());
        ok.then(|| Some(payload[24..].to_vec())).ok_or_else(err)
    }

    /// Durably replace one authenticated private checkpoint.
    pub fn save_checkpoint(&self, kind: CheckpointKind, bytes: &[u8]) -> Result<()> {
        let (name, magic, maximum) = kind.spec();
        if bytes.len() > maximum {
            return Err(Error::Corrupt(
                "cleanup checkpoint exceeds its byte bound".into(),
            ));
        }
        let _publish = unpoison(&self.locks.1);
        self.refuse_if_poisoned()?;
        let id = Uuid::new_v4();
        let payload = [magic.as_slice(), self.identity.as_bytes(), bytes].concat();
        let pending = self.paths.0.join(format!(".{name}.{id}.tmp"));
        write_private_file(&pending, &encode_frame(&self.key, id, 0, &payload)?)?;
        fs::rename(&pending, self.paths.0.join(name))
            .map_err(Error::from)
            .and_then(|()| sync_directory(&self.paths.0))
            .inspect_err(|_| drop(fs::remove_file(&pending)))
    }

    /// Remove an authenticated private checkpoint after verifying its ownership.
    pub fn clear_checkpoint(&self, kind: CheckpointKind) -> Result<()> {
        if self.load_checkpoint(kind)?.is_some() {
            fs::remove_file(self.paths.0.join(kind.spec().0))?;
            sync_directory(&self.paths.0)?;
        }
        Ok(())
    }

    /// Remove unnamed files after writers/mergers finish, returning `(held_bytes, removed_files)`.
    pub fn cleanup_files(&self) -> Result<(u64, u64)> {
        let (_m, _c, _p) = (
            unpoison(&self.locks.2),
            self.commit_lock()?,
            self.publish_lock()?,
        );
        self.refuse_if_poisoned()?;
        let (current, (retired_len, held)) = (self.held(), self.live_retired());
        if retired_len > MAX_ENTRIES {
            return Err(Error::WriteCapacity(
                "cleanup held-file bound exceeded".into(),
            ));
        }
        let keep: HashSet<_> = current
            .newest_first()
            .chain(&held)
            .map(|r| r.0 .0)
            .collect();
        let removed = sweep_runs(&self.paths.1, &keep, MAX_ENTRIES)?;
        sync_directory(&self.paths.1)?;
        current.verify_files("cleanup found an invalid current run file")?;
        Ok((held.iter().map(|r| r.0 .1[4]).sum(), removed))
    }
}

fn equal_files(left: &Path, right: &Path) -> Result<bool> {
    let (ml, mr) = (fs::symlink_metadata(left)?, fs::symlink_metadata(right)?);
    if !ml.is_file() || !mr.is_file() {
        return Err(invalid("log regular file"));
    }
    if ml.len() != mr.len() {
        return Ok(false);
    }
    let (mut l, mut r) = (File::open(left)?, File::open(right)?);
    let (mut a, mut b) = ([0; 8192], [0; 8192]);
    while let n @ 1.. = l.read(&mut a)? {
        r.read_exact(&mut b[..n])?;
        if a[..n] != b[..n] {
            return Ok(false);
        }
    }
    Ok(true)
}
