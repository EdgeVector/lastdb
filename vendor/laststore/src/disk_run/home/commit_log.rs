//! Retain original commit bytes and replay them on an exclusively owned restore home.
use super::commit::{meta, RunCommit};
use super::manifest::RunFile;
use super::{RunHome, RunRoot};
use crate::disk_run::RunReader;
use crate::durability::sync_directory;
use crate::{Error, Result};
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

fn invalid(reason: &str) -> Error {
    Error::Config(reason.to_owned())
}

impl RunCommit {
    /// The held root protects the new run from compaction until this link is durable.
    pub fn retain_file(&self, destination: &Path) -> Result<()> {
        private_parent(destination)?;
        let file = self
            .root
            .0
            .newest_first()
            .find(|file| file.meta.run_id == self.run_id)
            .ok_or_else(|| invalid("commit run is absent from its held root"))?;
        if file.meta.file_bytes != self.run_bytes {
            return Err(invalid("commit run length"));
        }
        link(&file.path, destination, self.run_bytes)
    }
}

impl RunHome {
    /// Restore-only import. Original encrypted records and tombstones remain unchanged.
    /// A repeated exact step after a crash is a no-op. No merge starts here.
    pub fn replay_commit_file(&self, source: &Path, before: u64, after: u64) -> Result<RunCommit> {
        if before.checked_add(1) != Some(after) {
            return Err(invalid("log frontier step"));
        }
        private_parent(source)?;
        let original = fs::symlink_metadata(source)?;
        if !original.is_file()
            || original.uid() != rustix::process::geteuid().as_raw()
            || original.mode() & 0o022 != 0
        {
            return Err(invalid("log source file"));
        }
        let reader = RunReader::open_bounded(
            source,
            &self.key,
            lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes,
        )?;
        let summary = reader.verified_summary(source)?;
        let run_id = summary.run_id;
        let run = meta(summary)?;
        let _commit = self
            .commit
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _publish = self
            .publish
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.refuse_if_poisoned()?;
        let held = self.held();
        if held.frontier == after {
            let old = held
                .newest_first()
                .find(|file| file.meta.run_id == run_id)
                .ok_or_else(|| invalid("replayed log run is absent"))?;
            if old.meta != run || !equal_files(&old.path, source)? {
                return Err(invalid("replayed log bytes differ"));
            }
            return Ok(RunCommit {
                run_id,
                root: RunRoot(Arc::clone(&held)),
                generation: held.generation,
                frontier: after,
                records: run.records,
                run_bytes: run.file_bytes,
            });
        }
        if held.frontier != before || held.newest_first().any(|file| file.meta.run_id == run_id) {
            return Err(invalid("log replay frontier or run identity"));
        }
        let target = self.runs.join(format!("{run_id}.ldrun"));
        if target.try_exists()? {
            if !equal_files(&target, source)? {
                return Err(invalid("log staged run differs"));
            }
        } else {
            link(source, &target, run.file_bytes)?;
        }
        let mut next = (*held).clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("log generation overflow"))?;
        next.frontier = after;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        let records = run.records;
        let run_bytes = run.file_bytes;
        let file = RunFile::new(&self.runs, run);
        next.levels[0].insert(0, Arc::clone(&file));
        let installed = self.install_or_discard(next, &[file])?;
        Ok(RunCommit {
            run_id,
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            frontier: installed.frontier,
            records,
            run_bytes,
        })
    }
}

fn private_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| invalid("log file parent"))?;
    let info = fs::symlink_metadata(parent)?;
    if !path.is_absolute()
        || parent.canonicalize()? != parent
        || !info.is_dir()
        || info.uid() != rustix::process::geteuid().as_raw()
        || info.mode() & 0o077 != 0
    {
        return Err(invalid("log private directory"));
    }
    Ok(())
}

fn link(source: &Path, target: &Path, bytes: u64) -> Result<()> {
    let before = fs::symlink_metadata(source)?;
    if !before.is_file() || before.len() != bytes {
        return Err(invalid("log original file"));
    }
    fs::hard_link(source, target)?;
    let held = fs::symlink_metadata(target)?;
    if !held.is_file()
        || (before.dev(), before.ino(), bytes) != (held.dev(), held.ino(), held.len())
    {
        return Err(invalid("log retained inode differs"));
    }
    File::open(target)?.sync_all()?;
    sync_directory(&File::open(
        target.parent().ok_or_else(|| invalid("log link parent"))?,
    )?)
}

fn equal_files(left: &Path, right: &Path) -> Result<bool> {
    for path in [left, right] {
        if !fs::symlink_metadata(path)?.is_file() {
            return Err(invalid("log regular file"));
        }
    }
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = File::open(left)?;
    let mut right = File::open(right)?;
    let mut a = [0u8; 65536];
    let mut b = [0u8; 65536];
    loop {
        let count = left.read(&mut a)?;
        if count == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..count])?;
        if a[..count] != b[..count] {
            return Ok(false);
        }
    }
}
