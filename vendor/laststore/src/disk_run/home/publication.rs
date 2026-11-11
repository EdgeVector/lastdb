//! CURRENT publication, parallel durability overlap, and restore commit-file replay.

use super::commit::{meta, overflow, RunCommit};
use super::manifest::{Manifest, RunFile, CURRENT, MAX_CURRENT_BYTES, MAX_LEVELS, MAX_LEVEL_RUNS};
use super::{RunHome, RunRoot};
use crate::disk_run::space::MergeSpace;
use crate::disk_run::{PreparedRun, RunReader};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{encode_frame, min_encoded_len, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

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
            lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes,
        )?;
        let summary = reader.verified_summary(source)?;
        let run_id = summary.run_id;
        let run = meta(summary)?;
        let _commit = self.commit_lock()?;
        let _publish = self.publish_lock()?;
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

    pub(in crate::disk_run::home) fn install(&self, next: Manifest) -> Result<Arc<Manifest>> {
        self.install_with_space(next, None)
    }

    pub(in crate::disk_run::home) fn install_merge(
        &self,
        next: Manifest,
        space: &MergeSpace,
    ) -> Result<Arc<Manifest>> {
        let installed = self.install_with_space(next, Some(space));
        if installed.is_ok() || self.is_poisoned() {
            space.keep_outputs();
        }
        installed
    }

    fn install_with_space(
        &self,
        next: Manifest,
        space: Option<&MergeSpace>,
    ) -> Result<Arc<Manifest>> {
        let staged = next.stage_with_space(&self.root, &self.key, space)?;
        self.publish_staged(next, &staged)
    }

    pub(in crate::disk_run::home) fn publish_staged(
        &self,
        next: Manifest,
        staged: &Path,
    ) -> Result<Arc<Manifest>> {
        if let Err(error) = Manifest::replace(&self.root, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        let next = Arc::new(next);
        self.install_current(&next)?;
        Ok(next)
    }

    pub(in crate::disk_run::home) fn install_prepared(
        &self,
        file: Arc<RunFile>,
        run: PreparedRun,
    ) -> Result<Arc<Manifest>> {
        let mut run = Some(run);
        let parallel = std::thread::scope(|scope| {
            let worker = match std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, || {
                    run.take()
                        .expect("unclaimed private run")
                        .finish()
                        .map(|_| ())
                }) {
                Ok(worker) => worker,
                Err(_) => return None,
            };
            let _publish = match self.publish_lock() {
                Ok(guard) => guard,
                Err(error) => {
                    let _ = worker.join();
                    return Some(Err(error));
                }
            };
            let staged = self.stage_new_run(&file);
            let durable = match worker.join() {
                Ok(result) => result,
                Err(_) => Err(Error::Config("disk run: durability worker panicked".into())),
            };
            if let Err(error) = durable {
                if let Ok((_, path)) = staged {
                    let _ = fs::remove_file(path);
                }
                return Some(Err(error));
            }
            Some(staged.and_then(|(next, path)| self.publish_staged(next, &path)))
        });
        match parallel {
            Some(result) => result,
            None => {
                run.take().expect("unclaimed private run").finish()?;
                let _publish = self.publish_lock()?;
                let (next, path) = self.stage_new_run(&file)?;
                self.publish_staged(next, &path)
            }
        }
    }

    fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        next.frontier = next.frontier.checked_add(1).ok_or_else(overflow)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root, &self.key)?;
        Ok((next, path))
    }
}

impl Manifest {
    pub(in crate::disk_run::home) fn stage(&self, root: &Path, key: &[u8; 32]) -> Result<PathBuf> {
        self.stage_with_space(root, key, None)
    }

    fn stage_with_space(
        &self,
        root: &Path,
        key: &[u8; 32],
        space: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        self.check_shape()?;
        let payload = self.encode();
        if self.levels.len() > MAX_LEVELS
            || self.levels.iter().any(|level| level.len() > MAX_LEVEL_RUNS)
            || self.merging.len() > MAX_LEVEL_RUNS
            || self.base.len() > MAX_LEVEL_RUNS
            || payload.len() > MAX_CURRENT_BYTES - min_encoded_len()
        {
            return Err(Error::WriteCapacity(
                "disk run home: CURRENT would exceed its open limits; merge first".into(),
            ));
        }
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let frame = encode_frame(key, header, &payload)?;
        if let Some(space) = space {
            space.admit_current(frame.len() as u64)?;
        }
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", self.generation));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        let written = file
            .write_all(&frame)
            .map_err(Error::from)
            .and_then(|()| sync_dirty_file(&file));
        if let Err(error) = written {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(tmp)
    }

    pub(in crate::disk_run::home) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(&File::open(root)?)
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
