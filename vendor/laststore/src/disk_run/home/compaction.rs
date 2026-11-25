//! Atomic run commits and tiered/incremental base compaction on [`RunHome`].

use super::manifest::{Manifest, RunFile, MAX_LEVELS};
use super::{invalid, run_key, unpoison, RunHome};
use crate::disk_run::{
    merge_runs, Found, MergeIter, MergeSpace, PreparedRun, RunRange, RunReader, RunWriter,
};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

const MERGE_WIDTH: usize = 8;
const MAX_MERGE_INPUTS: usize = 64;
const BASE_PIECES: u64 = 64;
const MAX_BASE_RUNS: usize = 4096;
const MAX_TRAILING_RUNTS: usize = 32;
const MIN_PIECE_BYTES: u64 = 256 << 10;
const MAX_PIECE_BYTES: u64 = 1 << 30;

/// Complete native stored mutation. The home derives its physical group.
#[derive(Clone, Debug)]
pub struct HomeMutation {
    /// Exact namespace identity, never a filesystem path.
    pub collection: String,
    /// Complete encoded LastStore document id.
    pub key: String,
    /// Opaque stored body, or None for a tombstone.
    pub body: Option<Vec<u8>>,
}

impl HomeMutation {
    /// Build a single stored mutation or tombstone.
    pub fn new(col: impl Into<String>, key: impl Into<String>, body: Option<Vec<u8>>) -> Self {
        Self {
            collection: col.into(),
            key: key.into(),
            body,
        }
    }
}

enum Plan {
    Tiered(usize, Vec<Arc<RunFile>>),
    Base(Option<Vec<Arc<RunFile>>>),
}

pub(super) fn tail(runs: &[Arc<RunFile>]) -> Vec<Arc<RunFile>> {
    runs[runs.len().saturating_sub(MAX_MERGE_INPUTS)..].to_vec()
}

fn plan(held: &Manifest) -> Option<Plan> {
    if let Some(level) = held.levels.iter().position(|l| l.len() >= MERGE_WIDTH) {
        return Some(Plan::Tiered(level, tail(&held.levels[level])));
    }
    if !held.merging.is_empty() {
        return Some(Plan::Base(None));
    }
    let tiered: Vec<_> = held.tiered().cloned().collect();
    let total: u64 = held.newest_first().map(|r| r.meta.logical_bytes).sum();
    let tier: u64 = tiered.iter().map(|r| r.meta.logical_bytes).sum();
    let stale: u64 = tiered
        .iter()
        .map(|r| {
            r.hidden()
                .unwrap_or(0)
                .saturating_add(r.meta.tombstone_key_bytes)
        })
        .sum();
    let over = |b: u64, share: u64, min: u64| b > min && b.saturating_mul(share) > total;
    (over(tier, 16, 1 << 20) || over(stale, 8, 64 << 10)).then(|| Plan::Base(Some(tail(&tiered))))
}

fn trim_levels(levels: &mut Vec<Vec<Arc<RunFile>>>) {
    while levels.last().is_some_and(Vec::is_empty) {
        levels.pop();
    }
}

impl RunHome {
    pub(super) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.commit_mutex.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.publish_mutex.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_staged(&self, next: Manifest, staged: &Path) -> Result<Arc<Manifest>> {
        if let Err(err) = Manifest::replace(&self.root_dir, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let next = Arc::new(next);
        *self.current.write().map_err(|_| self.poisoned_lock())? = Arc::clone(&next);
        Ok(next)
    }

    fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        let mut next = self.next_manifest(true)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root_dir, &self.key, None)?;
        Ok((next, path))
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }

    fn install_prepared(&self, file: Arc<RunFile>, run: PreparedRun) -> Result<Arc<Manifest>> {
        let slot = Mutex::new(Some(run));
        let finish = || slot.lock().unwrap().take().unwrap().finish().map(|_| ());
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, finish);
            let guard = self.publish_lock();
            let staged = guard.as_ref().ok().map(|_| self.stage_new_run(&file));
            let panicked = |_| Err(invalid("disk run: durability worker panicked"));
            let synced = worker.map_or_else(|_| finish(), |h| h.join().unwrap_or_else(panicked));
            let (_guard, (next, staged_path)) = (guard?, staged.unwrap()?);
            synced.inspect_err(|_| drop(fs::remove_file(&staged_path)))?;
            self.publish_staged(next, &staged_path)
        })
    }

    /// Write every operation into one new run, then publish it in a new CURRENT.
    pub fn commit(&self, operations: Vec<HomeMutation>) -> Result<()> {
        self.refuse_if_poisoned()?;
        if operations.is_empty() {
            return Err(invalid("disk run home: empty commit"));
        }
        let mut records: Vec<_> = operations
            .into_iter()
            .rev()
            .map(|op| Ok((run_key(&op.collection, op.key.as_bytes())?, op.body)))
            .collect::<Result<_>>()?;
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records.dedup_by(|later, first| later.0 == first.0);
        let _commit = self.commit_lock()?;
        let mut writer = RunWriter::open(&self.runs_dir, &self.key, None)?;
        for (key, body) in &records {
            writer.push(key, body.as_deref())?;
        }
        let prepared = writer.prepare_finish()?;
        let file = RunFile::new(&self.runs_dir, prepared.writer.meta.clone());
        let installed = self.install_prepared(Arc::clone(&file), prepared);
        if installed.is_err() && !self.is_poisoned() {
            file.retire();
        }
        installed.map(|_| ())
    }

    /// True when [`RunHome::merge_step_with_space`] has work.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|r| r.hidden().is_none()) || plan(&held).is_some()
    }

    /// Merge with this home's disk cap and the shared filesystem write quota.
    pub fn merge_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = unpoison(&self.merge_mutex);
        let held = self.held();
        self.count_hidden(&held)?;
        match plan(&held) {
            None => Ok(false),
            Some(Plan::Tiered(lvl, ins)) => self.merge_tiered(lvl, ins, policy).map(|()| true),
            Some(Plan::Base(start)) => self.base_step(start, policy).map(|()| true),
        }
    }

    fn merge_tiered(
        &self,
        level: usize,
        inputs: Vec<Arc<RunFile>>,
        policy: &SpacePolicy,
    ) -> Result<()> {
        if level + 1 >= MAX_LEVELS {
            return Err(Error::WriteCapacity(
                "disk run home: no level is left for a merge".into(),
            ));
        }
        let space = MergeSpace::new(self.path(), policy)?;
        let open_r = |r: &Arc<RunFile>| RunReader::open(&r.path, &self.key).map(Arc::new);
        let readers: Vec<_> = inputs.iter().map(open_r).collect::<Result<_>>()?;
        let mut writer = RunWriter::open(&self.runs_dir, &self.key, Some(&space))?;
        let written = merge_runs(readers.iter().map(RunReader::iter).collect(), &mut writer)?;
        let output = (written > 0).then(|| writer.finish()).transpose()?;
        let in_bytes: u64 = inputs.iter().map(|r| r.meta.logical_bytes).sum();
        let out_bytes = output.as_ref().map_or(0, |r| r.logical_bytes);
        let sum_hidden = inputs.iter().map(|r| r.hidden()).sum::<Option<u64>>();
        let hidden = sum_hidden.map(|h| h.saturating_sub(in_bytes.saturating_sub(out_bytes)));
        let _publish = self.publish_lock()?;
        let mut next = self.next_manifest(false)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|r| r.meta.id).collect();
        next.levels[level].retain(|r| !merged.contains(&r.meta.id));
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            let file = RunFile::new(&self.runs_dir, run);
            if let Some(h) = hidden {
                file.set_hidden(h);
            }
            next.levels[level + 1].insert(0, file);
        }
        trim_levels(&mut next.levels);
        self.install_merge(next, &space)?;
        self.retire_runs(&inputs);
        Ok(())
    }

    pub(super) fn base_step(
        &self,
        start: Option<Vec<Arc<RunFile>>>,
        policy: &SpacePolicy,
    ) -> Result<()> {
        let held = self.held();
        let (merging, cut) = start.as_ref().map_or_else(
            || (held.merging.clone(), held.merge_cut.clone()),
            |r| (r.clone(), None),
        );
        let done = cut
            .as_ref()
            .map_or(0, |c| held.base.partition_point(|r| r.meta.last_key <= *c));
        let bytes: u64 = merging
            .iter()
            .chain(&held.base)
            .map(|r| r.meta.file_bytes)
            .sum();
        let mut target = (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES);
        if held.base.len() > MAX_BASE_RUNS / 2 {
            target = target.max((bytes / (MAX_BASE_RUNS as u64 / 4)).min(MAX_PIECE_BYTES));
        }
        let (mut take, mut acc, mut runts) = (0, 0u64, 0);
        for run in &held.base[done..] {
            if take > 0 && acc >= target {
                let runt = run.meta.file_bytes < target / 2
                    && acc.saturating_add(run.meta.file_bytes) <= target.saturating_mul(2)
                    && runts < MAX_TRAILING_RUNTS;
                if !runt {
                    break;
                }
                runts += 1;
            }
            (take, acc) = (take + 1, acc.saturating_add(run.meta.file_bytes));
        }
        let taken = &held.base[done..done + take];
        let last = done + take == held.base.len();
        let upper = (!last)
            .then(|| taken.last().map(|r| r.meta.last_key.clone()))
            .flatten();
        let space = MergeSpace::new(self.path(), policy)?;
        let bounds = (cut.as_deref(), upper.as_deref());
        let pieces = self.write_base_pieces(&merging, taken, bounds, target, &space)?;
        let _publish = self.publish_lock()?;
        let mut next = self.next_manifest(false)?;
        if let Some(runs) = &start {
            let moved: HashSet<Uuid> = runs.iter().map(|r| r.meta.id).collect();
            for level in &mut next.levels {
                level.retain(|r| !moved.contains(&r.meta.id));
            }
            trim_levels(&mut next.levels);
            next.merging = runs.clone();
        }
        if let Some(p) = &pieces {
            next.base.splice(done..done + take, p.iter().cloned());
        }
        next.merge_cut = (!last).then_some(upper).flatten();
        if last {
            next.merging.clear();
        }
        self.install_merge(next, &space)?;
        if pieces.is_some() {
            self.retire_runs(taken);
        }
        if last {
            self.retire_runs(&merging);
        }
        Ok(())
    }

    fn write_base_pieces(
        &self,
        merging: &[Arc<RunFile>],
        taken: &[Arc<RunFile>],
        (cut, upper): (Option<&[u8]>, Option<&[u8]>),
        target: u64,
        space: &MergeSpace,
    ) -> Result<Option<Vec<Arc<RunFile>>>> {
        let overlaps = merging.iter().any(|r| {
            cut.is_none_or(|a| r.meta.last_key.as_slice() > a)
                && upper.is_none_or(|t| r.meta.first_key.as_slice() <= t)
        });
        let taken_bytes: u64 = taken.iter().map(|r| r.meta.file_bytes).sum();
        let small = taken
            .iter()
            .filter(|r| r.meta.file_bytes < target / 2)
            .count();
        let coalesce_cold = taken.len() > 1 && (taken_bytes <= target || small > 1);
        if !overlaps && !coalesce_cold {
            return Ok(None);
        }
        let mut sources = Vec::new();
        if overlaps {
            let lo = cut.map_or(Bound::Unbounded, Bound::Excluded);
            let up = upper.map_or(Bound::Unbounded, Bound::Included);
            for r in merging {
                sources.push(Arc::new(RunReader::open(&r.path, &self.key)?).range(lo, up));
            }
        }
        sources.push(RunRange::sequence(
            taken.iter().map(|r| r.path.clone()).collect(),
            &self.key,
            Bound::Unbounded,
            Bound::Unbounded,
        ));
        self.write_merged_pieces(sources, target, space).map(Some)
    }

    fn write_merged_pieces(
        &self,
        sources: Vec<RunRange>,
        target: u64,
        space: &MergeSpace,
    ) -> Result<Vec<Arc<RunFile>>> {
        let min_tail = (target / 2).min(4 << 20);
        let mut pieces = Vec::new();
        let res = (|| -> Result<()> {
            let (mut writer, mut tail, mut tail_bytes): (Option<RunWriter>, Vec<_>, u64) =
                (None, Vec::new(), 0);
            for record in MergeIter::new(sources) {
                let (key, Found::Value(val)) = record? else {
                    continue;
                };
                let rec_bytes = (key.len() + val.len()) as u64;
                if writer.as_ref().is_some_and(|w| w.bytes_written() >= target) {
                    if tail_bytes.saturating_add(rec_bytes) < min_tail {
                        tail_bytes = tail_bytes.saturating_add(rec_bytes);
                        tail.push((key, val));
                        continue;
                    }
                    let done = writer.take().unwrap().finish()?;
                    pieces.push(RunFile::new(&self.runs_dir, done));
                    let open =
                        writer.insert(RunWriter::open(&self.runs_dir, &self.key, Some(space))?);
                    for (tk, tv) in tail.drain(..) {
                        open.push(&tk, Some(&tv))?;
                    }
                    tail_bytes = 0;
                }
                let open = match &mut writer {
                    Some(w) => w,
                    s => s.insert(RunWriter::open(&self.runs_dir, &self.key, Some(space))?),
                };
                open.push(&key, Some(&val))?;
            }
            if let Some(mut open) = writer {
                for (tk, tv) in tail {
                    open.push(&tk, Some(&tv))?;
                }
                pieces.push(RunFile::new(&self.runs_dir, open.finish()?));
            }
            Ok(())
        })();
        res.inspect_err(|_| pieces.iter().for_each(|p| p.retire()))
            .map(|()| pieces)
    }
}
