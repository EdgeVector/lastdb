//! Commits, lock/staging helpers, merge planning, and tiered/base compaction.

use super::manifest::{Manifest, RunFile, MAX_LEVELS};
use super::{invalid, run_key, RunHome};
use crate::disk_run::{
    merge_runs, Found, MergeIter, MergeSpace, RunRange, RunReader, RunWriter, MAX_RUN_VALUE_BYTES,
};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, MutexGuard};
use uuid::Uuid;

const MERGE_WIDTH: usize = 8;
const MAX_MERGE_INPUTS: usize = 64;
const BASE_PIECES: u64 = 64;
const MIN_PIECE_BYTES: u64 = 256 << 10;
const MAX_PIECE_BYTES: u64 = 1 << 30;

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
    let total: u64 = held.newest_first().map(|r| r.meta.record_bytes).sum();
    let tier: u64 = tiered.iter().map(|r| r.meta.record_bytes).sum();
    let stale: u64 = tiered
        .iter()
        .map(|r| {
            r.hidden()
                .unwrap_or(0)
                .saturating_add(r.meta.tombstone_bytes)
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

/// One published commit.
#[derive(Debug, Clone)]
pub struct RunCommit {
    /// The immutable native file written by this commit.
    pub run_id: Uuid,
    /// Complete commits, this one included.
    pub frontier: u64,
    /// Size of the new run file.
    pub run_bytes: u64,
    pub(super) root: Arc<Manifest>,
}

impl RunCommit {
    pub(super) fn new(run_id: Uuid, run_bytes: u64, root: Arc<Manifest>) -> Self {
        Self {
            run_id,
            frontier: root.frontier,
            run_bytes,
            root,
        }
    }
}

impl RunHome {
    pub(super) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.locks.0.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.locks.1.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_staged(&self, next: Manifest, staged: &Path) -> Result<Arc<Manifest>> {
        if let Err(err) = Manifest::replace(&self.root, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let next = Arc::new(next);
        *self.current.write().map_err(|_| self.poisoned_lock())? = Arc::clone(&next);
        Ok(next)
    }

    pub(super) fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        let mut next = self.next_manifest(true)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root, &self.key, None)?;
        Ok((next, path))
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }

    /// True when [`RunHome::merge_step`] has work.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|r| r.hidden().is_none()) || plan(&held).is_some()
    }

    /// Write every operation into one new run, then publish it in a new CURRENT.
    pub fn commit(&self, operations: Vec<HomeMutation>) -> Result<RunCommit> {
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
        let mut writer = RunWriter::open(&self.runs, &self.key, None)?;
        for (k, body) in &records {
            writer.push(k, body.as_deref())?;
        }
        let prepared = writer.prepare_finish()?;
        let file = RunFile::new(&self.runs, prepared.0.meta.clone());
        let (run_id, run_bytes) = (file.meta.run_id, file.meta.file_bytes);
        let installed = self.install_prepared(Arc::clone(&file), prepared);
        if installed.is_err() && !self.is_poisoned() {
            file.retire();
        }
        let root = installed?;
        Ok(RunCommit::new(run_id, run_bytes, root))
    }

    /// Refuse a batch that [`RunHome::commit`] would refuse for its key or value sizes.
    pub fn check_mutations(operations: &[HomeMutation]) -> Result<()> {
        for op in operations {
            run_key(&op.collection, op.key.as_bytes())?;
            if op
                .body
                .as_ref()
                .is_some_and(|b| b.len() > MAX_RUN_VALUE_BYTES)
            {
                return Err(invalid("disk run home: value exceeds the run value limit"));
            }
        }
        Ok(())
    }

    /// Run a tiered or base merge step, first counting new hidden records.
    pub fn merge_step(&self) -> Result<bool> {
        self.merge_step_with_space(&SpacePolicy::default())
    }

    /// Merge with this home's disk cap and the shared filesystem write quota.
    pub fn merge_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = self.locks.2.lock().unwrap_or_else(|e| e.into_inner());
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
        let readers: Vec<_> = inputs
            .iter()
            .map(|r| RunReader::open(&r.path, &self.key).map(Arc::new))
            .collect::<Result<_>>()?;
        let mut writer = RunWriter::open(&self.runs, &self.key, Some(&space))?;
        let written = merge_runs(readers.iter().map(RunReader::iter).collect(), &mut writer)?;
        let output = (written > 0).then(|| writer.finish()).transpose()?;
        let in_bytes: u64 = inputs.iter().map(|r| r.meta.record_bytes).sum();
        let out_bytes = output.as_ref().map_or(0, |r| r.record_bytes);
        let hidden = inputs
            .iter()
            .map(|r| r.hidden())
            .sum::<Option<u64>>()
            .map(|h| h.saturating_sub(in_bytes.saturating_sub(out_bytes)));
        let _publish = self.publish_lock()?;
        let mut next = self.next_manifest(false)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|r| r.meta.run_id).collect();
        next.levels[level].retain(|r| !merged.contains(&r.meta.run_id));
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            let file = RunFile::new(&self.runs, run);
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
        let (merging, cut) = if let Some(runs) = &start {
            (runs.clone(), None)
        } else {
            (held.merging.clone(), held.merge_cut.clone())
        };
        let done = cut
            .as_ref()
            .map_or(0, |c| held.base.partition_point(|r| r.meta.last_key <= *c));
        let bytes: u64 = merging
            .iter()
            .chain(&held.base)
            .map(|r| r.meta.file_bytes)
            .sum();
        let target = (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES);
        let (mut take, mut acc) = (0, 0);
        for r in &held.base[done..] {
            if take > 0 && acc >= target {
                break;
            }
            (take, acc) = (take + 1, acc + r.meta.file_bytes);
        }
        let (taken, last) = (
            &held.base[done..done + take],
            done + take == held.base.len(),
        );
        let upper = if last {
            None
        } else {
            taken.last().map(|r| r.meta.last_key.clone())
        };
        let space = MergeSpace::new(self.path(), policy)?;
        let iv = (cut.as_deref(), upper.as_deref());
        let pieces = self.write_base_pieces(&merging, taken, iv, target, &space)?;
        let _publish = self.publish_lock()?;
        let mut next = self.next_manifest(false)?;
        if let Some(runs) = &start {
            let moved: HashSet<Uuid> = runs.iter().map(|r| r.meta.run_id).collect();
            for l in &mut next.levels {
                l.retain(|r| !moved.contains(&r.meta.run_id));
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
        if !merging.iter().any(|r| {
            cut.is_none_or(|a| r.meta.last_key.as_slice() > a)
                && upper.is_none_or(|t| r.meta.first_key.as_slice() <= t)
        }) {
            return Ok(None);
        }
        let (lo, up) = (
            cut.map_or(Bound::Unbounded, Bound::Excluded),
            upper.map_or(Bound::Unbounded, Bound::Included),
        );
        let mut sources: Vec<_> = merging
            .iter()
            .map(|r| Ok(Arc::new(RunReader::open(&r.path, &self.key)?).range(lo, up)))
            .collect::<Result<_>>()?;
        let paths = taken.iter().map(|r| r.path.clone()).collect();
        sources.push(RunRange::sequence(
            paths,
            &self.key,
            Bound::Unbounded,
            Bound::Unbounded,
        ));
        let mut pieces = Vec::new();
        let res = (|| -> Result<()> {
            let mut writer: Option<RunWriter> = None;
            for record in MergeIter::new(sources) {
                let (key, Found::Value(val)) = record? else {
                    continue;
                };
                let open = if let Some(w) = &mut writer {
                    w
                } else {
                    writer.insert(RunWriter::open(&self.runs, &self.key, Some(space))?)
                };
                open.push(&key, Some(&val))?;
                if open.bytes_written() >= target {
                    pieces.push(RunFile::new(&self.runs, writer.take().unwrap().finish()?));
                }
            }
            if let Some(open) = writer {
                pieces.push(RunFile::new(&self.runs, open.finish()?));
            }
            Ok(())
        })();
        res.inspect_err(|_| {
            for p in &pieces {
                p.retire();
            }
        })
        .map(|()| Some(pieces))
    }
}
