//! Tiered and incremental base compaction planning and execution for [`RunHome`].

use super::manifest::{Manifest, RunFile, MAX_LEVELS};
use super::{unpoison, RunHome};
use crate::disk_run::{merge_runs, Found, MergeIter, MergeSpace, RunRange, RunReader, RunWriter};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
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
    /// True when [`RunHome::merge_step`] has work.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|r| r.hidden().is_none()) || plan(&held).is_some()
    }

    /// Run a tiered or base merge step, first counting new hidden records.
    pub fn merge_step(&self) -> Result<bool> {
        self.merge_step_with_space(&SpacePolicy::default())
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
        let target = (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES);
        let (mut take, mut acc) = (0, 0);
        for run in &held.base[done..] {
            if take > 0 && acc >= target {
                break;
            }
            (take, acc) = (take + 1, acc + run.meta.file_bytes);
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
        if !overlaps {
            return Ok(None);
        }
        let lo = cut.map_or(Bound::Unbounded, Bound::Excluded);
        let up = upper.map_or(Bound::Unbounded, Bound::Included);
        let mut sources: Vec<_> = merging
            .iter()
            .map(|r| Ok(Arc::new(RunReader::open(&r.path, &self.key)?).range(lo, up)))
            .collect::<Result<_>>()?;
        sources.push(RunRange::sequence(
            taken.iter().map(|r| r.path.clone()).collect(),
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
                let open = match &mut writer {
                    Some(w) => w,
                    s => s.insert(RunWriter::open(&self.runs_dir, &self.key, Some(space))?),
                };
                open.push(&key, Some(&val))?;
                if open.bytes_written() >= target {
                    pieces.push(RunFile::new(
                        &self.runs_dir,
                        writer.take().unwrap().finish()?,
                    ));
                }
            }
            if let Some(open) = writer {
                pieces.push(RunFile::new(&self.runs_dir, open.finish()?));
            }
            Ok(())
        })();
        res.inspect_err(|_| pieces.iter().for_each(|p| p.retire()))
            .map(|()| Some(pieces))
    }
}
