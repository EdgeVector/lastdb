//! Commits (one level-0 run each), merge planning, and tiered/base compaction.

use super::manifest::{Manifest, RunFile, RunMeta, MAX_LEVELS};
use super::stream::run_key;
use super::{RunHome, RunRoot};
use crate::disk_run::space::MergeSpace;
use crate::disk_run::{merge_runs, RunReader, RunSummary, RunWriter, MAX_RUN_VALUE_BYTES};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

pub(in crate::disk_run::home) const MERGE_WIDTH: usize = 8;
pub(in crate::disk_run::home) const MAX_MERGE_INPUTS: usize = 64;
const TIER_SHARE: u64 = 16;
const STALE_SHARE: u64 = 8;
const MIN_BASE_MERGE_BYTES: u64 = 1 << 20;
const MIN_STALE_MERGE_BYTES: u64 = 64 << 10;
const BASE_PIECES: u64 = 64;
const MIN_PIECE_BYTES: u64 = 256 << 10;
const MAX_PIECE_BYTES: u64 = 1 << 30;

enum Plan {
    Tiered {
        level: usize,
        inputs: Vec<Arc<RunFile>>,
    },
    Base(Option<Vec<Arc<RunFile>>>),
}

fn plan(held: &Manifest) -> Option<Plan> {
    if let Some(level) = held.levels.iter().position(|l| l.len() >= MERGE_WIDTH) {
        let full = &held.levels[level];
        return Some(Plan::Tiered {
            level,
            inputs: full[full.len().saturating_sub(MAX_MERGE_INPUTS)..].to_vec(),
        });
    }
    if !held.merging.is_empty() {
        return Some(Plan::Base(None));
    }
    let tiered: Vec<&Arc<RunFile>> = held.tiered().collect();
    let total: u64 = held.newest_first().map(|run| run.meta.record_bytes).sum();
    let tier: u64 = tiered.iter().map(|run| run.meta.record_bytes).sum();
    let stale: u64 = tiered
        .iter()
        .map(|run| {
            run.hidden()
                .unwrap_or(0)
                .saturating_add(run.meta.tombstone_bytes)
        })
        .sum();
    let over = |b: u64, share: u64, min: u64| b > min && b.saturating_mul(share) > total;
    if !over(tier, TIER_SHARE, MIN_BASE_MERGE_BYTES)
        && !over(stale, STALE_SHARE, MIN_STALE_MERGE_BYTES)
    {
        return None;
    }
    let oldest = &tiered[tiered.len().saturating_sub(MAX_MERGE_INPUTS)..];
    Some(Plan::Base(Some(oldest.iter().copied().cloned().collect())))
}

/// Complete native stored mutation. The home derives its physical group.
#[derive(Clone)]
pub struct HomeMutation {
    /// Exact namespace identity, never a filesystem path.
    pub collection: String,
    /// Complete encoded LastStore document id.
    pub key: String,
    /// Opaque stored body, or None for a tombstone.
    pub body: Option<Vec<u8>>,
}

/// One published commit.
#[derive(Debug, Clone)]
pub struct RunCommit {
    /// The immutable native file written by this commit.
    pub run_id: Uuid,
    /// The state this commit installed.
    pub root: RunRoot,
    /// Generation of the CURRENT this commit published.
    pub generation: u64,
    /// Complete commits, this one included.
    pub frontier: u64,
    /// Records in the new run, after later duplicates replaced earlier ones.
    pub records: u64,
    /// Size of the new run file.
    pub run_bytes: u64,
}

pub(in crate::disk_run::home) fn meta(summary: RunSummary) -> Result<RunMeta> {
    let (Some(first_key), Some(last_key)) = (summary.first_key, summary.last_key) else {
        return Err(Error::Config(
            "disk run home: an empty run has no place".into(),
        ));
    };
    Ok(RunMeta {
        run_id: summary.run_id,
        records: summary.records,
        tombstones: summary.tombstones,
        record_bytes: summary.record_bytes,
        tombstone_bytes: summary.tombstone_bytes,
        file_bytes: summary.file_bytes,
        first_key,
        last_key,
    })
}

impl RunHome {
    /// True when [`RunHome::merge_step`] has work.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|run| run.hidden().is_none()) || plan(&held).is_some()
    }

    /// Write every operation into one new run, then publish it in a new CURRENT.
    pub fn commit(&self, operations: Vec<HomeMutation>) -> Result<RunCommit> {
        self.refuse_if_poisoned()?;
        if operations.is_empty() {
            return Err(Error::Config("disk run home: empty commit".into()));
        }
        let mut records = Vec::with_capacity(operations.len());
        for op in operations.into_iter().rev() {
            records.push((run_key(&op.collection, op.key.as_bytes())?, op.body));
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records.dedup_by(|later, first| later.0 == first.0);
        let _commit = self.commit_lock()?;
        let mut writer = RunWriter::create(&self.runs, &self.key)?;
        for (key, body) in &records {
            writer.push(key, body.as_deref())?;
        }
        let prepared = writer.prepare_finish()?;
        let run = meta(prepared.summary.clone())?;
        let (records, run_bytes, run_id) = (run.records, run.file_bytes, run.run_id);
        let file = RunFile::new(&self.runs, run);
        let installed = self.install_prepared(Arc::clone(&file), prepared);
        if installed.is_err() && !self.is_poisoned() {
            file.retire();
        }
        let installed = installed?;
        Ok(RunCommit {
            run_id,
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            frontier: installed.frontier,
            records,
            run_bytes,
        })
    }

    pub(in crate::disk_run::home) fn install_or_discard(
        &self,
        next: Manifest,
        fresh: &[Arc<RunFile>],
    ) -> Result<Arc<Manifest>> {
        let installed = self.install(next);
        if installed.is_err() && !self.is_poisoned() {
            for run in fresh {
                run.retire();
            }
        }
        installed
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
                return Err(Error::Config(
                    "disk run home: value exceeds the run value limit".into(),
                ));
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
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        self.count_hidden(&held)?;
        match plan(&held) {
            None => Ok(false),
            Some(Plan::Tiered { level, inputs }) => {
                self.merge_tiered(level, inputs, policy).map(|()| true)
            }
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
        let readers = inputs
            .iter()
            .map(|r| RunReader::open(&r.path, &self.key))
            .collect::<Result<Vec<_>>>()?;
        let mut writer = RunWriter::create_reserved(&self.runs, &self.key, &space)?;
        let output = match merge_runs(
            readers.iter().map(RunReader::iter).collect(),
            &mut writer,
            true,
        )? {
            0 => None,
            _ => Some(meta(writer.finish()?)?),
        };
        let input_bytes: u64 = inputs.iter().map(|run| run.meta.record_bytes).sum();
        let hidden = inputs
            .iter()
            .map(|run| run.hidden())
            .sum::<Option<u64>>()
            .map(|h| {
                h.saturating_sub(
                    input_bytes.saturating_sub(output.as_ref().map_or(0, |r| r.record_bytes)),
                )
            });
        let _publish = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|run| run.meta.run_id).collect();
        next.levels[level].retain(|run| !merged.contains(&run.meta.run_id));
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            let file = RunFile::new(&self.runs, run);
            if let Some(h) = hidden {
                file.set_hidden(h);
            }
            next.levels[level + 1].insert(0, Arc::clone(&file));
        }
        while next.levels.last().is_some_and(Vec::is_empty) {
            next.levels.pop();
        }
        self.install_merge(next, &space)?;
        for run in &inputs {
            run.retire();
        }
        self.note_retired(&inputs);
        Ok(())
    }

    pub(in crate::disk_run::home) fn base_step(
        &self,
        start: Option<Vec<Arc<RunFile>>>,
        policy: &SpacePolicy,
    ) -> Result<()> {
        let held = self.held();
        let (merging, cut) = match &start {
            Some(runs) => (runs.clone(), None),
            None => (held.merging.clone(), held.merge_cut.clone()),
        };
        let done = cut.as_ref().map_or(0, |cut| {
            held.base.partition_point(|run| run.meta.last_key <= *cut)
        });
        let bytes: u64 = merging
            .iter()
            .chain(&held.base)
            .map(|r| r.meta.file_bytes)
            .sum();
        let target = (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES);
        let (mut take, mut taken_bytes) = (0, 0);
        for run in &held.base[done..] {
            if take > 0 && taken_bytes >= target {
                break;
            }
            take += 1;
            taken_bytes += run.meta.file_bytes;
        }
        let taken = &held.base[done..done + take];
        let last = done + take == held.base.len();
        let upper = taken
            .last()
            .filter(|_| !last)
            .map(|run| run.meta.last_key.clone());
        let space = MergeSpace::new(self.path(), policy)?;
        let pieces = self.write_base_pieces(
            &merging,
            taken,
            cut.as_deref(),
            upper.as_deref(),
            target,
            &space,
        )?;
        let _publish = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        if let Some(runs) = &start {
            let moved: HashSet<Uuid> = runs.iter().map(|run| run.meta.run_id).collect();
            for level in &mut next.levels {
                level.retain(|run| !moved.contains(&run.meta.run_id));
            }
            while next.levels.last().is_some_and(Vec::is_empty) {
                next.levels.pop();
            }
            next.merging = runs.clone();
        }
        let mut base = next.base[..done].to_vec();
        base.extend(pieces.as_deref().unwrap_or(taken).iter().cloned());
        base.extend(next.base[done + take..].iter().cloned());
        next.base = base;
        if last {
            next.merging.clear();
            next.merge_cut = None;
        } else {
            next.merge_cut = upper;
        }
        self.install_merge(next, &space)?;
        if pieces.is_some() {
            for run in taken {
                run.retire();
            }
            self.note_retired(taken);
        }
        if last {
            for run in &merging {
                run.retire();
            }
            self.note_retired(&merging);
        }
        Ok(())
    }
}

pub(in crate::disk_run::home) fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
}
