//! Commits (one level-0 run each), merge dispatch, and tiered/base piece writers.

use super::manifest::{Manifest, RunFile, RunMeta, MAX_LEVELS};
use super::plan::{plan, Plan};
use super::stream::run_key;
use super::{RunHome, RunRoot};
use crate::disk_run::space::MergeSpace;
use crate::disk_run::{
    merge_runs, Found, MergeIter, MergeSource, RunReader, RunSequence, RunSummary, RunWriter,
};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
use uuid::Uuid;

pub(in crate::disk_run::home) const MERGE_WIDTH: usize = 8;
pub(in crate::disk_run::home) const MAX_MERGE_INPUTS: usize = 64;

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
    match (summary.first_key, summary.last_key) {
        (Some(first_key), Some(last_key)) => Ok(RunMeta {
            run_id: summary.run_id,
            records: summary.records,
            tombstones: summary.tombstones,
            record_bytes: summary.record_bytes,
            tombstone_bytes: summary.tombstone_bytes,
            file_bytes: summary.file_bytes,
            first_key,
            last_key,
        }),
        _ => Err(Error::Config(
            "disk run home: an empty run has no place".into(),
        )),
    }
}

impl RunHome {
    /// Write every operation into one new run, then publish it in a new CURRENT.
    pub fn commit(&self, operations: Vec<HomeMutation>) -> Result<RunCommit> {
        self.refuse_if_poisoned()?;
        if operations.is_empty() {
            return Err(Error::Config("disk run home: empty commit".into()));
        }
        let mut records = Vec::with_capacity(operations.len());
        for operation in operations.into_iter().rev() {
            let key = run_key(&operation.collection, operation.key.as_bytes())?;
            records.push((key, operation.body));
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records.dedup_by(|later, first| later.0 == first.0);
        let _commit = self.commit_lock()?;
        let mut writer = RunWriter::create(&self.runs, &self.key, self.options)?;
        for (key, body) in &records {
            writer.push(key, body.as_deref())?;
        }
        let prepared = writer.prepare_finish()?;
        let run = meta(prepared.summary().clone())?;
        let records = run.records;
        let run_bytes = run.file_bytes;
        let run_id = run.run_id;
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
        for operation in operations {
            run_key(&operation.collection, operation.key.as_bytes())?;
            if operation
                .body
                .as_ref()
                .is_some_and(|body| body.len() > super::MAX_VALUE_BYTES)
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
        let Some(plan) = plan(&held) else {
            return Ok(false);
        };
        match plan {
            Plan::Tiered { level, inputs } => self.merge_tiered(level, inputs, policy),
            Plan::Base(start) => self.base_step(start, policy),
        }
        .map(|()| true)
    }

    pub(in crate::disk_run::home) fn merge_tiered(
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
        let output = self.write_merge(&inputs, true, &space)?;
        let input_bytes: u64 = inputs.iter().map(|run| run.meta.record_bytes).sum();
        let hidden = inputs
            .iter()
            .map(|run| run.hidden())
            .sum::<Option<u64>>()
            .map(|hidden| {
                let output_bytes = output.as_ref().map_or(0, |run| run.record_bytes);
                hidden.saturating_sub(input_bytes.saturating_sub(output_bytes))
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
            if let Some(hidden) = hidden {
                file.set_hidden(hidden);
            }
            next.levels[level + 1].insert(0, Arc::clone(&file));
        }
        while next.levels.last().is_some_and(|l| l.is_empty()) {
            next.levels.pop();
        }
        let _installed = self.install_merge(next, &space)?;
        for run in &inputs {
            run.retire();
        }
        self.note_retired(&inputs);
        Ok(())
    }

    fn write_merge(
        &self,
        inputs: &[Arc<RunFile>],
        keep_tombstones: bool,
        space: &MergeSpace,
    ) -> Result<Option<RunMeta>> {
        let readers = inputs
            .iter()
            .map(|run| RunReader::open(&run.path, &self.key))
            .collect::<Result<Vec<_>>>()?;
        let mut writer = RunWriter::create_reserved(&self.runs, &self.key, self.options, space)?;
        let sources = readers.iter().map(|reader| reader.iter()).collect();
        match merge_runs(sources, &mut writer, keep_tombstones)? {
            0 => Ok(None),
            _ => Ok(Some(meta(writer.finish()?)?)),
        }
    }

    pub(in crate::disk_run::home) fn write_base_pieces(
        &self,
        merging: &[Arc<RunFile>],
        taken: &[Arc<RunFile>],
        cut: Option<&[u8]>,
        upper: Option<&[u8]>,
        target: u64,
        space: &MergeSpace,
    ) -> Result<Option<Vec<Arc<RunFile>>>> {
        if !merging
            .iter()
            .any(|run| run.meta.meets_merge_interval(cut, upper))
        {
            return Ok(None);
        }
        let lower = cut.map_or(Bound::Unbounded, Bound::Excluded);
        let upper = upper.map_or(Bound::Unbounded, Bound::Included);
        let mut sources = Vec::with_capacity(merging.len() + 1);
        for run in merging {
            let reader = Arc::new(RunReader::open(&run.path, &self.key)?);
            sources.push(MergeSource::Run(reader.shared_range(lower, upper)));
        }
        let paths = taken.iter().map(|run| run.path.clone()).collect();
        let sequence = RunSequence::new(paths, &self.key, Bound::Unbounded, Bound::Unbounded);
        sources.push(MergeSource::Sequence(sequence));
        let mut pieces = Vec::new();
        if let Err(error) =
            self.write_pieces(MergeIter::from_sources(sources), target, &mut pieces, space)
        {
            for piece in &pieces {
                piece.retire();
            }
            return Err(error);
        }
        Ok(Some(pieces))
    }

    fn write_pieces(
        &self,
        records: MergeIter<'_>,
        target: u64,
        pieces: &mut Vec<Arc<RunFile>>,
        space: &MergeSpace,
    ) -> Result<()> {
        let mut writer: Option<RunWriter> = None;
        for record in records {
            let (key, found) = record?;
            let Found::Value(value) = found else {
                continue;
            };
            if writer.is_none() {
                writer = Some(RunWriter::create_reserved(
                    &self.runs,
                    &self.key,
                    self.options,
                    space,
                )?);
            }
            let open = writer.as_mut().expect("writer is open");
            open.push(&key, Some(&value))?;
            if open.bytes_written() >= target {
                let finished = writer.take().expect("writer is open").finish()?;
                pieces.push(RunFile::new(&self.runs, meta(finished)?));
            }
        }
        if let Some(open) = writer {
            pieces.push(RunFile::new(&self.runs, meta(open.finish()?)?));
        }
        Ok(())
    }
}

pub(in crate::disk_run::home) fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
}
