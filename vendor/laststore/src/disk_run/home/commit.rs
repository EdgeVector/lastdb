//! Commits (one level-0 run each) and level merges.

use super::keys::run_key;
use super::manifest::{RunFile, RunMeta, MAX_LEVELS};
use super::RunHome;
use crate::disk_pack::home::HomeMutation;
use crate::disk_run::{merge_runs, RunReader, RunSummary, RunWriter};
use crate::{Error, Result};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// A level with this many runs is merged into one run on the next level.
pub const MERGE_WIDTH: usize = 8;

/// One published commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommit {
    /// Generation of the CURRENT this commit published.
    pub generation: u64,
    /// Complete commits, this one included.
    pub frontier: u64,
    /// Records in the new run, after later duplicates replaced earlier ones.
    pub records: u64,
    /// Size of the new run file.
    pub run_bytes: u64,
}

/// One published merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMerge {
    /// Generation of the CURRENT this merge published.
    pub generation: u64,
    /// The level whose runs were merged.
    pub level: usize,
    /// Runs merged.
    pub inputs: usize,
    /// Records in the output run; zero when every input record was a
    /// dropped tombstone and no output run exists.
    pub records: u64,
    /// Size of the output run file.
    pub run_bytes: u64,
    /// True when the output is the deepest level, so tombstones were dropped.
    pub dropped_tombstones: bool,
}

fn meta(summary: RunSummary) -> Result<RunMeta> {
    match (summary.first_key, summary.last_key) {
        (Some(first_key), Some(last_key)) => Ok(RunMeta {
            run_id: summary.run_id,
            records: summary.records,
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
    /// Write every operation into one new run, then publish it in a new
    /// CURRENT. A read sees all of the commit or none of it. When two
    /// operations name the same key, the later one wins.
    ///
    /// This call does not ack a logical write; the caller acks after it
    /// returns.
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
        // Stable sort of the reversed list: for equal keys the last
        // operation comes first, and dedup keeps the first of each key.
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records.dedup_by(|later, first| later.0 == first.0);
        let mut writer = RunWriter::create(&self.runs, &self.key, self.options)?;
        for (key, body) in &records {
            writer.push(key, body.as_deref())?;
        }
        // A run left by a failed finish is not in CURRENT; the next open
        // removes it.
        let run = meta(writer.finish()?)?;
        let records = run.records;
        let run_bytes = run.file_bytes;
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        next.frontier = next.frontier.checked_add(1).ok_or_else(overflow)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, RunFile::new(&self.runs, run));
        let installed = self.install(next)?;
        Ok(RunCommit {
            generation: installed.generation,
            frontier: installed.frontier,
            records,
            run_bytes,
        })
    }

    /// Merge the first full level (at least [`MERGE_WIDTH`] runs) into one
    /// run at the next level. Returns `None` when no level is full.
    ///
    /// Commits continue during the merge; only the final publish waits for
    /// a commit's publish. Reads that hold an older CURRENT keep the input
    /// files until they end.
    pub fn merge_step(&self) -> Result<Option<RunMerge>> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        let Some(level) = held.levels.iter().position(|l| l.len() >= MERGE_WIDTH) else {
            return Ok(None);
        };
        if level + 1 >= MAX_LEVELS {
            return Err(Error::WriteCapacity(
                "disk run home: no level is left for a merge".into(),
            ));
        }
        let inputs: Vec<Arc<RunFile>> = held.levels[level].clone();
        // Only merges add runs below level 0, and merges run one at a time,
        // so this stays true until the publish below.
        let deepest = held.levels.iter().skip(level + 1).all(|l| l.is_empty());
        drop(held);
        let readers = inputs
            .iter()
            .map(|run| RunReader::open(&run.path, &self.key))
            .collect::<Result<Vec<_>>>()?;
        let mut writer = RunWriter::create(&self.runs, &self.key, self.options)?;
        let sources = readers.iter().map(|reader| reader.iter()).collect();
        let written = merge_runs(sources, &mut writer, !deepest)?;
        let output = match written {
            0 => None,
            _ => Some(meta(writer.finish()?)?),
        };
        let (records, run_bytes) = output
            .as_ref()
            .map_or((0, 0), |run| (run.records, run.file_bytes));
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|run| run.meta.run_id).collect();
        next.levels[level].retain(|run| !merged.contains(&run.meta.run_id));
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            // Every input is newer than every run already on the next level.
            next.levels[level + 1].insert(0, RunFile::new(&self.runs, run));
        }
        while next.levels.last().is_some_and(|l| l.is_empty()) {
            next.levels.pop();
        }
        let installed = self.install(next)?;
        for run in &inputs {
            run.retire();
        }
        Ok(Some(RunMerge {
            generation: installed.generation,
            level,
            inputs: inputs.len(),
            records,
            run_bytes,
            dropped_tombstones: deepest,
        }))
    }
}

fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
}
