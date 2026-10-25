//! Commits (one level-0 run each) and level merges.

use super::keys::run_key;
use super::manifest::{Manifest, RunFile, RunMeta, MAX_LEVELS};
use super::{RunHome, RunRoot};
use crate::disk_pack::home::HomeMutation;
use crate::disk_run::{merge_runs, RunReader, RunSummary, RunWriter};
use crate::{Error, Result};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// A level with this many runs is merged into one run on the next level.
pub const MERGE_WIDTH: usize = 8;

/// A merge into the deepest level also rewrites that level's runs when they
/// hold at most this many times the input bytes. The output then replaces
/// the whole deepest level, so tombstones and old versions go away, and the
/// write cost of each merge stays bounded.
pub const LAST_LEVEL_RATIO: u64 = 8;

/// Most runs one merge reads. Each input holds one open file, so a level
/// that grew while merges lagged is merged in slices of its oldest runs.
pub const MAX_MERGE_INPUTS: usize = 64;

/// One published commit.
#[derive(Debug, Clone)]
pub struct RunCommit {
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

/// One published merge.
#[derive(Debug, Clone)]
pub struct RunMerge {
    /// The state this merge installed; its frontier is unchanged.
    pub root: RunRoot,
    /// Generation of the CURRENT this merge published.
    pub generation: u64,
    /// The level whose runs were merged.
    pub level: usize,
    /// Runs merged: the runs taken from `level`, plus the deepest level's
    /// runs when the merge absorbed them.
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
        let _commit = self.commit.lock().unwrap_or_else(|e| e.into_inner());
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
        let file = RunFile::new(&self.runs, run);
        next.levels[0].insert(0, Arc::clone(&file));
        let installed = self.install_or_discard(next, &[file])?;
        Ok(RunCommit {
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            frontier: installed.frontier,
            records,
            run_bytes,
        })
    }

    /// Publish `next`; when the publish fails before its rename, delete
    /// the new run files that only `next` names.
    fn install_or_discard(&self, next: Manifest, fresh: &[Arc<RunFile>]) -> Result<Arc<Manifest>> {
        let installed = self.install(next);
        if installed.is_err() && !self.is_poisoned() {
            for run in fresh {
                run.retire();
            }
        }
        installed
    }

    /// Merge the first full level (at least [`MERGE_WIDTH`] runs) into one
    /// run at the next level. Returns `None` when no level is full.
    ///
    /// When the next level is the deepest one and holds at most
    /// [`LAST_LEVEL_RATIO`] times the input bytes, its runs join the merge
    /// and the output replaces them. Tombstones are dropped when the output
    /// is the whole deepest level.
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
        // Only merges change levels below level 0, and merges run one at a
        // time, so these facts stay true until the publish below.
        // The oldest runs of the level: older than the runs left on it and
        // newer than every run on the next level.
        let full = &held.levels[level];
        let mut inputs: Vec<Arc<RunFile>> =
            full[full.len().saturating_sub(MAX_MERGE_INPUTS)..].to_vec();
        let target_is_deepest = held.levels.iter().skip(level + 2).all(|l| l.is_empty());
        let target = held.levels.get(level + 1).map_or(&[][..], Vec::as_slice);
        let input_bytes: u64 = inputs.iter().map(|run| run.meta.file_bytes).sum();
        let target_bytes: u64 = target.iter().map(|run| run.meta.file_bytes).sum();
        let absorb = target_is_deepest
            && !target.is_empty()
            && inputs.len() + target.len() <= MAX_MERGE_INPUTS
            && target_bytes <= input_bytes.saturating_mul(LAST_LEVEL_RATIO);
        if absorb {
            // Older than every input, so after them in newest-first order.
            inputs.extend(target.iter().cloned());
        }
        let drop_tombstones = target_is_deepest && (absorb || target.is_empty());
        drop(held);
        let output = self.write_merge(&inputs, !drop_tombstones)?;
        let (records, run_bytes) = output
            .as_ref()
            .map_or((0, 0), |run| (run.records, run.file_bytes));
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|run| run.meta.run_id).collect();
        for runs in next.levels.iter_mut().skip(level).take(2) {
            runs.retain(|run| !merged.contains(&run.meta.run_id));
        }
        let mut fresh = Vec::new();
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            // Every input is newer than every run left on the next level.
            let file = RunFile::new(&self.runs, run);
            next.levels[level + 1].insert(0, Arc::clone(&file));
            fresh.push(file);
        }
        while next.levels.last().is_some_and(|l| l.is_empty()) {
            next.levels.pop();
        }
        let installed = self.install_or_discard(next, &fresh)?;
        for run in &inputs {
            run.retire();
        }
        Ok(Some(RunMerge {
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            level,
            inputs: inputs.len(),
            records,
            run_bytes,
            dropped_tombstones: drop_tombstones,
        }))
    }

    /// Merge `inputs` (newest first) into one new durable run, or `None`
    /// when nothing is left to write.
    fn write_merge(
        &self,
        inputs: &[Arc<RunFile>],
        keep_tombstones: bool,
    ) -> Result<Option<RunMeta>> {
        let readers = inputs
            .iter()
            .map(|run| RunReader::open(&run.path, &self.key))
            .collect::<Result<Vec<_>>>()?;
        let mut writer = RunWriter::create(&self.runs, &self.key, self.options)?;
        let sources = readers.iter().map(|reader| reader.iter()).collect();
        match merge_runs(sources, &mut writer, keep_tombstones)? {
            0 => Ok(None),
            _ => Ok(Some(meta(writer.finish()?)?)),
        }
    }
}

fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
}
