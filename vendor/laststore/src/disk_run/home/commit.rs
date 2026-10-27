//! Commits (one level-0 run each) and tiered merges.

use super::keys::run_key;
use super::manifest::{Manifest, RunFile, RunMeta, MAX_LEVELS};
use super::{RunHome, RunRoot};
use crate::disk_run::{merge_runs, value, RunReader, RunSummary, RunWriter};
use crate::{Error, Result};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// A level with this many runs is merged into one run on the next level.
pub const MERGE_WIDTH: usize = 8;

/// Most runs one merge reads. Each input holds one open file, so a level
/// that grew while merges lagged is merged in slices of its oldest runs.
pub const MAX_MERGE_INPUTS: usize = 64;

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

/// What one merge step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeKind {
    /// The oldest runs of a full level became one run on the next level.
    Tiered {
        /// The level whose runs were merged.
        level: usize,
    },
    /// One step of a base merge: base runs and the merging runs' records in
    /// their key range became new base runs.
    Base {
        /// True at the last step, which removed the merging runs.
        done: bool,
    },
}

/// One published merge step.
#[derive(Debug, Clone)]
pub struct RunMerge {
    /// The state this step installed; its frontier is unchanged.
    pub root: RunRoot,
    /// Generation of the CURRENT this step published.
    pub generation: u64,
    /// What the step did.
    pub kind: MergeKind,
    /// Runs the step read.
    pub inputs: usize,
    /// Records in the output runs.
    pub records: u64,
    /// Size of the output run files.
    pub run_bytes: u64,
    /// True for a base step: no older run is left, so it drops tombstones.
    pub dropped_tombstones: bool,
}

pub(super) fn meta(summary: RunSummary) -> Result<RunMeta> {
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
        // The stored forms are made before the commit lock.
        for (_, body) in &mut records {
            *body = body.as_deref().map(value::encode);
        }
        let _commit = self.commit.lock().unwrap_or_else(|e| e.into_inner());
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
    pub(super) fn install_or_discard(
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

    /// Refuse a batch that [`RunHome::commit`] would refuse for its key
    /// or value sizes, before any work. A writer calls it at admission, so
    /// one oversized operation is refused alone and never fails a commit.
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

    /// Merge `inputs`, the oldest runs of `level`, into one run at the front
    /// of the next level. Tombstones stay: older runs can hold the keys.
    ///
    /// The output hides what its inputs hid, less the versions the merge
    /// dropped: for each key, the oldest input's hidden version stays hidden
    /// and the newer input versions are gone.
    ///
    /// Commits continue during the merge; only the final publish waits for
    /// a commit's publish. Reads that hold an older CURRENT keep the input
    /// files until they end.
    pub(super) fn merge_tiered(&self, level: usize, inputs: Vec<Arc<RunFile>>) -> Result<RunMerge> {
        if level + 1 >= MAX_LEVELS {
            return Err(Error::WriteCapacity(
                "disk run home: no level is left for a merge".into(),
            ));
        }
        let output = self.write_merge(&inputs, true)?;
        let (records, run_bytes) = output
            .as_ref()
            .map_or((0, 0), |run| (run.records, run.file_bytes));
        let input_bytes: u64 = inputs.iter().map(|run| run.meta.record_bytes).sum();
        let hidden = inputs
            .iter()
            .map(|run| run.hidden())
            .sum::<Option<u64>>()
            .map(|hidden| {
                let output_bytes = output.as_ref().map_or(0, |run| run.record_bytes);
                hidden.saturating_sub(input_bytes.saturating_sub(output_bytes))
            });
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        let merged: HashSet<Uuid> = inputs.iter().map(|run| run.meta.run_id).collect();
        next.levels[level].retain(|run| !merged.contains(&run.meta.run_id));
        let mut fresh = Vec::new();
        if let Some(run) = output {
            if next.levels.len() <= level + 1 {
                next.levels.push(Vec::new());
            }
            // Every input is newer than every run on the next level and
            // older than every run left on its own level.
            let file = RunFile::new(&self.runs, run);
            if let Some(hidden) = hidden {
                file.set_hidden(hidden);
            }
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
        self.note_retired(&inputs);
        Ok(RunMerge {
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            kind: MergeKind::Tiered { level },
            inputs: inputs.len(),
            records,
            run_bytes,
            dropped_tombstones: false,
        })
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

pub(super) fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
}
