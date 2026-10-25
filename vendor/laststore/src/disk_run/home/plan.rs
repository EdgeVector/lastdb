//! Merge planning, the stale byte count, and the base merge.
//!
//! The home keeps its size within a fixed share of the live data:
//!
//! - A tiered merge keeps the run count low: a level with [`MERGE_WIDTH`]
//!   runs becomes one run on the next level.
//! - A base merge moves the oldest tiered runs into the base, which holds
//!   each live key once and no tombstone. It starts when the tiered runs
//!   hold more than 1/[`TIER_SHARE`] of the record bytes, or when the stale
//!   bytes (older versions that newer records hide, and tombstones) are
//!   more than 1/[`STALE_SHARE`] of them.
//! - A base merge runs in steps. Each step takes the next base runs in key
//!   order and the merging runs' records in their key range, writes new base
//!   runs, publishes, and deletes the base runs it took. Only the merging
//!   runs stay until the last step.
//!
//! The bound, in record bytes (keys and values before compression), with L
//! the live bytes: outside a base merge the stale bytes are at most a
//! quarter of the total, so the home holds at most 4/3 L. A base merge adds
//! at most its merging runs (1/8 of the total when it starts) and two base
//! runs of 1/64 of the base each, so the peak is about 1.5 L. Each trigger
//! needs [`MIN_BASE_MERGE_BYTES`], so a small home can hold that much more.
//!
//! The stale bytes are counted, not estimated, so the bound holds for any
//! value sizes. Each tiered run knows the record bytes of the older versions
//! its records hide. The merge thread counts a commit's run with batch
//! lookups of its keys in the older runs; a tiered merge's output carries
//! the counts of its inputs.

use super::commit::{meta, overflow, MergeKind, RunMerge, MAX_MERGE_INPUTS, MERGE_WIDTH};
use super::manifest::{Manifest, RunFile};
use super::{RunHome, RunRoot};
use crate::disk_run::{Found, MergeIter, MergeSource, RunReader, RunSequence, RunWriter};
use crate::Result;
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
use uuid::Uuid;

/// A base merge starts when the tiered runs hold more than 1/8 of the
/// record bytes.
pub const TIER_SHARE: u64 = 8;
/// A base merge starts when the stale bytes are more than 1/4 of the record
/// bytes.
pub const STALE_SHARE: u64 = 4;
/// Neither trigger fires below this many bytes.
pub const MIN_BASE_MERGE_BYTES: u64 = 1 << 20;
/// A base run holds about 1/64 of the base, inside the bounds below. With
/// the CURRENT limit of 4096 base runs, the base can hold about 4 TiB.
const BASE_PIECES: u64 = 64;
const MIN_PIECE_BYTES: u64 = 1 << 20;
const MAX_PIECE_BYTES: u64 = 1 << 30;
/// Keys of one batch lookup when a run's hidden bytes are counted.
const COUNT_KEYS: usize = 4096;

enum Plan {
    Tiered {
        level: usize,
        inputs: Vec<Arc<RunFile>>,
    },
    /// Start a base merge of these tiered runs, or continue the base merge
    /// in progress (`None`).
    Base(Option<Vec<Arc<RunFile>>>),
}

/// The next merge, or `None`. A run without a hidden count counts as zero;
/// [`RunHome::merge_step`] counts every run first.
fn plan(held: &Manifest) -> Option<Plan> {
    if let Some(level) = held.levels.iter().position(|l| l.len() >= MERGE_WIDTH) {
        let full = &held.levels[level];
        let inputs = full[full.len().saturating_sub(MAX_MERGE_INPUTS)..].to_vec();
        return Some(Plan::Tiered { level, inputs });
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
    let over = |bytes: u64, share: u64| {
        bytes > MIN_BASE_MERGE_BYTES && bytes.saturating_mul(share) > total
    };
    if !over(tier, TIER_SHARE) && !over(stale, STALE_SHARE) {
        return None;
    }
    // The oldest tiered runs: every tiered run left is newer than them.
    let oldest = &tiered[tiered.len().saturating_sub(MAX_MERGE_INPUTS)..];
    Some(Plan::Base(Some(oldest.iter().copied().cloned().collect())))
}

/// The target size of one new base run.
fn piece_bytes(merging: &[Arc<RunFile>], base: &[Arc<RunFile>]) -> u64 {
    let bytes: u64 = merging
        .iter()
        .chain(base)
        .map(|run| run.meta.file_bytes)
        .sum();
    (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES)
}

impl RunHome {
    /// Run one merge step: a tiered merge of a full level, the start or the
    /// next step of a base merge. Returns `None` when the home needs no
    /// merge. It first counts the hidden bytes of new tiered runs.
    ///
    /// Commits continue during a step; only its publish waits for a
    /// commit's publish. Reads that hold an older CURRENT keep the input
    /// files until they end. A crash during a base merge leaves a CURRENT
    /// that names its progress, and the next step continues it.
    pub fn merge_step(&self) -> Result<Option<RunMerge>> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        // One state for the count and the plan: a run that a commit adds
        // after it waits for the next step, so the plan sees counted runs.
        let held = self.held();
        self.count_hidden(&held)?;
        let Some(plan) = plan(&held) else {
            return Ok(None);
        };
        match plan {
            Plan::Tiered { level, inputs } => self.merge_tiered(level, inputs),
            Plan::Base(start) => self.base_step(start),
        }
        .map(Some)
    }

    /// True when [`RunHome::merge_step`] has work: a new run to count, a
    /// full level, a base merge in progress, or a base merge trigger.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|run| run.hidden().is_none()) || plan(&held).is_some()
    }

    /// Count the hidden bytes of each tiered run of `held` that has no count
    /// yet. Only merges change the runs older than a tiered run, and the
    /// caller holds the merge lock.
    fn count_hidden(&self, held: &Manifest) -> Result<()> {
        let runs: Vec<&Arc<RunFile>> = held.newest_first().collect();
        let tiered = held.tiered().count();
        for (at, run) in runs.iter().enumerate().take(tiered) {
            if run.hidden().is_none() {
                run.set_hidden(self.hidden_bytes(run, &runs[at + 1..])?);
            }
        }
        Ok(())
    }

    /// Record bytes of the versions in `older` (newest first) that the
    /// records of `run` hide: for each key, its newest older version.
    fn hidden_bytes(&self, run: &RunFile, older: &[&Arc<RunFile>]) -> Result<u64> {
        let older: Vec<&Arc<RunFile>> = older
            .iter()
            .copied()
            .filter(|other| other.meta.meets(&run.meta))
            .collect();
        if older.is_empty() {
            return Ok(0);
        }
        let reader = RunReader::open(&run.path, &self.key)?;
        let mut readers: Vec<Option<RunReader>> = older.iter().map(|_| None).collect();
        let mut records = reader.iter();
        let mut hidden = 0u64;
        loop {
            let mut keys = Vec::with_capacity(COUNT_KEYS);
            for record in records.by_ref().take(COUNT_KEYS) {
                keys.push(record?.0);
            }
            let Some(first) = keys.first() else {
                return Ok(hidden);
            };
            // The keys come in key order: a run that ends before this batch
            // is not needed again.
            for (slot, other) in older.iter().enumerate() {
                if other.meta.last_key < *first {
                    readers[slot] = None;
                }
            }
            let bytes = self.older_bytes(&keys, &older, &mut readers)?;
            hidden = hidden.saturating_add(bytes);
        }
    }

    /// Record bytes of the newest version of each of `keys` (in key order)
    /// in `older`. It closes the reader of each run that ends inside the
    /// batch, so the base runs, which do not overlap, keep about one reader
    /// open.
    fn older_bytes(
        &self,
        keys: &[Vec<u8>],
        older: &[&Arc<RunFile>],
        readers: &mut [Option<RunReader>],
    ) -> Result<u64> {
        let mut open: Vec<usize> = (0..keys.len()).collect();
        let mut bytes = 0u64;
        for (slot, run) in older.iter().enumerate() {
            let wanted: Vec<usize> = open
                .iter()
                .copied()
                .filter(|&at| run.meta.covers(&keys[at]))
                .collect();
            if !wanted.is_empty() {
                if readers[slot].is_none() {
                    readers[slot] = Some(RunReader::open(&run.path, &self.key)?);
                }
                let reader = readers[slot].as_ref().expect("reader opened");
                let lookups: Vec<&[u8]> = wanted.iter().map(|&at| keys[at].as_slice()).collect();
                let mut found = HashSet::new();
                for (at, size) in wanted.into_iter().zip(reader.record_bytes_many(&lookups)?) {
                    if let Some(size) = size {
                        bytes = bytes.saturating_add(size);
                        found.insert(at);
                    }
                }
                open.retain(|at| !found.contains(at));
            }
            // Later batches hold larger keys only.
            if keys.last().is_some_and(|last| run.meta.last_key <= *last) {
                readers[slot] = None;
            }
            if open.is_empty() {
                break;
            }
        }
        Ok(bytes)
    }

    /// One step of a base merge. `start` names the tiered runs of a new
    /// base merge; `None` continues the base merge in progress.
    fn base_step(&self, start: Option<Vec<Arc<RunFile>>>) -> Result<RunMerge> {
        let held = self.held();
        let (merging, cut) = match &start {
            Some(runs) => (runs.clone(), None),
            None => (held.merging.clone(), held.merge_cut.clone()),
        };
        // Base runs through the cut are outputs of this merge; the runs
        // after it are its inputs, in key order.
        let done = cut.as_ref().map_or(0, |cut| {
            held.base.partition_point(|run| run.meta.last_key <= *cut)
        });
        let target = piece_bytes(&merging, &held.base);
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
        // The last step also takes the merging records above every base key.
        let upper = match taken.last() {
            Some(run) if !last => Some(run.meta.last_key.clone()),
            _ => None,
        };
        let pieces =
            self.write_base_pieces(&merging, taken, cut.as_deref(), upper.as_deref(), target)?;
        let records: u64 = pieces.iter().map(|run| run.meta.records).sum();
        let run_bytes: u64 = pieces.iter().map(|run| run.meta.file_bytes).sum();
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        if let Some(runs) = &start {
            let moved: HashSet<Uuid> = runs.iter().map(|run| run.meta.run_id).collect();
            for level in &mut next.levels {
                level.retain(|run| !moved.contains(&run.meta.run_id));
            }
            while next.levels.last().is_some_and(|l| l.is_empty()) {
                next.levels.pop();
            }
            next.merging = runs.clone();
        }
        // Only merges change the base, and merges run one at a time, so the
        // base is the one this step read.
        let mut base = next.base[..done].to_vec();
        base.extend(pieces.iter().cloned());
        base.extend(next.base[done + take..].iter().cloned());
        next.base = base;
        if last {
            next.merging.clear();
            next.merge_cut = None;
        } else {
            next.merge_cut = upper;
        }
        let installed = self.install_or_discard(next, &pieces)?;
        for run in taken {
            run.retire();
        }
        if last {
            for run in &merging {
                run.retire();
            }
        }
        Ok(RunMerge {
            root: RunRoot(Arc::clone(&installed)),
            generation: installed.generation,
            kind: MergeKind::Base { done: last },
            inputs: merging.len() + take,
            records,
            run_bytes,
            dropped_tombstones: true,
        })
    }

    /// Merge the records of `merging` in `(cut, upper]` with the `taken`
    /// base runs into new base runs of about `target` bytes each, without
    /// tombstones. An error deletes the runs this call wrote.
    fn write_base_pieces(
        &self,
        merging: &[Arc<RunFile>],
        taken: &[Arc<RunFile>],
        cut: Option<&[u8]>,
        upper: Option<&[u8]>,
        target: u64,
    ) -> Result<Vec<Arc<RunFile>>> {
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
        if let Err(error) = self.write_pieces(MergeIter::from_sources(sources), target, &mut pieces)
        {
            for piece in &pieces {
                piece.retire();
            }
            return Err(error);
        }
        Ok(pieces)
    }

    fn write_pieces(
        &self,
        records: MergeIter<'_>,
        target: u64,
        pieces: &mut Vec<Arc<RunFile>>,
    ) -> Result<()> {
        let mut writer: Option<RunWriter> = None;
        for record in records {
            let (key, found) = record?;
            let Found::Value(value) = found else {
                continue;
            };
            if writer.is_none() {
                writer = Some(RunWriter::create(&self.runs, &self.key, self.options)?);
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
