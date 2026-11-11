//! Merge planning, the stale byte count, and the base merge.

use super::commit::{overflow, MAX_MERGE_INPUTS, MERGE_WIDTH};
use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::disk_run::space::MergeSpace;
use crate::disk_run::RunReader;
use crate::{space::SpacePolicy, Result};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

const TIER_SHARE: u64 = 16;
const STALE_SHARE: u64 = 8;
const MIN_BASE_MERGE_BYTES: u64 = 1 << 20;
const MIN_STALE_MERGE_BYTES: u64 = 64 << 10;
const BASE_PIECES: u64 = 64;
const MIN_PIECE_BYTES: u64 = 256 << 10;
const MAX_PIECE_BYTES: u64 = 1 << 30;
const COUNT_KEYS: usize = 4096;

pub(in crate::disk_run::home) enum Plan {
    Tiered {
        level: usize,
        inputs: Vec<Arc<RunFile>>,
    },
    Base(Option<Vec<Arc<RunFile>>>),
}

pub(in crate::disk_run::home) fn plan(held: &Manifest) -> Option<Plan> {
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
    let over = |bytes: u64, share: u64, minimum: u64| {
        bytes > minimum && bytes.saturating_mul(share) > total
    };
    if !over(tier, TIER_SHARE, MIN_BASE_MERGE_BYTES)
        && !over(stale, STALE_SHARE, MIN_STALE_MERGE_BYTES)
    {
        return None;
    }
    let oldest = &tiered[tiered.len().saturating_sub(MAX_MERGE_INPUTS)..];
    Some(Plan::Base(Some(oldest.iter().copied().cloned().collect())))
}

fn piece_bytes(merging: &[Arc<RunFile>], base: &[Arc<RunFile>]) -> u64 {
    let bytes: u64 = merging
        .iter()
        .chain(base)
        .map(|run| run.meta.file_bytes)
        .sum();
    (bytes / BASE_PIECES).clamp(MIN_PIECE_BYTES, MAX_PIECE_BYTES)
}

impl RunHome {
    /// True when [`RunHome::merge_step`] has work.
    pub fn needs_merge(&self) -> bool {
        let held = self.held();
        held.tiered().any(|run| run.hidden().is_none()) || plan(&held).is_some()
    }

    pub(in crate::disk_run::home) fn count_hidden(&self, held: &Manifest) -> Result<()> {
        let runs: Vec<&Arc<RunFile>> = held.newest_first().collect();
        let tiered = held.tiered().count();
        for (at, run) in runs.iter().enumerate().take(tiered) {
            if run.hidden().is_none() {
                run.set_hidden(self.hidden_bytes(run, &runs[at + 1..])?);
            }
        }
        Ok(())
    }

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
            for (slot, other) in older.iter().enumerate() {
                if other.meta.last_key < *first {
                    readers[slot] = None;
                }
            }
            let bytes = self.older_bytes(&keys, &older, &mut readers)?;
            hidden = hidden.saturating_add(bytes);
        }
    }

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
            if keys.last().is_some_and(|last| run.meta.last_key <= *last) {
                readers[slot] = None;
            }
            if open.is_empty() {
                break;
            }
        }
        Ok(bytes)
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
        let upper = match taken.last() {
            Some(run) if !last => Some(run.meta.last_key.clone()),
            _ => None,
        };
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
            while next.levels.last().is_some_and(|l| l.is_empty()) {
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
        let _installed = self.install_merge(next, &space)?;
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
