//! Logged commits on [`RunHome`]: append one frame with one ordered flush, keep the commit in
//! the memory layer, and seal the memory layer into a run in the background.
//!
//! Lock order: merge, seal, commit, publish. Only a seal changes the frozen part.

use super::commit_log::{self, CommitLog, CommitLogStats, MAX_PAYLOAD};
use super::manifest::{Manifest, RunFile};
use super::{unpoison, RunHome, LOG};
use crate::disk_run::codec::put_record;
use crate::disk_run::{Found, RunWriter};
use crate::frame::encode_frame;
use crate::{Error, Result};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

/// Most log bytes in the active part before a background seal.
const SEAL_BYTES: u64 = 8 << 20;
/// Fewest log bytes in the active part that start a background seal.
const SEAL_MIN_BYTES: u64 = 64 << 10;
/// Memory layer bytes past which a commit seals before it writes.
const MAX_MEMORY_BYTES: u64 = 256 << 20;

pub(super) type Records = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Log bytes that start a seal: a quarter of the run bytes, kept between `SEAL_MIN_BYTES` and
/// `SEAL_BYTES`. Logged commits are frames, larger than the same records in a run, so a small
/// home seals early and its log adds at most about a quarter to its disk size.
fn seal_bytes(held: &Manifest) -> u64 {
    let runs: u64 = held.newest_first().map(|r| r.meta.file_bytes).sum();
    (runs / 4).clamp(SEAL_MIN_BYTES, SEAL_BYTES)
}

/// Estimated run bytes that `tombstones` new tombstones hide: the mean file bytes of one live
/// record in the runs. A tombstone frame is small, but the record it deletes stays in a run
/// until a seal and a merge.
fn hidden_bytes(held: &Manifest, tombstones: u64) -> u64 {
    if tombstones == 0 {
        return 0;
    }
    let (bytes, live) = held.newest_first().fold((0u64, 0u64), |(b, n), r| {
        let live = r.meta.records.saturating_sub(r.meta.tombstones);
        (b.saturating_add(r.meta.file_bytes), n.saturating_add(live))
    });
    (bytes / live.max(1)).saturating_mul(tombstones)
}

fn full() -> Error {
    Error::WriteCapacity("disk run home: the commit frontier is full".into())
}

impl RunHome {
    /// True when commits go through an open commit log.
    pub(super) fn log_open(&self) -> bool {
        self.log.as_ref().is_some_and(|l| !l.is_closed())
    }

    /// Commit sorted records with unique keys through the commit log.
    pub(super) fn commit_logged(&self, records: Records) -> Result<()> {
        let mut payload = Vec::new();
        let mut prev: &[u8] = &[];
        for (key, body) in &records {
            put_record(&mut payload, prev, key, body.as_deref());
            prev = key.as_slice();
        }
        let log = self.log.as_ref().expect("an open commit log");
        if payload.len() > MAX_PAYLOAD || self.held().mem.bytes() > MAX_MEMORY_BYTES {
            let _seal = unpoison(&self.seal_mutex);
            let _commit = self.commit_lock()?;
            self.seal_now()?;
            return self.commit_run(&records);
        }
        let _commit = self.commit_lock()?;
        if log.is_closed() {
            return self.commit_run(&records);
        }
        let held = self.held();
        let ctr = held
            .frontier
            .checked_add(held.mem.commits())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(full)?;
        let frame = encode_frame(&self.key, Uuid::new_v4(), ctr, &payload)?;
        if let Err(err) = log.append(&frame, ctr) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let tombstones = records.iter().filter(|(_, body)| body.is_none()).count() as u64;
        let hidden = hidden_bytes(&held, tombstones);
        let found = records
            .into_iter()
            .map(|(key, body)| (key, body.map_or(Found::Tombstone, Found::Value)))
            .collect();
        let mut cur = self.current.write().map_err(|_| self.poisoned_lock())?;
        let mut next = (**cur).clone();
        next.mem.push(found, frame.len() as u64, hidden);
        *cur = Arc::new(next);
        Ok(())
    }

    /// True when the memory layer is large or a seal stopped half way.
    pub(super) fn wants_seal(&self) -> bool {
        let held = self.held();
        held.mem.has_frozen() || held.mem.seal_weight() >= seal_bytes(&held)
    }

    /// One background seal: switch appends to a new log file, then write the old part as a run.
    pub(super) fn seal_step(&self) -> Result<()> {
        let _seal = unpoison(&self.seal_mutex);
        if let Some(log) = self.log.as_ref().filter(|l| !l.is_closed()) {
            log.add_spare()?;
            let _commit = self.commit_lock()?;
            self.freeze(Some(log))?;
        }
        self.write_frozen()
    }

    /// Seal every logged commit. The caller holds the seal and commit locks.
    pub(super) fn seal_now(&self) -> Result<()> {
        // A frozen part left by a failed seal goes first, then the active part.
        for _ in 0..2 {
            self.freeze(None)?;
            self.write_frozen()?;
        }
        Ok(())
    }

    fn freeze(&self, switch: Option<&CommitLog>) -> Result<()> {
        let mut cur = self.current.write().map_err(|_| self.poisoned_lock())?;
        let mut next = (**cur).clone();
        if next.mem.freeze() {
            if let Some(log) = switch {
                log.switch_to_spare();
            }
            *cur = Arc::new(next);
        }
        Ok(())
    }

    /// Write the frozen part as one new newest run and publish it. The caller holds the seal
    /// lock, so the frozen part cannot change.
    fn write_frozen(&self) -> Result<()> {
        let held = self.held();
        if !held.mem.has_frozen() {
            return Ok(());
        }
        let mut writer = RunWriter::open(&self.runs_dir, &self.key, None)?;
        for (key, body) in held.mem.frozen_records() {
            writer.push(&key, body.as_deref())?;
        }
        drop(held);
        let file = RunFile::new(&self.runs_dir, writer.finish()?);
        let published = (|| {
            let _publish = self.publish_lock()?;
            let mut next = self.next_manifest(false)?;
            next.frontier = (next.frontier)
                .checked_add(next.mem.frozen_commits())
                .ok_or_else(full)?;
            if next.levels.is_empty() {
                next.levels.push(Vec::new());
            }
            next.levels[0].insert(0, Arc::clone(&file));
            let staged = next.stage(&self.root_dir, &self.key, None)?;
            self.publish_with(next, &staged, true)
        })();
        match published {
            Ok(root) => self.log.as_ref().map_or(Ok(()), |l| l.prune(root.frontier)),
            Err(err) => {
                if !self.is_poisoned() {
                    file.retire();
                }
                Err(err)
            }
        }
    }

    /// Install a staged CURRENT. The memory layer comes from the live state, without the
    /// frozen part when `sealed` says a run in `next` now holds it.
    pub(super) fn publish_with(
        &self,
        mut next: Manifest,
        staged: &Path,
        sealed: bool,
    ) -> Result<Arc<Manifest>> {
        if let Err(err) = Manifest::replace(&self.root_dir, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let mut cur = self.current.write().map_err(|_| self.poisoned_lock())?;
        next.mem = if sealed {
            cur.mem.without_frozen()
        } else {
            cur.mem.clone()
        };
        let next = Arc::new(next);
        *cur = Arc::clone(&next);
        Ok(next)
    }

    /// Seal replayed commits of a home opened without a log, then remove its log.
    pub(super) fn seal_and_drop_log(&self) -> Result<()> {
        let (_seal, _commit) = (unpoison(&self.seal_mutex), self.commit_lock()?);
        self.seal_now()?;
        commit_log::remove_dir(&self.root_dir.join(LOG))
    }

    /// Seal every logged commit into a run, so CURRENT holds the whole home.
    pub fn seal_log(&self) -> Result<()> {
        self.refuse_if_poisoned()?;
        let (_seal, _commit) = (unpoison(&self.seal_mutex), self.commit_lock()?);
        self.seal_now()
    }

    /// Seal every logged commit and remove the commit log. Later commits write their own runs.
    /// Call it at a clean stop, so a stopped home is CURRENT and its runs only.
    pub fn close_log(&self) -> Result<()> {
        let Some(log) = &self.log else {
            return Ok(());
        };
        self.refuse_if_poisoned()?;
        let (_seal, _commit) = (unpoison(&self.seal_mutex), self.commit_lock()?);
        self.seal_now()?;
        log.remove()
    }

    /// Commit log flush counts and memory layer size. `None` for a home without a log.
    pub fn commit_log_stats(&self) -> Option<CommitLogStats> {
        let log = self.log.as_ref().filter(|l| !l.is_closed())?;
        Some(log.stats(&self.held().mem))
    }
}
