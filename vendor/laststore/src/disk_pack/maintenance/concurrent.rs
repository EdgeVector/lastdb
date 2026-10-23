//! A protected immutable copy, bounded catch-up rounds, and one root publication.
use super::compact::PackOutput;
use super::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::disk_pack::lifetime::GenerationHold;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{artifact, corrupt, generation, publish, Current};
use crate::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct ActivePack(Arc<WriteLock>);

impl Drop for ActivePack {
    fn drop(&mut self) {
        self.0.compaction_active.store(false, Ordering::Release);
    }
}

/// Captured under the writer boundary. No copy or disk walk occurs at capture.
pub struct CompactionPlan {
    lock: Arc<WriteLock>,
    source: GenerationHold,
    caught: GenerationHold,
    active: ActivePack,
    policy: super::CompactionPolicy,
}

/// Private complete output. Dropping it leaves only reclaimable unreferenced files.
pub struct PreparedCompaction {
    pub(super) lock: Arc<WriteLock>,
    pub(super) source: GenerationHold,
    pub(super) caught: GenerationHold,
    pub(super) output: PackOutput,
    _active: ActivePack,
    _space: SpaceReservation,
    policy: SpacePolicy,
    off_gate_millis: u64,
    rounds: u64,
    changes: u64,
    started: std::time::Instant,
}

impl CompactionPlan {
    pub(in crate::disk_pack) fn capture(
        lock: Arc<WriteLock>,
        current: &Current,
        policy: super::CompactionPolicy,
    ) -> Result<Self> {
        if lock.compaction_active.swap(true, Ordering::AcqRel) {
            return Err(crate::Error::WriteCapacity(
                "compaction already active".into(),
            ));
        }
        let active = ActivePack(Arc::clone(&lock));
        let lease = crate::disk_pack::lifetime::WriterReadLease::new(Arc::clone(&lock));
        let source = GenerationHold::from_writer(&lease, lock.root(), current)?;
        let caught = GenerationHold::from_writer(&lease, lock.root(), current)?;
        Ok(Self {
            lock,
            source,
            caught,
            active,
            policy,
        })
    }

    /// Reserve space and copy outside the write gate; the source lease protects all input.
    pub fn copy(self, policy: &SpacePolicy) -> Result<PreparedCompaction> {
        let started = std::time::Instant::now();
        let root = self.lock.root();
        let current = self.source.current();
        let boundary = self.policy.boundary(root, current)?;
        let reserve = if boundary == 0 {
            super::quote::replacement_bytes(root, current)?
        } else {
            super::quote::tier_bytes(root, current, boundary)?
        };
        let space = super::space::reserve_replacement(root, policy, reserve)?;
        let mut output = if boundary == 0 {
            super::compact::copy(&self.lock, current)?
        } else {
            super::tier::TierCopy::copy(root, current, boundary)?
        };
        output.receipt.tier_boundary = boundary;

        Ok(PreparedCompaction {
            lock: self.lock,
            source: self.source,
            caught: self.caught,
            output,
            _active: self.active,
            _space: space,
            policy: policy.clone(),
            off_gate_millis: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            rounds: 0,
            changes: 0,
            started,
        })
    }
}

impl PreparedCompaction {
    /// Replay only keys whose original mutation sequence is newer than the previous cut.
    /// The hold keeps that cut's files until the complete round ends.
    pub fn catch_up(&mut self, cut: GenerationHold) -> Result<u64> {
        let count = self.catch_up_bounded(cut, u64::MAX)?;
        self.off_gate_millis = self
            .started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        Ok(count)
    }

    pub(in crate::disk_pack) fn catch_up_bounded(
        &mut self,
        cut: GenerationHold,
        limit: u64,
    ) -> Result<u64> {
        if cut.root() != self.lock.root() {
            return Err(corrupt("compaction cut belongs to another home"));
        }
        self.reserve_delta(cut.current())?;
        let count = super::catch_up::run(self, cut.current(), limit)?;
        self.caught = cut;
        self.rounds += 1;
        self.changes = self.changes.saturating_add(count);
        Ok(count)
    }

    fn reserve_delta(&mut self, cut: &Current) -> Result<()> {
        if cut == self.caught.current() {
            return Ok(());
        }
        let receipt = generation::read_receipt(self.lock.root(), cut)?;
        let mut extra = 0u64;
        for (coordinate, record) in receipt.coordinates.iter().zip(&receipt.records) {
            if matches!(coordinate.kind, 1 | 2)
                && crate::disk_pack::bytes::u64_at(record, 72) > self.caught.current().generation
            {
                extra = extra
                    .checked_add(crate::disk_pack::bytes::u64_at(record, 32).saturating_mul(32))
                    .ok_or_else(|| corrupt("catch-up space quote overflow"))?;
            }
        }
        if extra > 0 {
            let total = self
                ._space
                .bytes()
                .checked_add(extra)
                .ok_or_else(|| corrupt("catch-up reservation overflow"))?;
            let mut extension = self._space.reserve_extension(&self.policy, total)?;
            std::mem::swap(&mut extension, &mut self._space);
            self._space.absorb(extension);
        }
        Ok(())
    }

    /// Number of completed catch-up rounds, excluding the final root handoff.
    pub fn rounds(&self) -> u64 {
        self.rounds
    }

    pub(in crate::disk_pack) fn finish(
        self,
        lock: &WriteLock,
    ) -> Result<(Current, MaintenanceReceipt)> {
        if !std::ptr::eq(lock, self.lock.as_ref()) {
            return Err(corrupt("compaction belongs to another writer lease"));
        }
        let mut output = self.output;
        output.receipt.copy_millis = self.off_gate_millis;
        output.receipt.catch_up_rounds = self.rounds;
        output.receipt.catch_up_records = self.changes;
        publish_output(lock, self.caught.current(), output)
    }
}

pub(super) fn publish_output(
    lock: &WriteLock,
    previous: &Current,
    mut output: PackOutput,
) -> Result<(Current, MaintenanceReceipt)> {
    let root = lock.root();
    if artifact::current(root)? != *previous {
        return Err(corrupt("CURRENT changed before compaction handoff"));
    }
    output.current.generation = publish::fresh_generation(
        root,
        previous
            .generation
            .max(output.current.generation.saturating_sub(1)),
    )?;
    output.current.previous_generation = previous.generation;
    output.current.frontier = previous.frontier;
    output.current.published_ms = artifact::unix_ms();
    let receipt = generation::write(root, &output.current, &output.dependencies)?;
    publish::publish_current(root, &output.current)?;
    lock.remember_checked(&output.current)?;
    output.receipt.generation = output.current.generation;
    output.receipt.parts_after = receipt.dependencies;
    Ok((output.current, output.receipt))
}
