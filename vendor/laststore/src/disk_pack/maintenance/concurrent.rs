//! A protected immutable copy, bounded catch-up rounds, and one root publication.
use super::compact::PackOutput;
use super::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::disk_pack::lifetime::GenerationHold;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{artifact, corrupt, generation, publish, Current};
use crate::Result;
use std::sync::Arc;

/// Captured under the writer boundary. No copy or disk walk occurs at capture.
pub struct CompactionPlan {
    lock: Arc<WriteLock>,
    source: GenerationHold,
    caught: GenerationHold,
    protection: crate::disk_pack::private_files::OutputFiles,
    policy: super::CompactionPolicy,
}

/// Private complete output. Dropping it leaves only reclaimable unreferenced files.
pub struct PreparedCompaction {
    pub(super) identity: uuid::Uuid,
    pub(super) lock: Arc<WriteLock>,
    // Keep original inputs protected until this job and its private output drop.
    _source: GenerationHold,
    pub(super) caught: GenerationHold,
    pub(super) output: PackOutput,
    pub(super) protection: crate::disk_pack::private_files::OutputFiles,
    pub(super) cached_receipt: Option<generation::Generation>,
    pub(super) merge_capture: Option<Arc<crate::disk_pack::snapshot::changes::ChangeCapture>>,
    pub(super) live: crate::disk_pack::live_parts::LiveParts,
    pub(super) failed: bool,
    pub(super) _space: SpaceReservation,
    pub(super) policy: SpacePolicy,
    pub(super) off_gate_millis: u64,
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
        let protection = crate::disk_pack::private_files::OutputFiles::new(Arc::clone(&lock.files));
        let lease = crate::disk_pack::lifetime::WriterReadLease::new(Arc::clone(&lock));
        let source = GenerationHold::from_writer(&lease, lock.root(), current)?;
        let caught = GenerationHold::from_writer(&lease, lock.root(), current)?;
        Ok(Self {
            lock,
            source,
            caught,
            protection,
            policy,
        })
    }

    /// Reserve space and copy outside the write gate; the source lease protects all input.
    pub fn copy(self, policy: &SpacePolicy) -> Result<PreparedCompaction> {
        let root = self.lock.root().to_path_buf();
        let mut output =
            crate::durability::prepare_native_files(&root, || self.copy_inner(policy))?;
        output.prepared();
        Ok(output)
    }

    fn copy_inner(self, policy: &SpacePolicy) -> Result<PreparedCompaction> {
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
            super::compact::copy(&self.lock, current, &self.protection)?
        } else {
            super::tier::TierCopy::copy(root, current, boundary, self.protection.clone())?
        };
        output.receipt.tier_boundary = boundary;
        let live = crate::disk_pack::live_parts::LiveParts::bootstrap(
            root,
            &output.current,
            &output.dependencies,
        )?;

        Ok(PreparedCompaction {
            identity: uuid::Uuid::new_v4(),
            lock: self.lock,
            _source: self.source,
            caught: self.caught,
            output,
            protection: self.protection,
            cached_receipt: None,
            merge_capture: None,
            live,
            failed: false,
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
        self.ensure_usable()?;
        if cut.current() == self.caught.current() {
            return Ok(0);
        }
        let root = self.lock.root().to_path_buf();
        self.failed = true;
        let count = crate::durability::prepare_native_files(&root, || self.catch_up_inner(cut))?;
        self.failed = false;
        self.prepared();
        Ok(count)
    }

    pub(super) fn catch_up_inner(&mut self, cut: GenerationHold) -> Result<u64> {
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
        let previous = self.caught.current();
        let next = cut.current();
        ensure_newer_compatible(previous, next)?;
        if next.frontier != previous.frontier {
            self.reserve_delta(next)?;
        }
        let count = super::catch_up::run(self, cut.current(), limit)?;
        // Another compaction can replace an intermediate cut without changing
        // mutation versions. Retain the exact grafted files before its hold ends.
        for coordinate in &self.output.dependencies {
            self.protection
                .protect_path(&coordinate.path(self.lock.root())?)?;
        }
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

    /// Selected adaptive tier boundary; zero denotes a full copy.
    pub fn tier_boundary(&self) -> u64 {
        self.output.receipt.tier_boundary
    }

    /// A failed private file or barrier operation invalidates this whole job.
    pub(in crate::disk_pack) fn ensure_usable(&self) -> Result<()> {
        if self.failed {
            return Err(crate::Error::Config(
                "failed compaction must be discarded".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::disk_pack) fn belongs_to(&self, lock: &WriteLock) -> bool {
        std::ptr::eq(lock, self.lock.as_ref())
    }

    pub(super) fn prepared(&mut self) {
        self.off_gate_millis = self
            .started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
    }

    pub(in crate::disk_pack) fn receipt(&mut self) -> MaintenanceReceipt {
        self.output.receipt.copy_millis = self.off_gate_millis;
        self.output.receipt.catch_up_rounds = self.rounds;
        self.output.receipt.catch_up_records = self.changes;
        std::mem::take(&mut self.output.receipt)
    }

    pub(in crate::disk_pack) fn finish(
        self,
        lock: &WriteLock,
    ) -> Result<(Current, MaintenanceReceipt)> {
        self.ensure_usable()?;
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
    output.current.required_features |= crate::disk_pack::FEATURE_LIVE_PART_COUNTS;
    // Copy and catch-up can carry conservative inputs. Count only the final
    // reachable tree after every private output file has finished.
    let live = crate::disk_pack::live_parts::LiveParts::bootstrap(
        root,
        &output.current,
        &output.dependencies,
    )?;
    let receipt = generation::write_live(root, &output.current, &live, None)?;
    lock.files.invalidate()?;
    publish::publish_current(root, &output.current)?;
    lock.remember_checked(&output.current)?;
    output.receipt.generation = output.current.generation;
    output.receipt.parts_after = receipt.dependencies;
    Ok((output.current, output.receipt))
}

pub(super) fn ensure_newer_compatible(previous: &Current, next: &Current) -> Result<()> {
    if next == previous {
        return Ok(());
    }
    if next.frontier < previous.frontier || next.generation <= previous.generation {
        return Err(corrupt(
            "compaction catch-up root is not a newer compatible snapshot",
        ));
    }
    if next.required_features != previous.required_features {
        if previous.required_features & crate::disk_pack::FEATURE_LIVE_PART_COUNTS == 0
            && next.required_features
                == previous.required_features | crate::disk_pack::FEATURE_LIVE_PART_COUNTS
        {
            // A foreground first publication can introduce exact receipts while
            // this older-root copy runs. Recapture rather than mix its formats.
            return Err(crate::Error::WriteCapacity(
                "compaction_behind: snapshot receipt format changed; recapture the source".into(),
            ));
        }
        return Err(corrupt("compaction catch-up snapshot features changed"));
    }
    Ok(())
}
