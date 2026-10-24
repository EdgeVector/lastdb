//! Merge exact captured locations; never enumerate neighboring stored records.
use super::PreparedCompaction;
use crate::disk_pack::lifetime::GenerationHold;
use crate::disk_pack::snapshot::changes::{CapturedBatch, ChangeCapture};
use crate::disk_pack::snapshot::PreparedMutation;
use crate::disk_pack::{corrupt, Current};
use crate::Result;
use std::sync::Arc;

pub(in crate::disk_pack) struct MergeDelta {
    pub(in crate::disk_pack) identity: uuid::Uuid,
    pub(in crate::disk_pack) expected: Current,
    pub(in crate::disk_pack) mutations: Vec<PreparedMutation>,
    _cut: GenerationHold,
    _batches: Vec<Arc<CapturedBatch>>,
}

impl MergeDelta {
    /// A captured location can outlive a physical rewrite of its source root.
    /// Each referenced block needs at most one data part and one block-map part.
    /// Count those older inputs separately from the latest root's closure.
    pub(in crate::disk_pack) fn payload_part_bound(&self) -> u64 {
        self.mutations
            .iter()
            .filter(|mutation| mutation.location.source == crate::disk_pack::Source::Pack)
            .fold(0u64, |parts, mutation| {
                parts.saturating_add(u64::from(mutation.location.blocks).saturating_mul(2))
            })
    }
}

impl PreparedCompaction {
    pub(in crate::disk_pack) fn has_identity(&self, identity: uuid::Uuid) -> bool {
        self.identity == identity
    }

    pub(in crate::disk_pack) fn publication_base(
        &mut self,
    ) -> crate::disk_pack::publish::PublicationBase {
        crate::disk_pack::publish::PublicationBase {
            current: self.output.current.clone(),
            live: std::mem::take(&mut self.live),
            cached: self.cached_receipt.take(),
        }
    }

    pub(in crate::disk_pack) fn dependency_parts(&self) -> u64 {
        self.live.len() as u64
    }

    pub(in crate::disk_pack) fn reserve_merge(&mut self, bytes: u64) -> Result<()> {
        let total = self
            ._space
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| corrupt("compaction merge reservation overflow"))?;
        let extension = self._space.reserve_extension(&self.policy, total)?;
        self._space.absorb(extension);
        Ok(())
    }

    /// The node calls this before it acquires the next capture's write gate.
    /// Old change arrays and generation holds drop outside that gate.
    pub fn clear_merge_capture(&mut self) {
        self.merge_capture.take();
    }

    pub(in crate::disk_pack) fn require_no_merge_capture(&self) -> Result<()> {
        if self.merge_capture.is_some() {
            return Err(corrupt("clear the previous merge capture before recapture"));
        }
        Ok(())
    }

    pub(in crate::disk_pack) fn set_merge_capture(&mut self, capture: Arc<ChangeCapture>) {
        debug_assert!(self.merge_capture.is_none());
        self.merge_capture = Some(capture);
    }

    /// This reads bounded in-memory key/location metadata, with no file access.
    pub(in crate::disk_pack) fn merge_delta(
        &self,
        cut: GenerationHold,
        limit: usize,
    ) -> Result<MergeDelta> {
        self.ensure_usable()?;
        super::concurrent::ensure_newer_compatible(self.caught.current(), cut.current())?;
        if cut.root() != self.lock.root() {
            return Err(corrupt("compaction merge cut belongs to another home"));
        }
        let capture = self.merge_capture.as_ref().ok_or_else(|| {
            crate::Error::WriteCapacity("compaction_behind: exact change capture is absent".into())
        })?;
        let captured = capture.snapshot(self.caught.current(), cut.current(), limit)?;
        Ok(MergeDelta {
            identity: self.identity,
            expected: cut.current().clone(),
            mutations: captured.mutations,
            _cut: cut,
            _batches: captured.batches,
        })
    }
}
