//! Optional cache admission. The existing streaming path remains the fallback.
use super::{RunRead, RunReader};
use crate::disk_run::reader::stream::{ProofBlock, ProofBounds, ProofHead};
use crate::{Error, Result};

impl RunRead {
    pub(in crate::disk_run::home) fn proof_head_at(
        &mut self,
        slot: usize,
        bounds: ProofBounds<'_>,
        allowance: u64,
        extra_retained: u64,
    ) -> Result<Option<ProofHead>> {
        if !self.runs[slot].0.meta.overlaps(bounds.lower, bounds.upper) {
            return Ok(None);
        }
        let coordinates = bounds
            .lower
            .len()
            .saturating_add(bounds.upper.map_or(0, <[u8]>::len));
        let extra = extra_retained.saturating_add(coordinates as u64);
        let admission = self
            .stream_admission(usize::try_from(extra).map_err(|_| Error::ReadBudgetExceeded)?)?
            .min(allowance);
        let retained = self
            .shared_metadata_bytes
            .max(self.retained_bytes())
            .saturating_add(self.costs.budget_bytes)
            .saturating_add(extra);
        let reader = RunReader::open_bounded_with_reserve(
            &self.runs[slot].0.path,
            &self.key,
            admission,
            retained,
        )?;
        self.costs.runs_opened += 1;
        reader.proof_head(bounds, admission, retained)
    }

    pub(in crate::disk_run::home) fn cached_block_value(
        &mut self,
        block: &ProofBlock,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.examine(1)?;
        self.charge((collection.len() + key.len()) as u64)?;
        let wanted = self.compound_key(collection, key.as_bytes())?;
        let resources = lastdb_storage_policy::BACKUP_RESOURCES;
        // Keep the original stream's exact frame workspace deduction for
        // selected-body admission, even though this cache needs less.
        let available = resources
            .working_memory_bytes
            .checked_sub(self.shared_metadata_bytes.max(self.retained_bytes()))
            .and_then(|n| n.checked_sub(self.costs.budget_bytes))
            .and_then(|n| n.checked_sub(wanted.capacity() as u64))
            .and_then(|n| n.checked_sub(block.stream_workspace()))
            .ok_or(Error::ReadBudgetExceeded)?;
        let admission = self.stream_admission(wanted.len())?.min(available);
        let value = block.value(&wanted, maximum, admission)?;
        if let Some(value) = &value {
            self.charge(value.len() as u64)?;
        }
        Ok(value)
    }
}
