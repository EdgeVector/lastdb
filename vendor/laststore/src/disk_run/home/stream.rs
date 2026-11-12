//! Memory-admitted streaming range and point reads for captured and stopped homes.

use super::range::RunRangePage;
use super::read::RunRead;
use super::{run_key, storage_key};
use crate::disk_run::RunReader;
use crate::{Error, Result};

pub(super) struct BoundedRange<'a> {
    pub(super) owner: &'a mut RunRead,
    pub(super) collection: String,
    pub(super) lower: Vec<u8>,
    pub(super) upper: Option<Vec<u8>>,
    pub(super) exclusive: bool,
    pub(super) exhausted: bool,
}

pub(super) fn should_yield(
    yielding: Option<(usize, usize)>,
    records: usize,
    examined: u64,
    limit: u64,
    has_last: bool,
) -> bool {
    yielding.is_some_and(|(held, keys)| {
        let rows = held + records;
        rows > 0
            && keys > 0
            && has_last
            && examined.saturating_add(keys.saturating_mul(rows).saturating_add(16) as u64) > limit
    })
}

impl RunRead {
    pub(super) fn stream_admission(&self, extra: usize) -> Result<u64> {
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = res
            .working_memory_bytes
            .checked_sub(self.shared_metadata_bytes.max(self.retained_bytes()))
            .and_then(|n| n.checked_sub(self.costs.budget_bytes))
            .and_then(|n| n.checked_sub(extra as u64))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(logical.min(global).min(res.decoded_values_bytes))
    }

    fn open_stream_reader(
        &mut self,
        slot: usize,
        admission: u64,
        extra: u64,
    ) -> Result<(RunReader, u64)> {
        let reserved = self
            .shared_metadata_bytes
            .max(self.retained_bytes())
            .checked_add(self.costs.budget_bytes)
            .and_then(|n| n.checked_add(extra))
            .ok_or(Error::ReadBudgetExceeded)?;
        let reader = RunReader::open_bounded_with_reserve(
            &self.runs[slot].0.path,
            &self.key,
            admission,
            reserved,
        )?;
        Ok((reader, reserved))
    }

    pub(super) fn streamed_value(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.examine(1)?;
        self.charge((collection.len() + key.len()) as u64)?;
        let length = collection
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(2))
            .ok_or(Error::ReadBudgetExceeded)?;
        let buffers = 2 * lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let check = |n: usize| {
            usize::try_from(
                (n as u64)
                    .checked_add(buffers)
                    .ok_or(Error::ReadBudgetExceeded)?,
            )
            .map_err(|_| Error::ReadBudgetExceeded)
            .and_then(|t| self.stream_admission(t))
        };
        check(length)?;
        let wanted = run_key(collection, key.as_bytes())?;
        check(wanted.capacity())?;
        let admission = self.stream_admission(wanted.len())?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            let (reader, reserved) =
                self.open_stream_reader(slot, admission, wanted.len() as u64)?;
            if let Some(hit) = reader.stream_body(&wanted, admission, reserved, maximum)? {
                if let Some(ref value) = hit {
                    self.charge(value.len() as u64)?;
                }
                return Ok(hit);
            }
        }
        Ok(None)
    }

    fn streamed_next_key(
        &mut self,
        collection: &str,
        lower: &[u8],
        upper: Option<&[u8]>,
        exclusive: bool,
    ) -> Result<Option<String>> {
        let mut selected: Option<Vec<u8>> = None;
        for slot in 0..self.runs.len() {
            let end = selected.as_deref().or(upper);
            if !self.runs[slot].0.meta.overlaps(lower, end) {
                continue;
            }
            let extra = lower
                .len()
                .checked_add(end.map_or(0, <[u8]>::len))
                .and_then(|n| n.checked_add(selected.as_ref().map_or(0, Vec::capacity)))
                .ok_or(Error::ReadBudgetExceeded)?;
            let admission = self.stream_admission(extra)?;
            let (reader, reserved) = self.open_stream_reader(slot, admission, extra as u64)?;
            if let Some(next) = reader.stream_key(lower, end, exclusive, admission, reserved)? {
                selected = Some(next);
            }
        }
        selected
            .map(|key| storage_key(collection, &key))
            .transpose()
    }
}

impl BoundedRange<'_> {
    pub(super) fn page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
        maximum: u64,
    ) -> Result<RunRangePage> {
        let (mut records, mut last, mut resume) = (Vec::new(), false, None);
        while !self.exhausted && records.len() < limit {
            if should_yield(
                yielding,
                records.len(),
                self.owner.costs.examined_keys,
                self.owner.budget.keys,
                last,
            ) {
                resume = Some(self.resume_key()?);
                break;
            }
            let Some(key) = self.owner.streamed_next_key(
                &self.collection,
                &self.lower,
                self.upper.as_deref(),
                self.exclusive,
            )?
            else {
                self.exhausted = true;
                break;
            };
            if self.owner.costs.examined_keys >= self.owner.budget.keys {
                if yielding.is_some() && last {
                    resume = Some(self.resume_key()?);
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            let coord = self.coordinate_bytes();
            let before = self.owner.shared_metadata_bytes;
            self.owner.shared_metadata_bytes = before
                .max(self.owner.retained_bytes())
                .checked_add(coord)
                .ok_or(Error::ReadBudgetExceeded)?;
            let value = self.owner.streamed_value(&self.collection, &key, maximum);
            self.owner.shared_metadata_bytes = before;
            let value = value?;
            self.lower = run_key(&self.collection, key.as_bytes())?;
            self.exclusive = true;
            last = true;
            if let Some(value) = value {
                records.push((key, value));
            }
        }
        Ok(RunRangePage {
            records,
            exhausted: self.exhausted,
            resume,
        })
    }

    fn coordinate_bytes(&self) -> u64 {
        (self.collection.capacity()
            + self.lower.capacity()
            + self.upper.as_ref().map_or(0, Vec::capacity)) as u64
    }

    fn resume_key(&self) -> Result<String> {
        let retained = self
            .owner
            .shared_metadata_bytes
            .max(self.owner.retained_bytes())
            .saturating_add(self.owner.costs.budget_bytes)
            .saturating_add(self.coordinate_bytes())
            .saturating_add(self.lower.len() as u64);
        if retained > lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        storage_key(&self.collection, &self.lower)
    }
}
