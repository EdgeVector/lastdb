//! Memory-bounded stream value lookups and bounded range pagination for stopped homes.

use super::{RangeMode, RunHashRange, RunRangePage};
use crate::disk_run::home::read::RunRead;
use crate::disk_run::home::{run_key, storage_key};
use crate::disk_run::RunReader;
use crate::{Error, Result};

impl RunRead {
    fn admit_stream(&self, extra: u64) -> Result<(u64, u64)> {
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let reserved = self
            .reserved_metadata_bytes
            .max(self.retained_bytes())
            .checked_add(self.costs.budget_bytes)
            .and_then(|n| n.checked_add(extra))
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = res
            .working_memory_bytes
            .checked_sub(reserved)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((logical.min(global).min(res.decoded_values_bytes), reserved))
    }

    pub(super) fn open_stream(&self, slot: usize, extra: u64) -> Result<(RunReader, u64, u64)> {
        let (adm, res) = self.admit_stream(extra)?;
        Ok((
            RunReader::open_bounded(&self.runs[slot].0.path, &self.key, adm, res)?,
            adm,
            res,
        ))
    }

    pub(in crate::disk_run::home) fn streamed_value(
        &mut self,
        col: &str,
        key: &str,
        max: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.account_memory_record(col.len(), key.len())?;
        let bufs = 2 * lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let add_bufs = |n: usize| {
            (n as u64)
                .checked_add(bufs)
                .ok_or(Error::ReadBudgetExceeded)
        };
        self.admit_stream(add_bufs(col.len() + key.len() + 2)?)?;
        let wanted = run_key(col, key.as_bytes())?;
        self.admit_stream(add_bufs(wanted.capacity())?)?;
        for slot in 0..self.runs.len() {
            if self.runs[slot].0.meta.covers(&wanted) {
                let (reader, adm, res) = self.open_stream(slot, wanted.len() as u64)?;
                if let Some(hit) = reader.stream_body(&wanted, (adm, res, max))? {
                    if let Some(v) = &hit {
                        self.charge(v.len() as u64)?;
                    }
                    return Ok(hit);
                }
            }
        }
        Ok(None)
    }
}

impl RunHashRange<'_> {
    pub(super) fn bounded_page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
        maximum: u64,
    ) -> Result<RunRangePage> {
        let (mut out, mut last, mut resume) = (Vec::new(), false, None);
        while !self.exhausted && out.len() < limit {
            if self.should_yield(yielding, out.len(), last) {
                resume = Some(self.resume_key()?);
                break;
            }
            let Some(key) = self.next_bounded_key()? else {
                self.exhausted = true;
                break;
            };
            if self.read.costs.examined_keys >= self.read.budget.keys {
                if yielding.is_some() && last {
                    resume = Some(self.resume_key()?);
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            let before = self.read.reserved_metadata_bytes;
            self.read.reserved_metadata_bytes = before
                .max(self.read.retained_bytes())
                .checked_add(self.coord_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            let val = self.read.streamed_value(&self.collection, &key, maximum);
            self.read.reserved_metadata_bytes = before;
            let val = val?;
            if let RangeMode::Bounded {
                lower, exclusive, ..
            } = &mut self.mode
            {
                (*lower, *exclusive) = (run_key(&self.collection, key.as_bytes())?, true);
            }
            last = true;
            if let Some(v) = val {
                out.push((key, v));
            }
        }
        Ok(RunRangePage {
            records: out,
            exhausted: self.exhausted,
            resume,
        })
    }

    fn next_bounded_key(&self) -> Result<Option<String>> {
        let RangeMode::Bounded {
            lower,
            upper,
            exclusive,
        } = &self.mode
        else {
            unreachable!()
        };
        let mut sel: Option<Vec<u8>> = None;
        for slot in 0..self.read.runs.len() {
            let end = sel.as_deref().unwrap_or(upper);
            if !self.read.runs[slot].0.meta.overlaps(lower, Some(end)) {
                continue;
            }
            let extra = lower
                .len()
                .checked_add(end.len())
                .and_then(|n| n.checked_add(sel.as_ref().map_or(0, Vec::capacity)))
                .ok_or(Error::ReadBudgetExceeded)? as u64;
            let (reader, adm, res) = self.read.open_stream(slot, extra)?;
            if let Some(next) = reader.stream_key((lower, Some(end), *exclusive), (adm, res))? {
                sel = Some(next);
            }
        }
        sel.map(|k| storage_key(&self.collection, &k)).transpose()
    }

    fn coord_bytes(&self) -> u64 {
        if let RangeMode::Bounded { lower, upper, .. } = &self.mode {
            (self.collection.capacity() + lower.capacity() + upper.capacity()) as u64
        } else {
            0
        }
    }

    fn resume_key(&self) -> Result<String> {
        let RangeMode::Bounded { lower, .. } = &self.mode else {
            unreachable!()
        };
        let retained = self
            .read
            .reserved_metadata_bytes
            .max(self.read.retained_bytes())
            .saturating_add(self.read.costs.budget_bytes + self.coord_bytes() + lower.len() as u64);
        (retained <= lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes)
            .then(|| storage_key(&self.collection, lower))
            .ok_or(Error::ReadBudgetExceeded)?
    }
}
