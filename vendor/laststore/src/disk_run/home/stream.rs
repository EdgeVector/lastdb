//! Run key encoding, bounded backup value reads, and bounded range cursor.

use super::range::RunRangePage;
use super::read::RunRead;
use crate::disk_run::codec::corrupt;
use crate::disk_run::reader::stream::SelectedBody;
use crate::disk_run::{RunReader, MAX_RUN_KEY_BYTES};
use crate::{Error, Result};

pub(in crate::disk_run::home) fn run_key(collection: &str, key: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(collection.len())
        .map_err(|_| Error::Config("disk run home: collection name too long".into()))?;
    let total = 2 + collection.len() + key.len();
    if total > MAX_RUN_KEY_BYTES {
        return Err(Error::Config(
            "disk run home: storage key exceeds the run key limit".into(),
        ));
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(collection.as_bytes());
    out.extend_from_slice(key);
    Ok(out)
}

pub(in crate::disk_run::home) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

pub(in crate::disk_run::home) fn storage_key(collection: &str, run_key: &[u8]) -> Result<String> {
    let at = 2 + collection.len();
    if run_key.len() < at
        || run_key[..2] != (collection.len() as u16).to_be_bytes()
        || &run_key[2..at] != collection.as_bytes()
    {
        return Err(corrupt("run key outside its collection"));
    }
    String::from_utf8(run_key[at..].to_vec()).map_err(|_| corrupt("storage key is not UTF-8"))
}

impl RunRead {
    fn stream_admission(&self, extra: usize) -> Result<u64> {
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(self.shared_metadata_bytes.max(self.retained_bytes()))
            .and_then(|n| n.checked_sub(self.costs.budget_bytes))
            .and_then(|n| n.checked_sub(extra as u64))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(logical
            .min(global)
            .min(lastdb_model::storage_policy::BACKUP_RESOURCES.decoded_values_bytes))
    }

    fn compound_key(&self, collection: &str, key: &[u8]) -> Result<Vec<u8>> {
        let length = collection
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(2))
            .ok_or(Error::ReadBudgetExceeded)?;
        let buffers = 2 * lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let check = |n: usize| {
            let total = (n as u64)
                .checked_add(buffers)
                .ok_or(Error::ReadBudgetExceeded)?;
            self.stream_admission(usize::try_from(total).map_err(|_| Error::ReadBudgetExceeded)?)
        };
        check(length)?;
        let wanted = run_key(collection, key)?;
        check(wanted.capacity())?;
        Ok(wanted)
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

    pub(in crate::disk_run::home) fn streamed_value(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.examine(1)?;
        self.charge((collection.len() + key.len()) as u64)?;
        let wanted = self.compound_key(collection, key.as_bytes())?;
        let admission = self.stream_admission(wanted.len())?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            let (reader, reserved) =
                self.open_stream_reader(slot, admission, wanted.len() as u64)?;
            if let Some(body) = reader.stream_body(&wanted, admission, reserved)? {
                return match body {
                    SelectedBody::Deleted => Ok(None),
                    SelectedBody::Value(body) => {
                        let limit = maximum.min(admission).min(body.memory_available);
                        let value = body.into_vec(limit)?;
                        self.charge(value.len() as u64)?;
                        Ok(Some(value))
                    }
                };
            }
        }
        Ok(None)
    }

    pub(in crate::disk_run::home) fn streamed_next_key(
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

pub(in crate::disk_run::home) struct BoundedRange<'a> {
    owner: &'a mut RunRead,
    collection: String,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    exclusive: bool,
    exhausted: bool,
}

impl<'a> BoundedRange<'a> {
    pub(in crate::disk_run::home) fn new(
        owner: &'a mut RunRead,
        collection: &str,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        exclusive: bool,
    ) -> Self {
        Self {
            owner,
            collection: collection.into(),
            lower,
            upper,
            exclusive,
            exhausted: false,
        }
    }

    pub(in crate::disk_run::home) fn account(&mut self, key: usize, body: usize) -> Result<()> {
        self.owner.account_memory_record(key, body)
    }

    pub(in crate::disk_run::home) fn page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
        maximum: u64,
    ) -> Result<RunRangePage> {
        let mut records = Vec::new();
        let mut last = false;
        let mut resume = None;
        while !self.exhausted && records.len() < limit {
            if let Some((held, keys)) = yielding {
                let rows = held + records.len();
                let needed = keys.saturating_mul(rows).saturating_add(16) as u64;
                if rows > 0
                    && keys > 0
                    && self.owner.costs.examined_keys.saturating_add(needed)
                        > self.owner.budget.keys
                    && last
                {
                    resume = Some(self.resume_key()?);
                    break;
                }
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
            let value = self.value(&key, maximum)?;
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

    fn value(&mut self, key: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        let before = self.owner.shared_metadata_bytes;
        self.owner.shared_metadata_bytes = before
            .max(self.owner.retained_bytes())
            .checked_add(self.coordinate_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let value = self.owner.streamed_value(&self.collection, key, maximum);
        self.owner.shared_metadata_bytes = before;
        value
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
