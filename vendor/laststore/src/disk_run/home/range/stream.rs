//! Backup cursor selects keys without retaining decoded value blocks.
use super::super::{
    keys::{run_key, storage_key},
    RunRead,
};
use super::RunRangePage;
use crate::{Error, Result};

pub(super) struct BoundedRange<'a> {
    owner: &'a mut RunRead,
    collection: String,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    exclusive: bool,
    exhausted: bool,
}
impl<'a> BoundedRange<'a> {
    pub fn new(
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
    pub fn account(&mut self, key: usize, body: usize) -> Result<()> {
        self.owner.account_memory_record(key, body)
    }
    pub fn page(
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
            let value = self.owner.streamed_value(&self.collection, &key, maximum)?;
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
    fn resume_key(&self) -> Result<String> {
        let retained = self
            .owner
            .shared_metadata_bytes
            .max(self.owner.retained_bytes())
            .saturating_add(self.owner.costs.budget_bytes)
            .saturating_add(self.lower.capacity() as u64)
            .saturating_add(self.lower.len() as u64);
        if retained > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        storage_key(&self.collection, &self.lower)
    }
}
