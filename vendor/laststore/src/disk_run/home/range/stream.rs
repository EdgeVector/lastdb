//! Backup cursor selects keys without retaining decoded value blocks.
use super::super::{
    keys::{run_key, storage_key},
    RunRead,
};
use super::RunRangePage;
use crate::{Error, Result};
#[path = "memo.rs"]
mod memo;
use memo::{HeadMemo, Selection};

pub(super) struct BoundedRange<'a> {
    owner: &'a mut RunRead,
    collection: String,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    exclusive: bool,
    exhausted: bool,
    memo: Option<HeadMemo>,
    memo_attempted: bool,
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
            memo: None,
            memo_attempted: false,
        }
    }
    pub(super) fn reset_page_budget(&mut self) {
        self.owner.costs.examined_keys = 0;
        self.owner.costs.budget_bytes = 0;
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
            let Some(key) = self.next_key()? else {
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
            if let Some(memo) = &mut self.memo {
                memo.consume(&self.lower);
            }
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
    fn next_key(&mut self) -> Result<Option<String>> {
        if !self.memo_attempted {
            self.memo = HeadMemo::new(
                self.owner,
                &self.lower,
                self.upper.as_deref(),
                self.coordinate_bytes(),
            );
            self.memo_attempted = true;
        }
        let retained = self.coordinate_bytes();
        if let Some(memo) = &mut self.memo {
            match memo.next(
                self.owner,
                &self.lower,
                self.upper.as_deref(),
                self.exclusive,
                retained,
            )? {
                Selection::Key(key) => return storage_key(&self.collection, &key).map(Some),
                Selection::Exhausted => return Ok(None),
                Selection::Fallback => self.memo = None,
            }
        }
        self.owner.streamed_next_key(
            &self.collection,
            &self.lower,
            self.upper.as_deref(),
            self.exclusive,
        )
    }
    fn coordinate_bytes(&self) -> u64 {
        (self.collection.capacity()
            + self.lower.capacity()
            + self.upper.as_ref().map_or(0, Vec::capacity)) as u64
    }
    fn retained_bytes(&self) -> u64 {
        self.coordinate_bytes()
            .saturating_add(self.memo.as_ref().map_or(0, HeadMemo::bytes))
    }
    fn value(&mut self, key: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        let costs = self.owner.costs;
        let value = self.reserved_value(key, maximum);
        if matches!(value, Err(Error::ReadBudgetExceeded)) && self.memo.is_some() {
            self.memo = None;
            // An optional cache must not reject a row that the stateless path
            // admits. The selected logical key still consumes one work slot.
            self.owner.costs.examined_keys = costs.examined_keys;
            self.owner.costs.budget_bytes = costs.budget_bytes;
            return self.reserved_value(key, maximum);
        }
        value
    }
    fn reserved_value(&mut self, key: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        let before = self.owner.shared_metadata_bytes;
        self.owner.shared_metadata_bytes = before
            .max(self.owner.retained_bytes())
            .checked_add(self.retained_bytes())
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
            .saturating_add(self.retained_bytes())
            .saturating_add(self.lower.len() as u64);
        if retained > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        storage_key(&self.collection, &self.lower)
    }
}
