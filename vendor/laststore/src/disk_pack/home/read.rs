use super::{HomeDescriptor, ReadCall};
use crate::disk_pack::{Current, HashRangeCursor, HashScope, RangeBounds, ReadBudget, ReadCosts};
use crate::Result;

/// One native read call. All exact, batch, and range work shares its captured root.
pub struct HomeRead {
    descriptor: HomeDescriptor,
    call: ReadCall,
}

impl HomeRead {
    pub(super) fn new(descriptor: HomeDescriptor, call: ReadCall) -> Self {
        Self { descriptor, call }
    }

    /// The immutable generation selected at call start.
    pub fn current(&self) -> &Current {
        self.call.current()
    }

    /// Fixed limits for this call, including the separately admitted write mode.
    pub fn budget(&self) -> ReadBudget {
        self.call.budget
    }

    /// Aggregate costs, including work before individual item errors.
    pub fn costs(&self) -> ReadCosts {
        self.call.costs()
    }

    /// Charge a memory or pending-write result before its body is copied.
    /// It consumes the same key/byte budget without reporting nonexistent file reads.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        self.call.examine()?;
        let bytes = key_bytes
            .checked_add(body_bytes)
            .ok_or_else(super::super::resource)?;
        self.call.charge(bytes as u64)
    }

    /// Resolve a complete storage id with the held placement codec.
    pub fn get(&mut self, collection: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.call.charge((collection.len() + key.len()) as u64)?;
        self.call.get(collection, self.descriptor.group(key)?, key)
    }

    /// Reject a body length at the located record before block planning or allocation.
    pub fn get_bounded(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.call.charge((collection.len() + key.len()) as u64)?;
        self.call
            .get_bounded(collection, self.descriptor.group(key)?, key, maximum)
    }

    /// Locate every exact key first and enforce each body cap before payload I/O.
    pub fn get_many_bounded(
        &mut self,
        coordinates: &[(String, String)],
        maximum: u64,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        if coordinates.len() > 10_000 {
            return Err(super::super::resource());
        }
        self.call.batch_get_bounded(
            coordinates.iter().map(|(collection, key)| {
                (
                    collection.as_str(),
                    self.descriptor.group(key),
                    key.as_str(),
                )
            }),
            Some(maximum),
        )
    }

    /// Preserve input slots and individual errors without resetting any budget.
    /// All coordinates are collected before the caller invokes this batch.
    pub fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        if coordinates.len() > 10_000 {
            return Err(super::super::resource());
        }
        self.call
            .batch_get(coordinates.iter().map(|(collection, key)| {
                (
                    collection.as_str(),
                    self.descriptor.group(key),
                    key.as_str(),
                )
            }))
    }

    /// Seek one complete encoded hash; derive every configured group locally.
    pub fn hash_range(
        &mut self,
        collection: &str,
        prefix: &str,
        bounds: RangeBounds,
    ) -> Result<HashRangeCursor<'_>> {
        let groups = self.descriptor.groups(prefix)?;
        self.call.hash_range(
            HashScope::new(collection.to_string(), prefix.to_string(), groups)?,
            bounds,
        )
    }

    /// Bounded internal metadata range across this collection's exact 1024 directory coordinates.
    /// Node bootstrap and explicit maintenance use this; user molecule reads use `hash_range`.
    /// No directory walk or group replay occurs. All work shares this call's limits.
    pub fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<HashRangeCursor<'_>> {
        if start.len() > super::super::KEY_BYTES || end.len() > super::super::KEY_BYTES {
            return Err(super::super::corrupt(
                "administrative bound exceeds key limit",
            ));
        }
        self.call
            .charge((collection.len() + start.len() + end.len() + 2048) as u64)?;
        let groups: Vec<u16> = (0..1024).collect();
        self.call.range_in_groups(
            collection.to_string(),
            start.to_vec(),
            end.to_vec(),
            &groups,
        )
    }

    /// Explicit internal inventory of one native group. A continuation reopens
    /// only this group, rather than seeking every group for each returned row.
    pub fn administrative_group_range(
        &mut self,
        collection: &str,
        group: u16,
        start: &[u8],
        end: &[u8],
    ) -> Result<HashRangeCursor<'_>> {
        if group >= 1024
            || start.len() > super::super::KEY_BYTES
            || end.len() > super::super::KEY_BYTES
        {
            return Err(super::super::corrupt("administrative group or bound"));
        }
        self.call
            .charge((collection.len() + start.len() + end.len() + 2) as u64)?;
        self.call.range_in_groups(
            collection.to_string(),
            start.to_vec(),
            end.to_vec(),
            &[group],
        )
    }
}
