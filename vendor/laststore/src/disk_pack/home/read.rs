use super::{HomeDescriptor, ReadCall};
use crate::disk_pack::{Current, HashRangeCursor, HashScope, RangeBounds, ReadCosts};
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

    /// Aggregate costs, including work before individual item errors.
    pub fn costs(&self) -> ReadCosts {
        self.call.costs()
    }

    /// Resolve a complete storage id with the held placement codec.
    pub fn get(&mut self, collection: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.call.charge((collection.len() + key.len()) as u64)?;
        self.call.get(collection, self.descriptor.group(key)?, key)
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
            .charge((coordinates.len() * std::mem::size_of::<Result<Option<Vec<u8>>>>()) as u64)?;
        let mut out = Vec::new();
        out.try_reserve_exact(coordinates.len())
            .map_err(|_| super::super::resource())?;
        for (collection, key) in coordinates {
            out.push(self.get(collection, key));
        }
        Ok(out)
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
}
