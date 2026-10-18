//! Locate all requested records before bounded, ordered payload I/O.

use super::block_plan::{block_id, BlockId, BlockPlan};
use super::{Location, ReadCall};
use crate::{Error, Result};
use std::collections::BTreeMap;

struct BatchKey<'a> {
    collection: &'a str,
    group: Result<u16>,
    key: &'a str,
}

impl ReadCall {
    pub(super) fn batch_get<'a>(
        &mut self,
        coordinates: impl ExactSizeIterator<Item = (&'a str, Result<u16>, &'a str)>,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        let count = coordinates.len();
        if count > 10_000 {
            return Err(super::resource());
        }
        let bytes = count
            .checked_mul(
                std::mem::size_of::<BatchKey<'_>>()
                    + std::mem::size_of::<Result<Option<Location>>>()
                    + std::mem::size_of::<Result<Option<Vec<u8>>>>(),
            )
            .ok_or_else(super::resource)?;
        self.charge(bytes as u64)?;
        let mut keys = Vec::new();
        let mut locations = Vec::new();
        let mut out = Vec::new();
        keys.try_reserve_exact(count)
            .map_err(|_| super::resource())?;
        locations
            .try_reserve_exact(count)
            .map_err(|_| super::resource())?;
        out.try_reserve_exact(count)
            .map_err(|_| super::resource())?;
        for (collection, group, key) in coordinates {
            keys.push(BatchKey {
                collection,
                group,
                key,
            });
        }
        for key in &keys {
            locations.push(self.locate_batch_key(key));
        }
        let (plans, mut failures) = self.plan_batch_blocks(&mut locations)?;
        self.read_planned_blocks(plans, &mut failures);
        for (key, location) in keys.iter().zip(locations) {
            out.push(match location {
                Ok(Some(location)) => self
                    .finish_batch_record(key.collection, key.key, &location, &failures)
                    .map(Some),
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            });
        }
        Ok(out)
    }

    fn locate_batch_key(&mut self, key: &BatchKey<'_>) -> Result<Option<Location>> {
        self.charge((key.collection.len() + key.key.len()) as u64)?;
        let group = key.group.as_ref().map_err(super::batch_error::copy)?;
        self.locate(key.collection, *group, key.key)
    }

    pub(super) fn plan_batch_blocks(
        &mut self,
        locations: &mut [Result<Option<Location>>],
    ) -> Result<(Vec<BlockPlan>, BTreeMap<BlockId, Error>)> {
        let mut plans = BTreeMap::new();
        let mut failures = BTreeMap::new();
        for location in locations {
            if let Ok(Some(found)) = location {
                if let Err(error) = self.add_record_blocks(found, &mut plans, &mut failures) {
                    *location = Err(error);
                }
            }
        }
        self.charge((plans.len() * std::mem::size_of::<BlockPlan>()) as u64)?;
        let mut ordered = Vec::new();
        ordered
            .try_reserve_exact(plans.len())
            .map_err(|_| super::resource())?;
        ordered.extend(plans.into_values());
        ordered.sort_unstable_by(|a, b| a.path.cmp(&b.path).then(a.offset.cmp(&b.offset)));
        Ok((ordered, failures))
    }

    fn add_record_blocks(
        &mut self,
        location: &Location,
        plans: &mut BTreeMap<BlockId, BlockPlan>,
        failures: &mut BTreeMap<BlockId, Error>,
    ) -> Result<()> {
        // A single oversized record must not create an unbounded planning loop.
        if u64::from(location.blocks) > self.budget.blocks {
            return Err(super::resource());
        }
        for index in 0..location.blocks {
            let ordinal = location
                .first_block
                .checked_add(u64::from(index))
                .ok_or_else(|| super::corrupt("record block overflow"))?;
            let identity = block_id(location, ordinal);
            if self.blocks.contains_key(&identity)
                || plans.contains_key(&identity)
                || failures.contains_key(&identity)
            {
                continue;
            }
            if self.blocks.len() + plans.len() + failures.len() >= self.budget.blocks as usize {
                return Err(super::resource());
            }
            self.charge((std::mem::size_of::<BlockPlan>() + 128) as u64)?;
            match self.plan_block(location, ordinal) {
                Ok(plan) => {
                    self.charge(plan.path.as_os_str().len() as u64)?;
                    plans.insert(identity, plan);
                }
                Err(error) => {
                    failures.insert(identity, error);
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish_batch_record(
        &mut self,
        collection: &str,
        key: &str,
        location: &Location,
        failures: &BTreeMap<BlockId, Error>,
    ) -> Result<Vec<u8>> {
        for index in 0..location.blocks {
            let ordinal = location
                .first_block
                .checked_add(u64::from(index))
                .ok_or_else(|| super::corrupt("record block overflow"))?;
            if let Some(error) = failures.get(&block_id(location, ordinal)) {
                return Err(super::batch_error::copy(error));
            }
        }
        self.read_record(collection, key, location)
    }
}
