//! Checked payload requirements collected before batch file reads.

use super::bytes::u32_at;
use super::call::Buffer;
use super::{corrupt, Location, ReadCall, Source, BLOCK_BYTES};
use crate::Result;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub(super) type BlockId = (Uuid, u64, u64);

pub(super) struct BlockPlan {
    pub(super) identity: BlockId,
    pub(super) path: PathBuf,
    pub(super) offset: u64,
    pub(super) stored: usize,
    pub(super) decoded: usize,
}

pub(super) fn block_id(location: &Location, ordinal: u64) -> BlockId {
    let sequence = if location.source == Source::Journal {
        location.version.batch
    } else {
        0
    };
    (location.object, sequence, ordinal)
}

impl ReadCall {
    /// Map and part checks happen before a block enters the physical read plan.
    pub(super) fn plan_block(&mut self, location: &Location, ordinal: u64) -> Result<BlockPlan> {
        let object = self.object_metadata(location)?;
        if object
            .as_ref()
            .is_some_and(|object| ordinal >= object.blocks)
        {
            return Err(corrupt("block outside published object length"));
        }
        let base = self.data_path(location);
        let map = self.map_page(&base, location.object, ordinal / super::MAP_ENTRIES)?;
        if let Some(object) = &object {
            if map.object_bounds() != Some((object.logical_bytes, object.data_parts))
                || map.generation() != object.generation
            {
                return Err(corrupt("block-map object bounds differ across pages"));
            }
        }
        let index = (ordinal % super::MAP_ENTRIES) as usize;
        if index >= map.count() {
            return Err(corrupt("block outside map"));
        }
        let entry = &map.payload()[index * 24..index * 24 + 24];
        let part = u32_at(entry, 0);
        let offset = u32_at(entry, 4);
        let stored = u32_at(entry, 8) as usize;
        let decoded = u32_at(entry, 12) as usize;
        if let Some(object) = &object {
            object.check_block(ordinal, part, decoded, map.count())?;
        }
        if u32_at(entry, 16) != 0
            || u32_at(entry, 20) != 0
            || offset < 128
            || !(81..=4192).contains(&stored)
            || !(1..=BLOCK_BYTES).contains(&decoded)
        {
            return Err(corrupt("invalid block map extent"));
        }
        let path = base.join(format!("part-{part:08x}"));
        self.verify_part(&path, location, part, ordinal)?;
        Ok(BlockPlan {
            identity: block_id(location, ordinal),
            path,
            offset: u64::from(offset),
            stored,
            decoded,
        })
    }

    pub(super) fn decode_planned(&mut self, plan: &BlockPlan, frame: &[u8]) -> Result<Arc<Buffer>> {
        if self.costs.payload_blocks >= self.budget.blocks {
            return Err(super::resource());
        }
        self.charge(plan.decoded as u64)?;
        let (object, sequence, ordinal) = plan.identity;
        let bytes = super::frame::decode(frame, object, sequence, ordinal, plan.decoded)?;
        let block = Arc::new(Buffer::from_vec(bytes));
        self.costs.payload_blocks += 1;
        self.blocks.insert(plan.identity, Arc::clone(&block));
        Ok(block)
    }
}
