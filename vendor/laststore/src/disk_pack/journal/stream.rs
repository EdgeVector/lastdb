use super::super::bytes::{u32_at, u64_at};
use super::super::call::Buffer;
use super::super::{
    corrupt, Current, Location, MutationVersion, ReadBudget, ReadCall, ReadCosts, Source,
};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

pub(super) struct Stream {
    pub(super) call: ReadCall,
    location: Location,
    block: Arc<Buffer>,
    block_ordinal: u64,
    last_extent: Option<(u32, u64)>,
    pub(super) position: u64,
    pub(super) limit: u64,
}

impl Stream {
    pub(super) fn new(
        root: &Path,
        mut current: Current,
        stream: Uuid,
        sequence: u64,
    ) -> Result<Self> {
        current.generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| corrupt("recovery generation overflow"))?;
        let call = ReadCall {
            root: root.to_path_buf(),
            current,
            // Recovery streams blocks instead of retaining a complete batch cache.
            // Cumulative work includes file bytes, decoded bytes, and operation copies.
            budget: ReadBudget {
                pages: 64,
                blocks: super::MAX_BATCH_BYTES.div_ceil(4096),
                bytes: super::MAX_BATCH_BYTES * 6,
                keys: super::MAX_OPERATIONS as u64,
            },
            costs: ReadCosts::default(),
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            payload_class: ReadClass::JournalReplay,
        };
        Ok(Self {
            call,
            location: Location {
                source: Source::Journal,
                version: MutationVersion {
                    batch: sequence,
                    operation: 0,
                },
                record_bytes: 96,
                object: stream,
                first_block: 0,
                first_position: 0,
                blocks: 1,
                digest: [0; 32],
            },
            block: Arc::new(Buffer::from_vec(Vec::new())),
            block_ordinal: u64::MAX,
            last_extent: None,
            position: 0,
            limit: super::MAX_BATCH_BYTES,
        })
    }

    pub(super) fn exact(&mut self, mut output: &mut [u8]) -> Result<()> {
        if self
            .position
            .checked_add(output.len() as u64)
            .map_or(true, |end| end > self.limit)
        {
            return Err(corrupt("journal field outside complete logical stream"));
        }
        while !output.is_empty() {
            let ordinal = self.position / 4096;
            if ordinal != self.block_ordinal {
                self.call.blocks.clear();
                self.block = self.call.read_block(&self.location, ordinal)?;
                self.check_extent_order(ordinal)?;
                self.block_ordinal = ordinal;
            }
            let offset = (self.position % 4096) as usize;
            if offset >= self.block.bytes.len() {
                return Err(corrupt("short journal block before declared end"));
            }
            let count = output.len().min(self.block.bytes.len() - offset);
            output[..count].copy_from_slice(&self.block.bytes[offset..offset + count]);
            output = &mut output[count..];
            self.position += count as u64;
        }
        Ok(())
    }

    fn check_extent_order(&mut self, ordinal: u64) -> Result<()> {
        let base = self.call.data_path(&self.location);
        let map = self.call.map_page(
            &base,
            self.location.object,
            ordinal / super::super::MAP_ENTRIES,
        )?;
        let offset = (ordinal % super::super::MAP_ENTRIES) as usize * 24;
        let entry = &map.payload()[offset..offset + 24];
        let part = u32_at(entry, 0);
        let start = u64::from(u32_at(entry, 4));
        let end = start + u64::from(u32_at(entry, 8));
        if self
            .last_extent
            .is_some_and(|(previous_part, previous_end)| {
                part < previous_part || (part == previous_part && start < previous_end)
            })
        {
            return Err(corrupt("journal frames overlap across map pages"));
        }
        self.last_extent = Some((part, end));
        Ok(())
    }

    pub(super) fn buffer(&mut self, length: usize) -> Result<Buffer> {
        self.call.charge(length as u64)?;
        if self
            .position
            .checked_add(length as u64)
            .map_or(true, |end| end > self.limit.saturating_sub(128))
        {
            return Err(corrupt("journal operation overlaps COMMIT"));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| super::refuse("recovery buffer admission failed"))?;
        bytes.resize(length, 0);
        let mut buffer = Buffer::from_vec(bytes);
        self.exact(&mut buffer.bytes)?;
        Ok(buffer)
    }

    pub(super) fn finish(&mut self) -> Result<u64> {
        if self.position != self.limit
            || self.block.bytes.len() != ((self.limit - 1) % 4096 + 1) as usize
        {
            return Err(corrupt(
                "COMMIT does not end at the journal object's logical end",
            ));
        }
        let blocks = self.limit.div_ceil(4096);
        if self.call.parts.values().map(|(_, end)| *end).max() != Some(blocks) {
            return Err(corrupt("journal part contains blocks after COMMIT"));
        }
        let base = self.call.data_path(&self.location);
        let last_map = (blocks - 1) / super::super::MAP_ENTRIES;
        let map = self.call.map_page(&base, self.location.object, last_map)?;
        if map.count() as u64 != (blocks - 1) % super::super::MAP_ENTRIES + 1 {
            return Err(corrupt("journal map contains entries after COMMIT"));
        }
        let map_file = base
            .join("map")
            .join(format!("part-{:08x}", last_map / 512));
        let bytes = measured::status(|| std::fs::metadata(map_file))?.len();
        if bytes != (last_map % 512 + 1) * super::super::PAGE_BYTES as u64 {
            return Err(corrupt("journal map part contains pages after COMMIT"));
        }
        let mut total = (last_map + 1) * super::super::PAGE_BYTES as u64;
        for path in self.call.parts.keys() {
            total = total
                .checked_add(measured::status(|| std::fs::metadata(path))?.len())
                .ok_or_else(|| corrupt("recovery physical byte count overflow"))?;
        }
        // Validate final adjacent map extents, including their physical part selection.
        let last = &map.payload()[(map.count() - 1) * 24..];
        if u32_at(last, 12) as usize != self.block.bytes.len() || u64_at(last, 16) != 0 {
            return Err(corrupt("journal final block map length"));
        }
        Ok(total)
    }
}
