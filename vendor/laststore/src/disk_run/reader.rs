//! Reads records from one run file by key.

use super::bloom::{key_hash, Filter};
use super::codec::{corrupt, decode_block, BlockRecords};
use super::footer::{get_refs_bounded, BlockRef, Footer, Top};
use super::merge::{ReaderRef, RunRange};
use super::{Found, INDEX_COUNTER_BASE};
use crate::frame::{decode_frame_bounded, min_encoded_len, DecodeScratch, PreparedFrameKey};
use crate::{Error, Result};
use std::fs::File;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use uuid::Uuid;

#[path = "reader_stream.rs"]
pub(crate) mod stream;

const MAX_MEMO_ENTRIES: usize = 64;
const MAX_MEMO_BYTES: u64 = 256 * 1024;

struct MemoEntry {
    run: Uuid,
    number: usize,
    block: Arc<IndexBlock>,
    bytes: u64,
}

pub(crate) struct MetadataMemo {
    entries: Vec<MemoEntry>,
    bytes: u64,
    maximum: u64,
}

impl MetadataMemo {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            entries: Vec::new(),
            bytes: 0,
            maximum: MAX_MEMO_BYTES.min(budget / 16),
        }
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.bytes
    }

    fn read(
        &mut self,
        reader: &RunReader,
        number: usize,
        scratch: &mut DecodeScratch,
    ) -> Result<Arc<IndexBlock>> {
        if let Some(entry) = self
            .entries
            .iter()
            .find(|e| e.run == reader.run_id() && e.number == number)
        {
            return Ok(Arc::clone(&entry.block));
        }
        let block = Arc::new(reader.read_index(number, Some(scratch))?);
        self.retain(reader.run_id(), number, &block);
        Ok(block)
    }

    fn retain(&mut self, run: Uuid, number: usize, block: &Arc<IndexBlock>) {
        let bytes = (std::mem::size_of::<IndexBlock>() + 2 * std::mem::size_of::<usize>()) as u64
            + block.footprint();
        let slots = MAX_MEMO_ENTRIES * std::mem::size_of::<MemoEntry>();
        if bytes.saturating_add(slots as u64) > self.maximum {
            return;
        }
        if self.entries.capacity() == 0 {
            if self.entries.try_reserve_exact(MAX_MEMO_ENTRIES).is_err() {
                return;
            }
            self.bytes = (self.entries.capacity() * std::mem::size_of::<MemoEntry>()) as u64;
        }
        while !self.entries.is_empty()
            && (self.entries.len() == MAX_MEMO_ENTRIES
                || self.bytes.saturating_add(bytes) > self.maximum)
        {
            self.bytes -= self.entries.remove(0).bytes;
        }
        if self.bytes.saturating_add(bytes) <= self.maximum {
            self.entries.push(MemoEntry {
                run,
                number,
                block: Arc::clone(block),
                bytes,
            });
            self.bytes += bytes;
        }
    }
}

pub(crate) struct RunReader {
    pub(super) reserved_memory: u64,
    pub(super) file: File,
    pub(super) data_key: [u8; 32],
    pub(super) frame_key: Option<Box<PreparedFrameKey>>,
    pub(super) footer: Footer,
    pub(super) top: Top,
}

pub(super) struct IndexBlock {
    pub(super) blocks: Vec<BlockRef>,
    pub(super) filter_bytes: Vec<u8>,
}

impl IndexBlock {
    pub(super) fn parse(
        payload: Vec<u8>,
        first_key: &[u8],
        available: Option<u64>,
    ) -> Result<Self> {
        let mut at = 0;
        let blocks = get_refs_bounded(&payload, &mut at, available.unwrap_or(u64::MAX))?;
        if blocks.first().map(|b| b.first_key.as_slice()) != Some(first_key) {
            return Err(corrupt("index block does not start at its top entry"));
        }
        let start = at;
        Filter::read(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("trailing bytes in index block"));
        }
        if let Some(limit) = available {
            let allocated = blocks.capacity() * std::mem::size_of::<BlockRef>()
                + blocks.iter().map(|b| b.first_key.capacity()).sum::<usize>();
            if (payload.capacity() + allocated + payload.len() - start) as u64 > limit {
                return Err(Error::ReadBudgetExceeded);
            }
        }
        Ok(Self {
            blocks,
            filter_bytes: payload[start..].to_vec(),
        })
    }

    pub(super) fn may_hold(&self, hash: u64) -> Result<bool> {
        Ok(Filter::read(&self.filter_bytes, &mut 0)?.may_hold(hash))
    }

    pub(super) fn footprint(&self) -> u64 {
        (self.blocks.capacity() * std::mem::size_of::<BlockRef>()
            + self
                .blocks
                .iter()
                .map(|b| b.first_key.capacity())
                .sum::<usize>()
            + self.filter_bytes.capacity()) as u64
    }
}

impl RunReader {
    pub(crate) fn run_id(&self) -> Uuid {
        self.footer.run_id
    }

    pub(crate) fn records(&self) -> u64 {
        self.footer.records
    }

    pub(crate) fn last_key(&self) -> Option<&[u8]> {
        (self.footer.records > 0).then_some(self.top.last_key.as_slice())
    }

    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        self.top.indexes.first().map(|i| i.first_key.as_slice())
    }

    pub(super) fn read_frame(
        &self,
        offset: u64,
        len: u32,
        counter: u64,
        max: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<Vec<u8>> {
        if len as usize > max.saturating_add(8 + min_encoded_len()) {
            return Err(corrupt("stored block length over its limit"));
        }
        let mut frame = vec![0u8; len as usize];
        self.file.read_exact_at(&mut frame, offset)?;
        self.decode_checked_frame(&frame, counter, max, scratch)
    }

    pub(super) fn decode_checked_frame(
        &self,
        frame: &[u8],
        counter: u64,
        max: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<Vec<u8>> {
        if frame.len() > max.saturating_add(8 + min_encoded_len()) {
            return Err(corrupt("stored block length over its limit"));
        }
        let decoded = match &self.frame_key {
            Some(key) => key.decode(&self.data_key, frame, max, scratch),
            None => decode_frame_bounded(&self.data_key, frame, max),
        }?;
        if decoded.header.chunk_uuid != self.footer.run_id || decoded.header.counter != counter {
            return Err(corrupt("block belongs to another place"));
        }
        Ok(decoded.payload)
    }

    pub(super) fn block_limit(&self) -> usize {
        self.footer.max_payload as usize
    }

    pub(super) fn index_count(&self) -> usize {
        self.top.indexes.len()
    }

    pub(super) fn read_index(
        &self,
        number: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<IndexBlock> {
        let entry = &self.top.indexes[number];
        let payload = self.read_frame(
            entry.offset,
            entry.len,
            INDEX_COUNTER_BASE + entry.number,
            self.block_limit(),
            scratch,
        )?;
        IndexBlock::parse(payload, &entry.first_key, None)
    }

    pub(super) fn read_data(
        &self,
        block: &BlockRef,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<BlockRecords> {
        let payload = self.read_frame(
            block.offset,
            block.len,
            block.number,
            self.block_limit(),
            scratch,
        )?;
        let records = decode_block(&payload)?;
        if records.first().map(|(k, _)| k) != Some(&block.first_key) {
            return Err(corrupt("data block does not start at its index entry"));
        }
        Ok(records)
    }

    pub(super) fn index_for(&self, key: &[u8]) -> Option<usize> {
        self.top
            .indexes
            .partition_point(|i| i.first_key.as_slice() <= key)
            .checked_sub(1)
    }

    pub(crate) fn get_many_with(
        &self,
        keys: &[&[u8]],
        metadata: &mut MetadataMemo,
    ) -> Result<Vec<Option<Found>>> {
        self.lookup_many(keys, Found::clone, metadata)
    }

    pub(crate) fn record_bytes_many(&self, keys: &[&[u8]]) -> Result<Vec<Option<u64>>> {
        let sizes = self.lookup_many(
            keys,
            |found| found.value().map_or(0, <[u8]>::len),
            &mut MetadataMemo::new(0),
        )?;
        Ok(sizes
            .into_iter()
            .zip(keys)
            .map(|(size, key)| size.map(|size| (size + key.len()) as u64))
            .collect())
    }

    fn lookup_many<T>(
        &self,
        keys: &[&[u8]],
        pick: impl Fn(&Found) -> T,
        metadata: &mut MetadataMemo,
    ) -> Result<Vec<Option<T>>> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let mut out: Vec<Option<T>> = keys.iter().map(|_| None).collect();
        let mut index: Option<(usize, Arc<IndexBlock>)> = None;
        let mut data: Option<(u64, BlockRecords)> = None;
        let mut scratch = DecodeScratch::default();
        for slot in order {
            let key = keys[slot];
            let Some(number) = self.index_for(key) else {
                continue;
            };
            if index.as_ref().map(|(n, _)| *n) != Some(number) {
                index = Some((number, metadata.read(self, number, &mut scratch)?));
            }
            let (_, block_index) = index.as_ref().expect("index loaded");
            if !block_index.may_hold(key_hash(key))? {
                continue;
            }
            let Some(block) = block_for(block_index, key) else {
                continue;
            };
            if data.as_ref().map(|(n, _)| *n) != Some(block.number) {
                data = Some((block.number, self.read_data(block, Some(&mut scratch))?));
            }
            out[slot] = find(&data.as_ref().expect("block loaded").1, key, &pick);
        }
        Ok(out)
    }

    pub(crate) fn range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> RunRange<'_> {
        RunRange::new(ReaderRef::Borrowed(self), lower, upper)
    }

    pub(crate) fn shared_range(
        self: Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RunRange<'static> {
        RunRange::new(ReaderRef::Shared(self), lower, upper)
    }

    pub(crate) fn iter(&self) -> RunRange<'_> {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }
}

fn block_for<'a>(index: &'a IndexBlock, key: &[u8]) -> Option<&'a BlockRef> {
    index
        .blocks
        .partition_point(|b| b.first_key.as_slice() <= key)
        .checked_sub(1)
        .map(|n| &index.blocks[n])
}

fn find<T>(records: &[(Vec<u8>, Found)], key: &[u8], pick: impl Fn(&Found) -> T) -> Option<T> {
    records
        .binary_search_by(|(k, _)| k.as_slice().cmp(key))
        .ok()
        .map(|n| pick(&records[n].1))
}
