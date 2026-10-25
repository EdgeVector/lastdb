//! Reads records from one run file by key.

use super::bloom::{key_hash, Filter};
use super::codec::{corrupt, decode_block};
use super::footer::{get_refs, BlockRef, Footer, Top, FOOTER_BYTES};
use super::range::RunRange;
use super::{Found, INDEX_COUNTER_BASE, TOP_COUNTER};
use crate::frame::{decode_frame_bounded, MAX_DECOMPRESSED_FRAME_BYTES};
use crate::Result;
use std::fs::File;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use uuid::Uuid;

/// One open run for the length of one call. It holds the file, the footer
/// and the top block; index and data blocks are read when a lookup needs
/// them and dropped after.
pub struct RunReader {
    file: File,
    data_key: [u8; 32],
    footer: Footer,
    top: Top,
}

/// One decoded index block.
pub(super) struct IndexBlock {
    pub(super) blocks: Vec<BlockRef>,
    filter_bytes: Vec<u8>,
}

impl IndexBlock {
    fn may_hold(&self, hash: u64) -> Result<bool> {
        let mut at = 0;
        Ok(Filter::read(&self.filter_bytes, &mut at)?.may_hold(hash))
    }
}

impl RunReader {
    /// Opens a run and checks its footer and top block.
    pub fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let mut raw = [0u8; FOOTER_BYTES];
        file.read_exact_at(&mut raw, len - FOOTER_BYTES as u64)?;
        let footer = Footer::decode(&raw)?;
        if footer.top_offset + u64::from(footer.top_len) + FOOTER_BYTES as u64 != len {
            return Err(corrupt("top block is not before the footer"));
        }
        let mut reader = Self {
            file,
            data_key: *data_key,
            footer,
            top: Top {
                records: 0,
                data_blocks: 0,
                max_payload: 0,
                indexes: Vec::new(),
                last_key: Vec::new(),
            },
        };
        let payload = reader.read_frame(
            footer.top_offset,
            footer.top_len,
            TOP_COUNTER,
            MAX_DECOMPRESSED_FRAME_BYTES,
        )?;
        reader.top = Top::decode(&payload)?;
        if !reader.top.agrees_with(&footer) {
            return Err(corrupt("footer and top block disagree"));
        }
        Ok(reader)
    }

    /// Run id from the footer.
    pub fn run_id(&self) -> Uuid {
        self.footer.run_id
    }

    /// Records in the run, tombstones included.
    pub fn records(&self) -> u64 {
        self.footer.records
    }

    /// Largest key, or `None` for an empty run.
    pub fn last_key(&self) -> Option<&[u8]> {
        (self.footer.records > 0).then_some(self.top.last_key.as_slice())
    }

    /// Smallest key, or `None` for an empty run.
    pub fn first_key(&self) -> Option<&[u8]> {
        self.top.indexes.first().map(|i| i.first_key.as_slice())
    }

    fn read_frame(&self, offset: u64, len: u32, counter: u64, max: usize) -> Result<Vec<u8>> {
        let mut frame = vec![0u8; len as usize];
        self.file.read_exact_at(&mut frame, offset)?;
        let decoded = decode_frame_bounded(&self.data_key, &frame, max)?;
        if decoded.header.chunk_uuid != self.footer.run_id || decoded.header.counter != counter {
            return Err(corrupt("block belongs to another place"));
        }
        Ok(decoded.payload)
    }

    fn block_limit(&self) -> usize {
        self.footer.max_payload as usize
    }

    pub(super) fn index_count(&self) -> usize {
        self.top.indexes.len()
    }

    pub(super) fn read_index(&self, number: usize) -> Result<IndexBlock> {
        let entry = &self.top.indexes[number];
        let payload = self.read_frame(
            entry.offset,
            entry.len,
            INDEX_COUNTER_BASE + entry.number,
            self.block_limit(),
        )?;
        let mut at = 0;
        let blocks = get_refs(&payload, &mut at)?;
        if blocks.first().map(|b| &b.first_key) != Some(&entry.first_key) {
            return Err(corrupt("index block does not start at its top entry"));
        }
        let filter_start = at;
        Filter::read(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("trailing bytes in index block"));
        }
        Ok(IndexBlock {
            blocks,
            filter_bytes: payload[filter_start..].to_vec(),
        })
    }

    pub(super) fn read_data(&self, block: &BlockRef) -> Result<Vec<(Vec<u8>, Found)>> {
        let payload = self.read_frame(block.offset, block.len, block.number, self.block_limit())?;
        let records = decode_block(&payload)?;
        if records.first().map(|(k, _)| k) != Some(&block.first_key) {
            return Err(corrupt("data block does not start at its index entry"));
        }
        Ok(records)
    }

    /// The index block whose range holds `key`, if any.
    pub(super) fn index_for(&self, key: &[u8]) -> Option<usize> {
        let n = self
            .top
            .indexes
            .partition_point(|i| i.first_key.as_slice() <= key);
        n.checked_sub(1)
    }

    /// Looks up one key. `None` means this run does not hold the key; a
    /// newer run can still hide an older value with [`Found::Tombstone`].
    pub fn get(&self, key: &[u8]) -> Result<Option<Found>> {
        let Some(index) = self.index_for(key) else {
            return Ok(None);
        };
        let index = self.read_index(index)?;
        lookup_in(self, &index, key)
    }

    /// Looks up many keys with one read of each index and data block they
    /// need. Results come back in the order of `keys`.
    pub fn get_many(&self, keys: &[&[u8]]) -> Result<Vec<Option<Found>>> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let mut out = vec![None; keys.len()];
        let mut index: Option<(usize, IndexBlock)> = None;
        let mut data: Option<(u64, Vec<(Vec<u8>, Found)>)> = None;
        for slot in order {
            let key = keys[slot];
            let Some(number) = self.index_for(key) else {
                continue;
            };
            if index.as_ref().map(|(n, _)| *n) != Some(number) {
                index = Some((number, self.read_index(number)?));
            }
            let (_, block_index) = index.as_ref().expect("index loaded");
            if !block_index.may_hold(key_hash(key))? {
                continue;
            }
            let Some(block) = block_for(block_index, key) else {
                continue;
            };
            if data.as_ref().map(|(n, _)| *n) != Some(block.number) {
                data = Some((block.number, self.read_data(block)?));
            }
            let (_, records) = data.as_ref().expect("block loaded");
            out[slot] = find(records, key);
        }
        Ok(out)
    }

    /// Records with keys inside the bounds, in key order.
    pub fn range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> RunRange<'_> {
        RunRange::new(self, lower, upper)
    }

    /// Every record of the run, in key order.
    pub fn iter(&self) -> RunRange<'_> {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }
}

fn lookup_in(reader: &RunReader, index: &IndexBlock, key: &[u8]) -> Result<Option<Found>> {
    if !index.may_hold(key_hash(key))? {
        return Ok(None);
    }
    let Some(block) = block_for(index, key) else {
        return Ok(None);
    };
    Ok(find(&reader.read_data(block)?, key))
}

pub(super) fn block_for<'a>(index: &'a IndexBlock, key: &[u8]) -> Option<&'a BlockRef> {
    let n = index
        .blocks
        .partition_point(|b| b.first_key.as_slice() <= key);
    n.checked_sub(1).map(|n| &index.blocks[n])
}

fn find(records: &[(Vec<u8>, Found)], key: &[u8]) -> Option<Found> {
    records
        .binary_search_by(|(k, _)| k.as_slice().cmp(key))
        .ok()
        .map(|n| records[n].1.clone())
}
