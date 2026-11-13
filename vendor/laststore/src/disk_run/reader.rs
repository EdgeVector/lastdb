//! Reads, opens, and verifies records from one run file.

use super::bloom::{key_hash, may_hold, read_filter};
use super::codec::{corrupt, decode_block, BlockRecords};
use super::footer::{get_refs_bounded, refs_footprint, BlockRef, Footer, Top, FOOTER_BYTES};
use super::merge::RunRange;
use super::{Found, INDEX_COUNTER_BASE, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER};
use crate::frame::{decode_frame, new_cipher, DecodeScratch, MIN_ENCODED_LEN};
use crate::{Error, Result};
use aes_gcm::Aes256Gcm;
use std::fs::File;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

#[path = "reader_stream.rs"]
mod stream;

const MAX_MEMO_ENTRIES: usize = 64;
const MAX_MEMO_BYTES: u64 = 256 * 1024;
const TAIL_BYTES: usize = 512;
type MemoItem = (Uuid, usize, Arc<IndexBlock>, u64);

pub(crate) struct MetadataMemo {
    entries: Vec<MemoItem>,
    pub(crate) bytes: u64,
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

    fn read(&mut self, r: &RunReader, n: usize, s: &mut DecodeScratch) -> Result<Arc<IndexBlock>> {
        let run = r.footer.run_id;
        if let Some((_, _, b, _)) = self
            .entries
            .iter()
            .find(|(id, i, _, _)| (*id, *i) == (run, n))
        {
            return Ok(Arc::clone(b));
        }
        let block = Arc::new(r.read_index(n, Some(s), None)?);
        let item_sz = std::mem::size_of::<MemoItem>();
        let bytes = (std::mem::size_of::<IndexBlock>() + 2 * std::mem::size_of::<usize>()) as u64
            + block.footprint();
        if bytes.saturating_add((MAX_MEMO_ENTRIES * item_sz) as u64) <= self.maximum
            && (self.entries.capacity() > 0
                || self.entries.try_reserve_exact(MAX_MEMO_ENTRIES).is_ok())
        {
            if self.bytes == 0 {
                self.bytes = (self.entries.capacity() * item_sz) as u64;
            }
            while !self.entries.is_empty()
                && (self.entries.len() >= MAX_MEMO_ENTRIES
                    || self.bytes.saturating_add(bytes) > self.maximum)
            {
                self.bytes -= self.entries.remove(0).3;
            }
            if self.bytes.saturating_add(bytes) <= self.maximum {
                self.entries.push((run, n, Arc::clone(&block), bytes));
                self.bytes += bytes;
            }
        }
        Ok(block)
    }
}

pub(crate) struct RunReader {
    pub(super) reserved_memory: u64,
    pub(super) file: File,
    pub(super) data_key: [u8; 32],
    cipher: Option<Aes256Gcm>,
    pub(super) footer: Footer,
    pub(super) top: Top,
}

pub(super) struct IndexBlock(pub(super) Vec<BlockRef>, Vec<u8>);

impl IndexBlock {
    pub(super) fn parse(payload: Vec<u8>, first_key: &[u8], avail: Option<u64>) -> Result<Self> {
        let mut at = 0;
        let (blocks, _) = get_refs_bounded(&payload, &mut at, avail.unwrap_or(u64::MAX))?;
        if blocks.first().map(|b| b.first_key.as_slice()) != Some(first_key) {
            return Err(corrupt("index block does not start at its top entry"));
        }
        let filter = read_filter(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("trailing bytes in index block"));
        }
        let mem =
            (payload.capacity() + refs_footprint(&blocks, blocks.capacity()) + filter.len()) as u64;
        if avail.is_some_and(|lim| mem > lim) {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(Self(blocks, filter.to_vec()))
    }

    pub(super) fn block_pos(&self, key: &[u8]) -> Option<usize> {
        self.0
            .partition_point(|b| b.first_key.as_slice() <= key)
            .checked_sub(1)
    }

    pub(super) fn locate(&self, key: &[u8]) -> Option<&BlockRef> {
        may_hold(&self.1, key_hash(key))
            .then(|| self.block_pos(key).map(|i| &self.0[i]))
            .flatten()
    }

    pub(super) fn footprint(&self) -> u64 {
        (refs_footprint(&self.0, self.0.capacity()) + self.1.capacity()) as u64
    }
}

impl RunReader {
    pub(crate) fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_inner(path, data_key, None, 0)
    }

    pub(crate) fn open_bounded(p: &Path, k: &[u8; 32], b: u64, res: u64) -> Result<Self> {
        Self::open_inner(p, k, Some(b), res)
    }

    fn open_inner(p: &Path, key: &[u8; 32], bytes: Option<u64>, reserved: u64) -> Result<Self> {
        let named = p
            .file_name()
            .and_then(|n| n.to_str()?.strip_suffix(RUN_SUFFIX))
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| corrupt("run file name is not <run id>.ldrun"))?;
        let file = File::open(p)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let tail_len = bytes.map_or(len.min(TAIL_BYTES as u64) as usize, |_| FOOTER_BYTES);
        let mut tail = vec![0u8; tail_len];
        file.read_exact_at(&mut tail, len - tail_len as u64)?;
        let f = Footer::decode(tail[tail_len - FOOTER_BYTES..].try_into().unwrap())?;
        if bytes.is_some_and(|n| u64::from(f.top_len).saturating_mul(4) > n) {
            return Err(Error::ReadBudgetExceeded);
        }
        if f.run_id != named {
            return Err(corrupt("run file name and run id disagree"));
        }
        if f.top_offset
            .checked_add(u64::from(f.top_len) + FOOTER_BYTES as u64)
            != Some(len)
        {
            return Err(corrupt("top block is not before the footer"));
        }
        let cipher = bytes
            .is_none()
            .then(|| new_cipher(key, f.run_id))
            .transpose()?;
        let mut r = Self {
            reserved_memory: reserved,
            file,
            data_key: *key,
            cipher,
            footer: f,
            top: Top::default(),
        };
        let in_tail = f
            .top_offset
            .checked_sub(len - tail_len as u64)
            .and_then(|o| usize::try_from(o).ok())
            .filter(|&at| bytes.is_none() && at + f.top_len as usize <= tail_len);
        let top_bytes = match (bytes, in_tail) {
            (Some(a), _) => {
                r.read_metadata_frame(f.top_offset, f.top_len, TOP_COUNTER, MAX_TOP_BYTES, a)?
            }
            (None, Some(at)) => r.decode_checked(
                &tail[at..at + f.top_len as usize],
                TOP_COUNTER,
                MAX_TOP_BYTES,
                None,
            )?,
            (None, None) => {
                r.read_frame(f.top_offset, f.top_len, TOP_COUNTER, MAX_TOP_BYTES, None)?
            }
        };
        r.top = Top::decode_bounded(&top_bytes, bytes.unwrap_or(u64::MAX))?;
        if r.top.counts
            != (
                f.records,
                u64::from(f.data_blocks),
                u64::from(f.max_payload),
            )
        {
            return Err(corrupt("footer and top block disagree"));
        }
        Ok(r)
    }

    fn read_frame(
        &self,
        off: u64,
        len: u32,
        ctr: u64,
        max: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<Vec<u8>> {
        if len as usize > max.saturating_add(8 + MIN_ENCODED_LEN) {
            return Err(corrupt("stored block length over its limit"));
        }
        let mut frame = vec![0u8; len as usize];
        self.file.read_exact_at(&mut frame, off)?;
        self.decode_checked(&frame, ctr, max, scratch)
    }

    fn decode_checked(
        &self,
        frame: &[u8],
        ctr: u64,
        max: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<Vec<u8>> {
        let prep = self.cipher.as_ref().map(|c| (self.footer.run_id, c));
        let (chunk, cnt, payload) = decode_frame(&self.data_key, frame, max, prep, scratch)?;
        ((chunk, cnt) == (self.footer.run_id, ctr))
            .then_some(payload)
            .ok_or_else(|| corrupt("block belongs to another place"))
    }

    pub(super) fn read_index(
        &self,
        n: usize,
        scratch: Option<&mut DecodeScratch>,
        adm: Option<u64>,
    ) -> Result<IndexBlock> {
        let e = &self.top.indexes[n];
        let (max, ctr) = (
            self.footer.max_payload as usize,
            INDEX_COUNTER_BASE + e.number,
        );
        let avail = adm
            .map(|a| {
                a.checked_sub(self.metadata_bytes())
                    .ok_or(Error::ReadBudgetExceeded)
            })
            .transpose()?;
        let raw = if let Some(a) = avail {
            self.read_metadata_frame(e.offset, e.len, ctr, max, a)?
        } else {
            self.read_frame(e.offset, e.len, ctr, max, scratch)?
        };
        IndexBlock::parse(raw, &e.first_key, avail)
    }

    pub(super) fn read_data(
        &self,
        b: &BlockRef,
        s: Option<&mut DecodeScratch>,
    ) -> Result<BlockRecords> {
        let records = decode_block(&self.read_frame(
            b.offset,
            b.len,
            b.number,
            self.footer.max_payload as usize,
            s,
        )?)?;
        (records.first().map(|(k, _)| k) == Some(&b.first_key))
            .then_some(records)
            .ok_or_else(|| corrupt("data block first key mismatch"))
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
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let (mut out, mut index, mut data, mut scratch) =
            (vec![None; keys.len()], None, None, DecodeScratch::default());
        for slot in order {
            let key = keys[slot];
            let Some(number) = self.index_for(key) else {
                continue;
            };
            if index.as_ref().map(|(n, _)| *n) != Some(number) {
                index = Some((number, metadata.read(self, number, &mut scratch)?));
            }
            let Some(block) = index.as_ref().unwrap().1.locate(key) else {
                continue;
            };
            if data.as_ref().map(|(n, _)| *n) != Some(block.number) {
                data = Some((block.number, self.read_data(block, Some(&mut scratch))?));
            }
            let recs = &data.as_ref().unwrap().1;
            if let Ok(n) = recs.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                out[slot] = Some(recs[n].1.clone());
            }
        }
        Ok(out)
    }

    pub(crate) fn range(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> RunRange {
        RunRange::new(Arc::clone(self), lower, upper)
    }

    pub(crate) fn iter(self: &Arc<Self>) -> RunRange {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }
}
