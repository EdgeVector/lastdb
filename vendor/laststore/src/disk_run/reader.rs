//! Opens run files, memoizes index blocks, and reads records from data blocks.

use super::codec::{corrupt, decode_block, BlockRecords};
use super::footer::{BlockRef, Footer, Span, Top, FOOTER_BYTES};
use super::{
    Found, IndexBlock, RunRange, INDEX_COUNTER_BASE, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::frame::{decode_frame, new_cipher, DecodeScratch, MIN_ENCODED_LEN};
use crate::{Error, Result};
use aes_gcm::Aes256Gcm;
use std::fs::File;
use std::mem::size_of;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const TAIL_BYTES: usize = 512;
const MAX_MEMO_ENTRIES: usize = 64;
const MAX_MEMO_BYTES: u64 = 256 * 1024;
type Scratch<'a> = Option<&'a mut DecodeScratch>;

struct MemoEntry {
    run_id: Uuid,
    index_num: usize,
    block: Arc<IndexBlock>,
    bytes: u64,
}

pub(crate) struct MetadataMemo {
    entries: Vec<MemoEntry>,
    retained_bytes: u64,
    max_bytes: u64,
}

impl MetadataMemo {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            entries: Vec::new(),
            retained_bytes: 0,
            max_bytes: MAX_MEMO_BYTES.min(budget / 16),
        }
    }

    pub(crate) fn read(
        &mut self,
        reader: &RunReader,
        index_num: usize,
        scratch: &mut DecodeScratch,
    ) -> Result<Arc<IndexBlock>> {
        let run_id = reader.footer.run_id;
        if let Some(entry) = self
            .entries
            .iter()
            .find(|e| e.run_id == run_id && e.index_num == index_num)
        {
            return Ok(Arc::clone(&entry.block));
        }
        let block = Arc::new(reader.read_index(index_num, Some(scratch))?);
        let item_sz = size_of::<MemoEntry>();
        let bytes = (size_of::<IndexBlock>() + 2 * size_of::<usize>()) as u64 + block.footprint();
        let cap_ok =
            self.entries.capacity() > 0 || self.entries.try_reserve_exact(MAX_MEMO_ENTRIES).is_ok();
        if bytes.saturating_add((MAX_MEMO_ENTRIES * item_sz) as u64) <= self.max_bytes && cap_ok {
            if self.retained_bytes == 0 {
                self.retained_bytes = (self.entries.capacity() * item_sz) as u64;
            }
            while self.entries.len() >= MAX_MEMO_ENTRIES
                || self.retained_bytes.saturating_add(bytes) > self.max_bytes
            {
                self.retained_bytes -= self.entries.remove(0).bytes;
            }
            self.entries.push(MemoEntry {
                run_id,
                index_num,
                block: Arc::clone(&block),
                bytes,
            });
            self.retained_bytes += bytes;
        }
        Ok(block)
    }
}

pub(crate) struct RunReader {
    file: File,
    data_key: [u8; 32],
    cipher: Aes256Gcm,
    pub(crate) footer: Footer,
    pub(crate) top: Top,
}

impl RunReader {
    pub(crate) fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_with_limit(path, data_key, None)
    }

    pub(crate) fn open_with_limit(
        path: &Path,
        key: &[u8; 32],
        top_limit: Option<u64>,
    ) -> Result<Self> {
        let named = path
            .file_name()
            .and_then(|n| n.to_str()?.strip_suffix(RUN_SUFFIX))
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| corrupt("run file name is not <run id>.ldrun"))?;
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let tail_len = top_limit.map_or(len.min(TAIL_BYTES as u64) as usize, |_| FOOTER_BYTES);
        let (mut tail, base) = (vec![0u8; tail_len], len - tail_len as u64);
        file.read_exact_at(&mut tail, base)?;
        let footer = Footer::decode(tail[tail_len - FOOTER_BYTES..].try_into().unwrap())?;
        if top_limit.is_some_and(|allow| u64::from(footer.top_len) > allow) {
            return Err(Error::ReadBudgetExceeded);
        }
        if footer.run_id != named
            || footer
                .top_offset
                .checked_add(u64::from(footer.top_len) + FOOTER_BYTES as u64)
                != Some(len)
        {
            return Err(corrupt("run footer identity or top offset"));
        }
        let cipher = new_cipher(key, footer.run_id)?;
        let mut reader = Self {
            file,
            data_key: *key,
            cipher,
            footer,
            top: Top::default(),
        };
        let (span, top_len) = (
            (footer.top_offset, footer.top_len, TOP_COUNTER),
            footer.top_len as usize,
        );
        let in_tail = footer
            .top_offset
            .checked_sub(base)
            .and_then(|o| usize::try_from(o).ok())
            .filter(|&i| top_limit.is_none() && i + top_len <= tail_len);
        let raw = match in_tail {
            Some(i) => {
                reader.decode_checked(&tail[i..i + top_len], TOP_COUNTER, MAX_TOP_BYTES, None)?
            }
            None => reader.read_frame(span, MAX_TOP_BYTES, None)?,
        };
        reader.top = Top::decode_bounded(&raw, top_limit.unwrap_or(u64::MAX))?;
        let top_matches = (
            reader.top.records,
            reader.top.data_blocks,
            reader.top.max_payload_bytes,
        ) == (
            footer.records,
            u64::from(footer.data_blocks),
            u64::from(footer.max_payload_bytes),
        );
        top_matches
            .then_some(reader)
            .ok_or_else(|| corrupt("footer and top block disagree"))
    }

    fn check_len(len: u32, max: usize) -> Result<()> {
        (len as usize <= max.saturating_add(8 + MIN_ENCODED_LEN))
            .then_some(())
            .ok_or_else(|| corrupt("stored block over limit"))
    }

    fn read_frame(&self, (off, len, ctr): Span, max: usize, sc: Scratch<'_>) -> Result<Vec<u8>> {
        Self::check_len(len, max)?;
        let mut frame = vec![0u8; len as usize];
        self.file.read_exact_at(&mut frame, off)?;
        self.decode_checked(&frame, ctr, max, sc)
    }

    fn decode_checked(
        &self,
        frame: &[u8],
        counter: u64,
        max: usize,
        scratch: Scratch<'_>,
    ) -> Result<Vec<u8>> {
        let prepared = Some((self.footer.run_id, &self.cipher));
        let (chunk, cnt, payload) = decode_frame(&self.data_key, frame, max, (prepared, scratch))?;
        ((chunk, cnt) == (self.footer.run_id, counter))
            .then_some(payload)
            .ok_or_else(|| corrupt("block belongs to another place"))
    }

    pub(crate) fn read_index(&self, index_num: usize, scratch: Scratch<'_>) -> Result<IndexBlock> {
        let (entry, max) = (
            &self.top.indexes[index_num],
            self.footer.max_payload_bytes as usize,
        );
        let span = (entry.offset, entry.len, INDEX_COUNTER_BASE + entry.number);
        IndexBlock::parse(self.read_frame(span, max, scratch)?, &entry.first_key)
    }

    pub(crate) fn read_data(&self, block: &BlockRef, scratch: Scratch<'_>) -> Result<BlockRecords> {
        let max = self.footer.max_payload_bytes as usize;
        let records = decode_block(&self.read_frame(block.span(), max, scratch)?)?;
        (records.first().map(|(k, _)| k) == Some(&block.first_key))
            .then_some(records)
            .ok_or_else(|| corrupt("data block first key mismatch"))
    }

    pub(crate) fn index_for(&self, key: &[u8]) -> Option<usize> {
        self.top
            .indexes
            .partition_point(|i| i.first_key.as_slice() <= key)
            .checked_sub(1)
    }

    pub(crate) fn get_many_with(
        &self,
        keys: &[&[u8]],
        mut load_idx: impl FnMut(&Self, usize, &mut DecodeScratch) -> Result<Arc<IndexBlock>>,
    ) -> Result<Vec<Option<Found>>> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let (mut out, mut idx, mut data, mut scratch) =
            (vec![None; keys.len()], None, None, DecodeScratch::default());
        for slot in order {
            let key = keys[slot];
            let Some(num) = self.index_for(key) else {
                continue;
            };
            if idx.as_ref().map(|(n, _)| *n) != Some(num) {
                idx = Some((num, load_idx(self, num, &mut scratch)?));
            }
            let Some(block) = idx.as_ref().unwrap().1.locate(key) else {
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
        let mut scan = RunRange::sequence(
            vec![],
            &[0; 32],
            lower.map(<[u8]>::to_vec),
            upper.map(<[u8]>::to_vec),
        );
        scan.set_reader(Arc::clone(self));
        scan
    }

    pub(crate) fn iter(self: &Arc<Self>) -> RunRange {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }
}
