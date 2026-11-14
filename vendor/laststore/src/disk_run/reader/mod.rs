//! Opens run files, verifies footers/indexes, and reads records from data blocks.

mod stream;

pub(in crate::disk_run) use stream::read_vec;

use super::codec::{corrupt, decode_block, BlockRecords};
use super::footer::{refs_footprint, BlockRef, Footer, Span, Top, FOOTER_BYTES};
use super::{
    Found, IndexBlock, RunRange, INDEX_COUNTER_BASE, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::frame::{decode_frame, new_cipher, DecodeScratch, MIN_ENCODED_LEN};
use crate::{Error, Result};
use aes_gcm::Aes256Gcm;
use std::fs::File;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const TAIL_BYTES: usize = 512;
type Scratch<'a> = Option<&'a mut DecodeScratch>;

pub(crate) struct RunReader {
    pub(super) reserved_bytes: u64,
    pub(super) file: File,
    pub(super) data_key: [u8; 32],
    cipher: Option<Aes256Gcm>,
    pub(crate) footer: Footer,
    pub(crate) top: Top,
}

impl RunReader {
    pub(crate) fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_inner(path, data_key, None, 0)
    }

    pub(crate) fn open_bounded(
        path: &Path,
        data_key: &[u8; 32],
        bytes: u64,
        reserved: u64,
    ) -> Result<Self> {
        Self::open_inner(path, data_key, Some(bytes), reserved)
    }

    fn open_inner(path: &Path, key: &[u8; 32], bytes: Option<u64>, reserved: u64) -> Result<Self> {
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
        let tail_len = bytes.map_or(len.min(TAIL_BYTES as u64) as usize, |_| FOOTER_BYTES);
        let (mut tail, base) = (vec![0u8; tail_len], len - tail_len as u64);
        file.read_exact_at(&mut tail, base)?;
        let footer = Footer::decode(tail[tail_len - FOOTER_BYTES..].try_into().unwrap())?;
        if bytes.is_some_and(|n| u64::from(footer.top_len).saturating_mul(4) > n) {
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
        let cipher = bytes
            .is_none()
            .then(|| new_cipher(key, footer.run_id))
            .transpose()?;
        let mut reader = Self {
            reserved_bytes: reserved,
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
            .filter(|&i| bytes.is_none() && i + top_len <= tail_len);
        let raw = match (bytes, in_tail) {
            (Some(adm), _) => reader.read_meta(span, MAX_TOP_BYTES, adm)?,
            (None, Some(i)) => {
                reader.decode_checked(&tail[i..i + top_len], TOP_COUNTER, MAX_TOP_BYTES, None)?
            }
            (None, None) => reader.read_frame(span, MAX_TOP_BYTES, None)?,
        };
        reader.top = Top::decode_bounded(&raw, bytes.unwrap_or(u64::MAX))?;
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

    pub(super) fn check_len(len: u32, max: usize) -> Result<()> {
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
        let prepared = self.cipher.as_ref().map(|c| (self.footer.run_id, c));
        let (chunk, cnt, payload) = decode_frame(&self.data_key, frame, max, (prepared, scratch))?;
        ((chunk, cnt) == (self.footer.run_id, counter))
            .then_some(payload)
            .ok_or_else(|| corrupt("block belongs to another place"))
    }

    pub(super) fn meta_bytes(&self) -> u64 {
        (refs_footprint(&self.top.indexes, self.top.indexes.capacity())
            + self.top.last_key.capacity()) as u64
    }

    pub(crate) fn read_index(
        &self,
        index_num: usize,
        scratch: Scratch<'_>,
        admitted: Option<u64>,
    ) -> Result<IndexBlock> {
        let (entry, max) = (
            &self.top.indexes[index_num],
            self.footer.max_payload_bytes as usize,
        );
        let span = (entry.offset, entry.len, INDEX_COUNTER_BASE + entry.number);
        let (raw, avail) = if let Some(adm) = admitted {
            let av = adm
                .checked_sub(self.meta_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            (self.read_meta(span, max, av)?, Some(av))
        } else {
            (self.read_frame(span, max, scratch)?, None)
        };
        IndexBlock::parse(raw, &entry.first_key, avail)
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
