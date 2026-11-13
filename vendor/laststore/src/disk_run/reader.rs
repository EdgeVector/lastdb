//! Reads, opens, and streams records from run files.

use super::codec::{corrupt, decode_block, BlockRecords};
use super::footer::{refs_footprint, try_vec, BlockRef, Footer, Span, Top, FOOTER_BYTES};
use super::{
    Block, Found, IndexBlock, RunRange, INDEX_COUNTER_BASE, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::frame::{decode_frame, new_cipher, DecodeScratch, VerifiedFrameReader, MIN_ENCODED_LEN};
use crate::{Error, Result};
use aes_gcm::Aes256Gcm;
use std::cmp::Ordering;
use std::fs::File;
use std::io::Read;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const TAIL_BYTES: usize = 512;
type Scratch<'a> = Option<&'a mut DecodeScratch>;

pub(super) fn read_vec(r: &mut impl Read, len: usize) -> Result<Vec<u8>> {
    let mut out = try_vec(len)?;
    out.resize(len, 0);
    r.read_exact(&mut out).map(|()| out).map_err(Into::into)
}

pub(crate) struct RunReader(
    u64,
    File,
    [u8; 32],
    Option<Aes256Gcm>,
    pub(crate) Footer,
    pub(crate) Top,
);

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
        let (mut tail, base) = (vec![0u8; tail_len], len - tail_len as u64);
        file.read_exact_at(&mut tail, base)?;
        let f = Footer::decode(tail[tail_len - FOOTER_BYTES..].try_into().unwrap())?;
        if bytes.is_some_and(|n| u64::from(f.2).saturating_mul(4) > n) {
            return Err(Error::ReadBudgetExceeded);
        }
        if f.0 != named || f.1.checked_add(u64::from(f.2) + FOOTER_BYTES as u64) != Some(len) {
            return Err(corrupt("run footer identity or top offset"));
        }
        let cipher = bytes.is_none().then(|| new_cipher(key, f.0)).transpose()?;
        let mut r = Self(reserved, file, *key, cipher, f, Top::default());
        let (span, tlen) = ((f.1, f.2, TOP_COUNTER), f.2 as usize);
        let in_tail =
            f.1.checked_sub(base)
                .and_then(|o| usize::try_from(o).ok())
                .filter(|&i| bytes.is_none() && i + tlen <= tail_len);
        let raw = match (bytes, in_tail) {
            (Some(a), _) => r.read_meta(span, MAX_TOP_BYTES, a)?,
            (None, Some(i)) => {
                r.decode_checked(&tail[i..i + tlen], TOP_COUNTER, MAX_TOP_BYTES, None)?
            }
            (None, None) => r.read_frame(span, MAX_TOP_BYTES, None)?,
        };
        r.5 = Top::decode_bounded(&raw, bytes.unwrap_or(u64::MAX))?;
        (r.5 .0 == (f.3, f.5.into(), f.4.into()))
            .then_some(r)
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
        self.1.read_exact_at(&mut frame, off)?;
        self.decode_checked(&frame, ctr, max, sc)
    }

    fn decode_checked(
        &self,
        frame: &[u8],
        ctr: u64,
        max: usize,
        sc: Scratch<'_>,
    ) -> Result<Vec<u8>> {
        let prep = self.3.as_ref().map(|c| (self.4 .0, c));
        let (chunk, cnt, payload) = decode_frame(&self.2, frame, max, (prep, sc))?;
        ((chunk, cnt) == (self.4 .0, ctr))
            .then_some(payload)
            .ok_or_else(|| corrupt("block belongs to another place"))
    }

    fn meta_bytes(&self) -> u64 {
        (refs_footprint(&self.5 .1, self.5 .1.capacity()) + self.5 .2.capacity()) as u64
    }

    pub(crate) fn read_index(
        &self,
        n: usize,
        sc: Scratch<'_>,
        adm: Option<u64>,
    ) -> Result<IndexBlock> {
        let (e, max) = (&self.5 .1[n], self.4 .4 as usize);
        let span = (e.2, e.3, INDEX_COUNTER_BASE + e.1);
        let (raw, avail) = if let Some(a) = adm {
            let av = a
                .checked_sub(self.meta_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            (self.read_meta(span, max, av)?, Some(av))
        } else {
            (self.read_frame(span, max, sc)?, None)
        };
        IndexBlock::parse(raw, &e.0, avail)
    }

    pub(crate) fn read_data(&self, b: &BlockRef, sc: Scratch<'_>) -> Result<BlockRecords> {
        let records = decode_block(&self.read_frame(b.span(), self.4 .4 as usize, sc)?)?;
        (records.first().map(|(k, _)| k) == Some(&b.0))
            .then_some(records)
            .ok_or_else(|| corrupt("data block first key mismatch"))
    }

    pub(crate) fn index_for(&self, key: &[u8]) -> Option<usize> {
        self.5
             .1
            .partition_point(|i| i.0.as_slice() <= key)
            .checked_sub(1)
    }

    pub(crate) fn get_many_with(
        &self,
        keys: &[&[u8]],
        mut load_idx: impl FnMut(&Self, usize, &mut DecodeScratch) -> Result<Arc<IndexBlock>>,
    ) -> Result<Vec<Option<Found>>> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let (mut out, mut idx, mut data, mut sc) =
            (vec![None; keys.len()], None, None, DecodeScratch::default());
        for slot in order {
            let key = keys[slot];
            let Some(num) = self.index_for(key) else {
                continue;
            };
            if idx.as_ref().map(|(n, _)| *n) != Some(num) {
                idx = Some((num, load_idx(self, num, &mut sc)?));
            }
            let Some(block) = idx.as_ref().unwrap().1.locate(key) else {
                continue;
            };
            if data.as_ref().map(|(n, _)| *n) != Some(block.1) {
                data = Some((block.1, self.read_data(block, Some(&mut sc))?));
            }
            let recs = &data.as_ref().unwrap().1;
            if let Ok(n) = recs.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                out[slot] = Some(recs[n].1.clone());
            }
        }
        Ok(out)
    }

    pub(crate) fn range(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> RunRange {
        let mut s = RunRange::sequence(
            vec![],
            &[0; 32],
            lower.map(<[u8]>::to_vec),
            upper.map(<[u8]>::to_vec),
        );
        s.set_reader(Arc::clone(self));
        s
    }

    pub(crate) fn iter(self: &Arc<Self>) -> RunRange {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }

    fn open_verified(
        &self,
        (off, len, ctr): Span,
        max: usize,
        held: u64,
    ) -> Result<(VerifiedFrameReader, u64)> {
        Self::check_len(len, max)?;
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        if held
            .checked_add(2 * res.stream_buffer_bytes + 16)
            .is_none_or(|n| n > res.working_memory_bytes)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let input = VerifiedFrameReader::open(&self.1, off, u64::from(len), &self.2)?;
        if input.2 != (self.4 .0, ctr) || input.3 .0 > max as u64 {
            return Err(corrupt("stream frame identity or limit"));
        }
        let avail = res
            .working_memory_bytes
            .checked_sub(input.3 .1)
            .and_then(|n| n.checked_sub(held))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((input, avail))
    }

    fn read_meta(&self, span: Span, max: usize, adm: u64) -> Result<Vec<u8>> {
        let held = self
            .0
            .checked_add(self.meta_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let (mut input, avail) = self.open_verified(span, max, held)?;
        let count = usize::try_from(input.3 .0)
            .ok()
            .filter(|_| input.3 .0 <= adm.min(avail))
            .ok_or(Error::ReadBudgetExceeded)?;
        let out = read_vec(&mut input, count)?;
        (input.read(&mut [0; 1])? == 0)
            .then_some(out)
            .ok_or_else(|| corrupt("stream metadata frame length"))
    }

    pub(crate) fn open_block(
        &self,
        r: &BlockRef,
        idx: &IndexBlock,
        c: u64,
        h: u64,
    ) -> Result<Block> {
        let held = h.saturating_add(self.meta_bytes() + idx.footprint());
        let (input, av) = self.open_verified(r.span(), self.4 .4 as usize, held)?;
        Ok(Block(input, Vec::new(), false, (0, c.min(av), av)))
    }

    pub(crate) fn stream_body(
        &self,
        key: &[u8],
        (adm, held, max): (u64, u64, u64),
    ) -> Result<Option<Option<Vec<u8>>>> {
        let Some(num) = self.index_for(key) else {
            return Ok(None);
        };
        let index = self.read_index(num, None, Some(adm))?;
        let Some(r) = index.locate(key) else {
            return Ok(None);
        };
        let mut b = self.open_block(r, &index, adm, held)?;
        while let Some(live) = b.next(Some(&r.0))? {
            match b.1.as_slice().cmp(key) {
                Ordering::Less => {}
                Ordering::Greater => return b.finish().map(|()| None),
                Ordering::Equal if !live => return b.finish().map(|()| Some(None)),
                Ordering::Equal => {
                    let lim = max
                        .min(adm)
                        .min(b.3 .2.saturating_sub(b.1.capacity() as u64));
                    let n = usize::try_from(b.3 .0)
                        .ok()
                        .filter(|_| b.3 .0 <= lim)
                        .ok_or(Error::ReadBudgetExceeded)?;
                    let out = read_vec(&mut b.0, n)?;
                    b.3 .0 = 0;
                    return b.finish().map(|()| Some(Some(out)));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn stream_key(
        &self,
        (lo, up, excl): (&[u8], Option<&[u8]>, bool),
        (adm, held): (u64, u64),
    ) -> Result<Option<Vec<u8>>> {
        let start = self.index_for(lo).unwrap_or(0);
        for num in start..self.5 .1.len() {
            let index = self.read_index(num, None, Some(adm))?;
            let first = if num == start {
                index.block_pos(lo).unwrap_or(0)
            } else {
                0
            };
            for r in &index.0[first..] {
                if up.is_some_and(|end| r.0.as_slice() >= end) {
                    return Ok(None);
                }
                let mut b = self.open_block(r, &index, adm, held)?;
                while b.next(Some(&r.0))?.is_some() {
                    let k = b.1.as_slice();
                    if (k > lo || (!excl && k == lo)) && up.is_none_or(|end| k < end) {
                        let key = b.1.clone();
                        return b.finish().map(|()| Some(key));
                    }
                    if up.is_some_and(|end| k >= end) {
                        return b.finish().map(|()| None);
                    }
                }
            }
        }
        Ok(None)
    }
}
