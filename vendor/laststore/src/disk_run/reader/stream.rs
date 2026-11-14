//! Verified streaming frame readers and bounded block scans on [`RunReader`].

use super::RunReader;
use crate::disk_run::codec::corrupt;
use crate::disk_run::footer::{try_vec, BlockRef, Span};
use crate::disk_run::{Block, IndexBlock};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::Read;

pub(in crate::disk_run) fn read_vec(reader: &mut impl Read, len: usize) -> Result<Vec<u8>> {
    let mut out = try_vec(len)?;
    out.resize(len, 0);
    reader
        .read_exact(&mut out)
        .map(|()| out)
        .map_err(Into::into)
}

impl RunReader {
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
        let input = VerifiedFrameReader::open(&self.file, off, u64::from(len), &self.data_key)?;
        if (input.run_id, input.counter) != (self.footer.run_id, ctr)
            || input.plain_len > max as u64
        {
            return Err(corrupt("stream frame identity or limit"));
        }
        let avail = res
            .working_memory_bytes
            .checked_sub(input.reserved_memory_bytes)
            .and_then(|n| n.checked_sub(held))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((input, avail))
    }

    pub(super) fn read_meta(&self, span: Span, max: usize, adm: u64) -> Result<Vec<u8>> {
        let held = self
            .reserved_bytes
            .checked_add(self.meta_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let (mut input, avail) = self.open_verified(span, max, held)?;
        let count = usize::try_from(input.plain_len)
            .ok()
            .filter(|_| input.plain_len <= adm.min(avail))
            .ok_or(Error::ReadBudgetExceeded)?;
        let out = read_vec(&mut input, count)?;
        (input.read(&mut [0; 1])? == 0)
            .then_some(out)
            .ok_or_else(|| corrupt("stream metadata frame length"))
    }

    fn open_block(
        &self,
        block_ref: &BlockRef,
        index: &IndexBlock,
        key_budget: u64,
        held: u64,
    ) -> Result<Block> {
        let total_held = held.saturating_add(self.meta_bytes() + index.footprint());
        let max = self.footer.max_payload_bytes as usize;
        let (reader, avail_bytes) = self.open_verified(block_ref.span(), max, total_held)?;
        Ok(Block::new(reader, key_budget.min(avail_bytes), avail_bytes))
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
        let Some(block_ref) = index.locate(key) else {
            return Ok(None);
        };
        let mut block = self.open_block(block_ref, &index, adm, held)?;
        while let Some(live) = block.next(Some(&block_ref.first_key))? {
            match block.key.as_slice().cmp(key) {
                Ordering::Less => {}
                Ordering::Greater => return block.finish().map(|()| None),
                Ordering::Equal if !live => return block.finish().map(|()| Some(None)),
                Ordering::Equal => {
                    let lim = max.min(adm).min(
                        block
                            .avail_bytes
                            .saturating_sub(block.key.capacity() as u64),
                    );
                    let len = usize::try_from(block.pending_value_bytes)
                        .ok()
                        .filter(|_| block.pending_value_bytes <= lim)
                        .ok_or(Error::ReadBudgetExceeded)?;
                    let out = read_vec(&mut block.reader, len)?;
                    block.pending_value_bytes = 0;
                    return block.finish().map(|()| Some(Some(out)));
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
        for num in start..self.top.indexes.len() {
            let index = self.read_index(num, None, Some(adm))?;
            let first = if num == start {
                index.block_pos(lo).unwrap_or(0)
            } else {
                0
            };
            for block_ref in &index.blocks[first..] {
                if up.is_some_and(|end| block_ref.first_key.as_slice() >= end) {
                    return Ok(None);
                }
                let mut block = self.open_block(block_ref, &index, adm, held)?;
                while block.next(Some(&block_ref.first_key))?.is_some() {
                    let cur = block.key.as_slice();
                    if (cur > lo || (!excl && cur == lo)) && up.is_none_or(|end| cur < end) {
                        let key = block.key.clone();
                        return block.finish().map(|()| Some(key));
                    }
                    if up.is_some_and(|end| cur >= end) {
                        return block.finish().map(|()| None);
                    }
                }
            }
        }
        Ok(None)
    }
}
