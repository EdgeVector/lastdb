//! Backup-only bounded selection of a key, metadata frames, and stored bodies.

use super::{IndexBlock, RunReader};
use crate::disk_run::bloom::key_hash;
use crate::disk_run::codec::corrupt;
use crate::disk_run::footer::{refs_footprint, try_vec, BlockRef};
use crate::disk_run::{RunMeta, INDEX_COUNTER_BASE, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::{VerifiedFrameReader, MIN_ENCODED_LEN};
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::{Read, Write};
use std::path::Path;

struct Block {
    input: VerifiedFrameReader,
    key: Vec<u8>,
    seen: bool,
    remaining: u64,
    coordinate_bytes: u64,
    memory_available: u64,
}

impl Block {
    fn new(reader: &RunReader, r: &BlockRef, coord_bytes: u64, retained: u64) -> Result<Self> {
        let max = reader.footer.max_payload as usize;
        let (input, memory_available) =
            reader.open_verified_frame(r.offset, r.len, r.number, max, retained)?;
        Ok(Self {
            input,
            key: Vec::new(),
            seen: false,
            remaining: 0,
            coordinate_bytes: coord_bytes.min(memory_available),
            memory_available,
        })
    }

    fn varint(&mut self) -> Result<Option<u64>> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let mut b = [0u8; 1];
            if self.input.read(&mut b)? == 0 {
                return (shift == 0)
                    .then_some(None)
                    .ok_or_else(|| corrupt("stream varint ends early"));
            }
            if shift == 63 && b[0] > 1 {
                return Err(corrupt("stream varint overflow"));
            }
            value |= u64::from(b[0] & 0x7f) << shift;
            if b[0] & 0x80 == 0 {
                return Ok(Some(value));
            }
        }
        Err(corrupt("stream varint overflow"))
    }

    fn length(&mut self, max: usize) -> Result<usize> {
        self.varint()?
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n <= max)
            .ok_or_else(|| corrupt("stream record length"))
    }

    fn next(&mut self) -> Result<Option<bool>> {
        copy_exact(&mut self.input, &mut std::io::sink(), self.remaining)?;
        self.remaining = 0;
        let Some(shared) = self.varint()? else {
            return Ok(None);
        };
        let shared = usize::try_from(shared)
            .ok()
            .filter(|&n| n <= self.key.len())
            .ok_or_else(|| corrupt("stream key prefix"))?;
        let suffix = self.length(MAX_RUN_KEY_BYTES - shared)?;
        let len = shared + suffix;
        if (len.max(self.key.capacity()) as u64).saturating_mul(2) > self.coordinate_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        let mut tail = try_vec(suffix)?;
        tail.resize(suffix, 0);
        self.input.read_exact(&mut tail)?;
        if len == 0 || self.seen && tail.as_slice() <= &self.key[shared..] {
            return Err(corrupt("stream block key order"));
        }
        self.key.truncate(shared);
        self.key
            .try_reserve_exact(len.saturating_sub(self.key.len()))
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.key.extend_from_slice(&tail);
        drop(tail);
        self.seen = true;
        let tag = self.length(MAX_RUN_VALUE_BYTES + 1)?;
        self.remaining = tag.saturating_sub(1) as u64;
        Ok(Some(tag != 0))
    }

    fn finish(mut self) -> Result<()> {
        while self.next()?.is_some() {}
        Ok(())
    }

    fn checked_next(&mut self, r: &BlockRef) -> Result<Option<bool>> {
        let first = !self.seen;
        let row = self.next()?;
        if first && (row.is_none() || self.key != r.first_key) {
            return Err(corrupt("data block does not start at its index entry"));
        }
        Ok(row)
    }
}

impl RunReader {
    fn metadata_bytes(&self) -> u64 {
        (refs_footprint(&self.top.indexes, self.top.indexes.capacity())
            + self.top.last_key.capacity()) as u64
    }

    fn open_verified_frame(
        &self,
        off: u64,
        len: u32,
        ctr: u64,
        max: usize,
        held: u64,
    ) -> Result<(VerifiedFrameReader, u64)> {
        if len as usize > max.saturating_add(8 + MIN_ENCODED_LEN) {
            return Err(corrupt("stored block length over its limit"));
        }
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        if held
            .checked_add(2 * res.stream_buffer_bytes + 16)
            .is_none_or(|n| n > res.working_memory_bytes)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let input = VerifiedFrameReader::open(&self.file, off, u64::from(len), &self.data_key)?;
        if (input.chunk_uuid, input.counter) != (self.footer.run_id, ctr)
            || input.payload_len > max as u64
        {
            return Err(corrupt("stream frame identity or limit"));
        }
        let avail = res
            .working_memory_bytes
            .checked_sub(input.memory_bytes)
            .and_then(|n| n.checked_sub(held))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((input, avail))
    }

    pub(in crate::disk_run) fn read_metadata_frame(
        &self,
        off: u64,
        len: u32,
        ctr: u64,
        max: usize,
        adm: u64,
    ) -> Result<Vec<u8>> {
        let held = self
            .reserved_memory
            .checked_add(self.metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let (mut input, avail) = self.open_verified_frame(off, len, ctr, max, held)?;
        let count = (input.payload_len <= adm.min(avail))
            .then(|| usize::try_from(input.payload_len).ok())
            .flatten()
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut output = try_vec(count)?;
        output.resize(count, 0);
        input.read_exact(&mut output)?;
        (input.read(&mut [0; 1])? == 0)
            .then_some(output)
            .ok_or_else(|| corrupt("stream metadata frame length"))
    }

    fn stream_index(&self, number: usize, admission: u64) -> Result<IndexBlock> {
        let (e, max) = (&self.top.indexes[number], self.footer.max_payload as usize);
        let avail = admission
            .checked_sub(self.metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let payload =
            self.read_metadata_frame(e.offset, e.len, INDEX_COUNTER_BASE + e.number, max, avail)?;
        IndexBlock::parse(payload, &e.first_key, Some(avail))
    }

    fn open_block(&self, r: &BlockRef, idx: &IndexBlock, coord: u64, held: u64) -> Result<Block> {
        Block::new(
            self,
            r,
            coord,
            held.saturating_add(self.metadata_bytes() + idx.footprint()),
        )
    }

    pub(crate) fn verified_summary(&self, path: &Path) -> Result<RunMeta> {
        let admission = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        let file_bytes = std::fs::metadata(path)?.len();
        let (mut records, mut tombstones, mut record_bytes, mut tombstone_bytes, mut blocks) =
            (0u64, 0u64, 0u64, 0u64, 0u64);
        let (mut first_key, mut last_key): (Option<Vec<u8>>, Option<Vec<u8>>) = (None, None);
        for number in 0..self.top.indexes.len() {
            let index = self.stream_index(number, admission)?;
            for r in &index.blocks {
                let extra = last_key.as_ref().map_or(0, |k| k.capacity() as u64);
                let mut b = self.open_block(r, &index, admission / 4, extra)?;
                while let Some(live) = b.checked_next(r)? {
                    if last_key.as_ref().is_some_and(|old| old >= &b.key) {
                        return Err(corrupt("imported mutation run key order"));
                    }
                    records = records.checked_add(1).ok_or(Error::ReadBudgetExceeded)?;
                    let row = (b.key.len() as u64).checked_add(b.remaining);
                    record_bytes = row
                        .and_then(|n| record_bytes.checked_add(n))
                        .ok_or(Error::ReadBudgetExceeded)?;
                    if !live {
                        tombstones += 1;
                        tombstone_bytes += b.key.len() as u64;
                    }
                    first_key.get_or_insert_with(|| b.key.clone());
                    last_key = Some(b.key.clone());
                }
                blocks += 1;
            }
        }
        let exp = (
            self.footer.records,
            u64::from(self.footer.data_blocks),
            self.top.indexes.first().map(|i| i.first_key.as_slice()),
            (self.footer.records > 0).then_some(self.top.last_key.as_slice()),
        );
        if (records, blocks, first_key.as_deref(), last_key.as_deref()) != exp {
            return Err(corrupt("imported mutation run complete summary"));
        }
        let (Some(first_key), Some(last_key)) = (first_key, last_key) else {
            return Err(Error::Config(
                "disk run home: an empty run has no place".into(),
            ));
        };
        Ok(RunMeta {
            run_id: self.footer.run_id,
            records,
            tombstones,
            record_bytes,
            tombstone_bytes,
            file_bytes,
            first_key,
            last_key,
        })
    }

    pub(crate) fn stream_body(
        &self,
        wanted: &[u8],
        adm: u64,
        held: u64,
        max: u64,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let Some(number) = self.index_for(wanted) else {
            return Ok(None);
        };
        let index = self.stream_index(number, adm)?;
        let Some(slot) = index
            .may_hold(key_hash(wanted))
            .then(|| index.block_pos(wanted))
            .flatten()
        else {
            return Ok(None);
        };
        let r = &index.blocks[slot];
        let mut b = self.open_block(r, &index, adm, held)?;
        while let Some(live) = b.checked_next(r)? {
            match b.key.as_slice().cmp(wanted) {
                Ordering::Less => {}
                Ordering::Greater => return b.finish().map(|()| None),
                Ordering::Equal if !live => return b.finish().map(|()| Some(None)),
                Ordering::Equal => {
                    let lim = max
                        .min(adm)
                        .min(b.memory_available.saturating_sub(b.key.capacity() as u64));
                    let len = (b.remaining <= lim)
                        .then(|| usize::try_from(b.remaining).ok())
                        .flatten()
                        .ok_or(Error::ReadBudgetExceeded)?;
                    let mut out = try_vec(len)?;
                    copy_exact(&mut b.input, &mut out, b.remaining)?;
                    b.remaining = 0;
                    return b.finish().map(|()| Some(Some(out)));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn stream_key(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        excl: bool,
        adm: u64,
        held: u64,
    ) -> Result<Option<Vec<u8>>> {
        let start = self.index_for(lower).unwrap_or(0);
        for number in start..self.top.indexes.len() {
            let index = self.stream_index(number, adm)?;
            let first = (number == start)
                .then(|| index.block_pos(lower))
                .flatten()
                .unwrap_or(0);
            for r in &index.blocks[first..] {
                if upper.is_some_and(|end| r.first_key.as_slice() >= end) {
                    return Ok(None);
                }
                let mut b = self.open_block(r, &index, adm, held)?;
                while b.checked_next(r)?.is_some() {
                    let k = b.key.as_slice();
                    if (k > lower || !excl && k == lower) && upper.is_none_or(|end| k < end) {
                        let key = b.key.clone();
                        return b.finish().map(|()| Some(key));
                    }
                    if upper.is_some_and(|end| k >= end) {
                        return b.finish().map(|()| None);
                    }
                }
            }
        }
        Ok(None)
    }
}

fn copy_exact(input: &mut dyn Read, out: &mut dyn Write, mut bytes: u64) -> Result<()> {
    let mut buf = [0u8; 8192];
    while bytes != 0 {
        let take = bytes.min(buf.len() as u64) as usize;
        input.read_exact(&mut buf[..take])?;
        out.write_all(&buf[..take])?;
        bytes -= take as u64;
    }
    Ok(())
}
