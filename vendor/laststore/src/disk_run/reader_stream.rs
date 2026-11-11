//! Backup-only bounded selection of a key, metadata frames, and stored bodies.

use super::{IndexBlock, RunReader};
use crate::disk_run::bloom::key_hash;
use crate::disk_run::codec::corrupt;
use crate::disk_run::footer::BlockRef;
use crate::disk_run::{INDEX_COUNTER_BASE, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::{Read, Write};

pub(crate) enum SelectedBody {
    Deleted,
    Value(StreamBody),
}

pub(crate) struct StreamBody {
    block: Block,
    bytes: u64,
    pub(crate) memory_available: u64,
}

impl StreamBody {
    pub(crate) fn into_vec(mut self, maximum: u64) -> Result<Vec<u8>> {
        if self.bytes > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        let length = usize::try_from(self.bytes).map_err(|_| Error::ReadBudgetExceeded)?;
        let mut out = Vec::new();
        out.try_reserve_exact(length)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        copy_exact(&mut self.block.input, &mut out, self.bytes)?;
        self.block.remaining = 0;
        self.block.finish()?;
        Ok(out)
    }
}

pub(in crate::disk_run) struct Block {
    pub(in crate::disk_run) input: VerifiedFrameReader,
    pub(in crate::disk_run) key: Vec<u8>,
    seen: bool,
    pub(in crate::disk_run) remaining: u64,
    coordinate_bytes: u64,
    memory_available: u64,
}

impl Block {
    pub(in crate::disk_run) fn new(
        reader: &RunReader,
        reference: &BlockRef,
        coordinate_bytes: u64,
        retained: u64,
    ) -> Result<Self> {
        let max_encoded = reader
            .block_limit()
            .saturating_add(8 + crate::frame::min_encoded_len());
        if reference.len as usize > max_encoded {
            return Err(corrupt("stored block length over its limit"));
        }
        let (input, memory_available) = reader.open_verified_frame(
            reference.offset,
            reference.len,
            reference.number,
            reader.block_limit(),
            retained,
        )?;
        Ok(Self {
            input,
            key: Vec::new(),
            seen: false,
            remaining: 0,
            coordinate_bytes: coordinate_bytes.min(memory_available),
            memory_available,
        })
    }

    fn varint(&mut self) -> Result<Option<u64>> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let mut byte = [0u8; 1];
            if self.input.read(&mut byte)? == 0 {
                return if shift == 0 {
                    Ok(None)
                } else {
                    Err(corrupt("stream varint ends early"))
                };
            }
            if shift == 63 && byte[0] > 1 {
                return Err(corrupt("stream varint overflow"));
            }
            value |= u64::from(byte[0] & 0x7f) << shift;
            if byte[0] & 0x80 == 0 {
                return Ok(Some(value));
            }
        }
        Err(corrupt("stream varint overflow"))
    }

    fn length(&mut self, maximum: usize) -> Result<usize> {
        let n = self
            .varint()?
            .ok_or_else(|| corrupt("stream record ends early"))?;
        usize::try_from(n)
            .ok()
            .filter(|n| *n <= maximum)
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
            .filter(|n| *n <= self.key.len())
            .ok_or_else(|| corrupt("stream key prefix"))?;
        let suffix = self.length(MAX_RUN_KEY_BYTES - shared)?;
        let length = shared + suffix;
        if (length.max(self.key.capacity()) as u64).saturating_mul(2) > self.coordinate_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        self.key
            .reserve_exact(length.saturating_sub(self.key.len()));
        let (old_len, mut order, mut remaining, mut at) =
            (self.key.len(), Ordering::Equal, suffix, shared);
        let mut buffer = [0u8; 65536];
        while remaining != 0 {
            let n = remaining.min(buffer.len());
            self.input.read_exact(&mut buffer[..n])?;
            for &byte in &buffer[..n] {
                if order == Ordering::Equal {
                    order = self
                        .key
                        .get(at)
                        .map_or(Ordering::Greater, |old| byte.cmp(old));
                }
                if at < self.key.len() {
                    self.key[at] = byte;
                } else {
                    self.key.push(byte);
                }
                at += 1;
            }
            remaining -= n;
        }
        if order == Ordering::Equal {
            order = length.cmp(&old_len);
        }
        self.key.truncate(length);
        if self.key.is_empty() || self.seen && order != Ordering::Greater {
            return Err(corrupt("stream block key order"));
        }
        self.seen = true;
        let tag = self.length(MAX_RUN_VALUE_BYTES + 1)?;
        self.remaining = tag.saturating_sub(1) as u64;
        Ok(Some(tag != 0))
    }

    pub(in crate::disk_run) fn finish(mut self) -> Result<()> {
        while self.next()?.is_some() {}
        Ok(())
    }

    pub(in crate::disk_run) fn checked_next(
        &mut self,
        reference: &BlockRef,
    ) -> Result<Option<bool>> {
        let first = !self.seen;
        let row = self.next()?;
        if first && (row.is_none() || self.key != reference.first_key) {
            return Err(corrupt("data block does not start at its index entry"));
        }
        Ok(row)
    }
}

impl RunReader {
    pub(in crate::disk_run) fn metadata_bytes(&self) -> u64 {
        (self.top.indexes.capacity() * std::mem::size_of::<BlockRef>()
            + self.top.last_key.capacity()
            + self
                .top
                .indexes
                .iter()
                .map(|r| r.first_key.capacity())
                .sum::<usize>()) as u64
    }

    fn open_verified_frame(
        &self,
        offset: u64,
        len: u32,
        counter: u64,
        maximum: usize,
        retained: u64,
    ) -> Result<(VerifiedFrameReader, u64)> {
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        if retained
            .checked_add(2 * res.stream_buffer_bytes + 16)
            .is_none_or(|n| n > res.working_memory_bytes)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let input = VerifiedFrameReader::open(&self.file, offset, u64::from(len), &self.data_key)?;
        let header = input.header();
        if header.chunk_uuid != self.footer.run_id
            || header.counter != counter
            || input.payload_len > maximum as u64
        {
            return Err(corrupt("stream frame identity or limit"));
        }
        let available = res
            .working_memory_bytes
            .checked_sub(input.memory_bytes)
            .and_then(|n| n.checked_sub(retained))
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((input, available))
    }

    pub(in crate::disk_run) fn read_metadata_frame(
        &self,
        offset: u64,
        len: u32,
        counter: u64,
        maximum: usize,
        admission: u64,
    ) -> Result<Vec<u8>> {
        let retained = self
            .reserved_memory
            .checked_add(self.metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let (mut input, available) =
            self.open_verified_frame(offset, len, counter, maximum, retained)?;
        if input.payload_len > admission || input.payload_len > available {
            return Err(Error::ReadBudgetExceeded);
        }
        let count = usize::try_from(input.payload_len).map_err(|_| Error::ReadBudgetExceeded)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        output.resize(count, 0);
        input.read_exact(&mut output)?;
        if input.read(&mut [0; 1])? != 0 {
            return Err(corrupt("stream metadata frame length"));
        }
        Ok(output)
    }

    pub(in crate::disk_run) fn stream_index(
        &self,
        number: usize,
        admission: u64,
    ) -> Result<IndexBlock> {
        let entry = &self.top.indexes[number];
        let available = admission
            .checked_sub(self.metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let payload = self.read_metadata_frame(
            entry.offset,
            entry.len,
            INDEX_COUNTER_BASE + entry.number,
            self.block_limit(),
            available,
        )?;
        IndexBlock::parse(payload, &entry.first_key, Some(available))
    }

    pub(crate) fn stream_body(
        &self,
        wanted: &[u8],
        admission: u64,
        retained: u64,
    ) -> Result<Option<SelectedBody>> {
        let Some(number) = self.index_for(wanted) else {
            return Ok(None);
        };
        let index = self.stream_index(number, admission)?;
        if !index.may_hold(key_hash(wanted))? {
            return Ok(None);
        }
        let Some(slot) = index
            .blocks
            .partition_point(|b| b.first_key.as_slice() <= wanted)
            .checked_sub(1)
        else {
            return Ok(None);
        };
        let reference = &index.blocks[slot];
        let mut block = Block::new(
            self,
            reference,
            admission,
            retained.saturating_add(self.metadata_bytes() + index.footprint()),
        )?;
        while let Some(live) = block.checked_next(reference)? {
            match block.key.as_slice().cmp(wanted) {
                Ordering::Less => {}
                Ordering::Greater => return block.finish().map(|()| None),
                Ordering::Equal if !live => {
                    return block.finish().map(|()| Some(SelectedBody::Deleted))
                }
                Ordering::Equal => {
                    let bytes = block.remaining;
                    let memory_available = block
                        .memory_available
                        .saturating_sub(block.key.capacity() as u64);
                    return Ok(Some(SelectedBody::Value(StreamBody {
                        block,
                        bytes,
                        memory_available,
                    })));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn stream_key(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        exclusive: bool,
        admission: u64,
        retained: u64,
    ) -> Result<Option<Vec<u8>>> {
        let start = self.index_for(lower).unwrap_or(0);
        for number in start..self.index_count() {
            let index = self.stream_index(number, admission)?;
            let first = if number == start {
                index
                    .blocks
                    .partition_point(|b| b.first_key.as_slice() <= lower)
                    .saturating_sub(1)
            } else {
                0
            };
            let metadata = self.metadata_bytes() + index.footprint();
            for reference in &index.blocks[first..] {
                if upper.is_some_and(|end| reference.first_key.as_slice() >= end) {
                    return Ok(None);
                }
                let mut block = Block::new(
                    self,
                    reference,
                    admission,
                    retained.saturating_add(metadata),
                )?;
                while block.checked_next(reference)?.is_some() {
                    if (block.key.as_slice() > lower || !exclusive && block.key.as_slice() == lower)
                        && upper.is_none_or(|end| block.key.as_slice() < end)
                    {
                        let key = block.key.clone();
                        return block.finish().map(|()| Some(key));
                    }
                    if upper.is_some_and(|end| block.key.as_slice() >= end) {
                        return block.finish().map(|()| None);
                    }
                }
            }
        }
        Ok(None)
    }
}

fn copy_exact(input: &mut dyn Read, out: &mut dyn Write, mut bytes: u64) -> Result<()> {
    let mut buffer = [0u8; 65536];
    while bytes != 0 {
        let take = bytes.min(buffer.len() as u64) as usize;
        input.read_exact(&mut buffer[..take])?;
        out.write_all(&buffer[..take])?;
        bytes -= take as u64;
    }
    Ok(())
}
