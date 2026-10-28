//! Backup-only bounded selection of a key and its existing stored body.
use super::super::{codec::corrupt, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use super::{BlockRef, RunReader};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::Read;
#[path = "stream/body.rs"]
mod body;
#[path = "stream/diagnostic.rs"]
mod diagnostic;
#[path = "stream/diagnostic_io.rs"]
mod diagnostic_io;
#[path = "stream/import.rs"]
mod import;
use body::copy_exact;
pub(crate) use body::StreamBody;
pub(crate) use diagnostic::SummaryCursor;

pub(crate) enum SelectedBody {
    Deleted,
    Value(StreamBody),
}
struct Block {
    input: VerifiedFrameReader,
    key: Vec<u8>,
    seen: bool,
    remaining: u64,
    coordinate_bytes: u64,
    memory_available: u64,
}
impl Block {
    fn new(
        reader: &RunReader,
        reference: &BlockRef,
        coordinate_bytes: u64,
        retained: u64,
    ) -> Result<Self> {
        if reference.len as usize
            > reader
                .block_limit()
                .saturating_add(8 + crate::frame::min_encoded_len())
        {
            return Err(corrupt("stored block length over its limit"));
        }
        preflight_frame(retained)?;
        let input = VerifiedFrameReader::open(
            &reader.file,
            reference.offset,
            u64::from(reference.len),
            &reader.data_key,
        )?;
        let header = input.header();
        if header.chunk_uuid != reader.footer.run_id
            || header.counter != reference.number
            || input.payload_len() > reader.block_limit() as u64
        {
            return Err(corrupt("stream block identity or length"));
        }
        let memory_available = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(input.memory_bytes())
            .and_then(|n| n.checked_sub(retained))
            .ok_or(Error::ReadBudgetExceeded)?;
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
            let n = self.input.read(&mut byte)?;
            if n == 0 {
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
        let old_len = self.key.len();
        let mut order = Ordering::Equal;
        let mut remaining = suffix;
        let mut at = shared;
        let mut buffer = [0u8; 65536];
        while remaining != 0 {
            let n = remaining.min(buffer.len());
            self.input.read_exact(&mut buffer[..n])?;
            for byte in &buffer[..n] {
                if order == Ordering::Equal {
                    order = self
                        .key
                        .get(at)
                        .map_or(Ordering::Greater, |old| byte.cmp(old));
                }
                if at < self.key.len() {
                    self.key[at] = *byte;
                } else {
                    self.key.push(*byte);
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
    fn finish(mut self) -> Result<()> {
        while self.next()?.is_some() {}
        Ok(())
    }
    fn checked_next(&mut self, reference: &BlockRef) -> Result<Option<bool>> {
        let first = !self.seen;
        let row = self.next()?;
        if first && (row.is_none() || self.key != reference.first_key) {
            return Err(corrupt("data block does not start at its index entry"));
        }
        Ok(row)
    }
}
impl RunReader {
    fn metadata_bytes(&self) -> u64 {
        (self.top.indexes.capacity() * std::mem::size_of::<BlockRef>()
            + self.top.last_key.capacity()
            + self
                .top
                .indexes
                .iter()
                .map(|r| r.first_key.capacity())
                .sum::<usize>()) as u64
    }
    pub(super) fn read_metadata_frame(
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
        preflight_frame(retained)?;
        let mut input =
            VerifiedFrameReader::open(&self.file, offset, u64::from(len), &self.data_key)?;
        let header = input.header();
        let bytes = input.payload_len();
        if header.chunk_uuid != self.footer.run_id
            || header.counter != counter
            || bytes > maximum as u64
        {
            return Err(corrupt("stream metadata frame identity or limit"));
        }
        if bytes > admission
            || bytes
                .saturating_add(input.memory_bytes())
                .saturating_add(self.metadata_bytes())
                .saturating_add(self.reserved_memory)
                > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let count = usize::try_from(bytes).map_err(|_| Error::ReadBudgetExceeded)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        output.resize(count, 0);
        input.read_exact(&mut output)?;
        let mut extra = [0; 1];
        if input.read(&mut extra)? != 0 {
            return Err(corrupt("stream metadata frame length"));
        }
        Ok(output)
    }
    fn stream_index(&self, number: usize, admission: u64) -> Result<super::IndexBlock> {
        let entry = &self.top.indexes[number];
        let available = admission
            .checked_sub(self.metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let payload = self.read_metadata_frame(
            entry.offset,
            entry.len,
            super::super::INDEX_COUNTER_BASE + entry.number,
            self.block_limit(),
            available,
        )?;
        let mut at = 0;
        let blocks = super::super::footer::get_refs_bounded(&payload, &mut at, available)?;
        if blocks.first().map(|b| &b.first_key) != Some(&entry.first_key) {
            return Err(corrupt("index block does not start at its top entry"));
        }
        let start = at;
        super::super::bloom::Filter::read(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("trailing bytes in index block"));
        }
        let allocated = blocks.capacity() * std::mem::size_of::<BlockRef>()
            + blocks.iter().map(|b| b.first_key.capacity()).sum::<usize>();
        if (payload.capacity() + allocated + payload.len() - start) as u64 > available {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(super::IndexBlock {
            blocks,
            filter_bytes: payload[start..].to_vec(),
        })
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
        if !index.may_hold(super::key_hash(wanted))? {
            return Ok(None);
        }
        let Some(slot) = index
            .blocks
            .partition_point(|b| b.first_key.as_slice() <= wanted)
            .checked_sub(1)
        else {
            return Ok(None);
        };
        let metadata = self.metadata_bytes()
            + (index.blocks.capacity() * std::mem::size_of::<BlockRef>()
                + index
                    .blocks
                    .iter()
                    .map(|b| b.first_key.capacity())
                    .sum::<usize>()
                + index.filter_bytes.capacity()) as u64;
        let reference = &index.blocks[slot];
        let mut block = Block::new(
            self,
            reference,
            admission,
            retained.saturating_add(metadata),
        )?;
        while let Some(live) = block.checked_next(reference)? {
            match block.key.as_slice().cmp(wanted) {
                Ordering::Less => {}
                Ordering::Greater => {
                    block.finish()?;
                    return Ok(None);
                }
                Ordering::Equal => {
                    if !live {
                        block.finish()?;
                        return Ok(Some(SelectedBody::Deleted));
                    }
                    let bytes = block.remaining;
                    let memory_available = block
                        .memory_available
                        .saturating_sub(block.key.capacity() as u64);
                    return Ok(Some(SelectedBody::Value(StreamBody {
                        block,
                        bytes,
                        memory_available,
                        tag: None,
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
            for reference in &index.blocks[first..] {
                if upper.is_some_and(|end| reference.first_key.as_slice() >= end) {
                    return Ok(None);
                }
                let metadata = self.metadata_bytes()
                    + (index.blocks.capacity() * std::mem::size_of::<BlockRef>()
                        + index
                            .blocks
                            .iter()
                            .map(|b| b.first_key.capacity())
                            .sum::<usize>()
                        + index.filter_bytes.capacity()) as u64;
                let mut block = Block::new(
                    self,
                    reference,
                    admission,
                    retained.saturating_add(metadata),
                )?;
                while block.checked_next(reference)?.is_some() {
                    if (block.key.as_slice() > lower || !exclusive && block.key.as_slice() == lower)
                        && upper.map_or(true, |end| block.key.as_slice() < end)
                    {
                        let key = block.key.clone();
                        block.finish()?;
                        return Ok(Some(key));
                    }
                    if upper.is_some_and(|end| block.key.as_slice() >= end) {
                        block.finish()?;
                        return Ok(None);
                    }
                }
            }
        }
        Ok(None)
    }
}

fn preflight_frame(retained: u64) -> Result<()> {
    let resources = lastdb_storage_policy::BACKUP_RESOURCES;
    let minimum = 2 * resources.stream_buffer_bytes + 16;
    if retained
        .checked_add(minimum)
        .map_or(true, |n| n > resources.working_memory_bytes)
    {
        Err(Error::ReadBudgetExceeded)
    } else {
        Ok(())
    }
}
