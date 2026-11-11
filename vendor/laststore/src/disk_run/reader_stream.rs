//! Backup-only bounded selection of a key and its existing stored body.

use super::{IndexBlock, RunReader};
use crate::disk_run::bloom::key_hash;
use crate::disk_run::codec::corrupt;
use crate::disk_run::footer::BlockRef;
use crate::disk_run::reader_diagnostic::preflight_frame;
use crate::disk_run::reader_open::StreamBody;
use crate::disk_run::{MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::{Read, Write};

pub(crate) enum SelectedBody {
    Deleted,
    Value(StreamBody),
}

pub(in crate::disk_run) struct Block {
    pub(in crate::disk_run) input: VerifiedFrameReader,
    pub(in crate::disk_run) key: Vec<u8>,
    seen: bool,
    pub(in crate::disk_run) remaining: u64,
    coordinate_bytes: u64,
    pub(in crate::disk_run) memory_available: u64,
}

impl Block {
    pub(in crate::disk_run) fn new(
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
        let memory_available = lastdb_model::storage_policy::BACKUP_RESOURCES
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
        let metadata = self.metadata_bytes() + index_footprint(&index);
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
            let metadata = self.metadata_bytes() + index_footprint(&index);
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

fn index_footprint(index: &IndexBlock) -> u64 {
    (index.blocks.capacity() * std::mem::size_of::<BlockRef>()
        + index
            .blocks
            .iter()
            .map(|b| b.first_key.capacity())
            .sum::<usize>()
        + index.filter_bytes.capacity()) as u64
}

pub(in crate::disk_run) fn copy_exact(
    input: &mut dyn Read,
    out: &mut dyn Write,
    mut bytes: u64,
) -> Result<()> {
    let mut buffer = [0u8; 65536];
    while bytes != 0 {
        let take = bytes.min(buffer.len() as u64) as usize;
        input.read_exact(&mut buffer[..take])?;
        out.write_all(&buffer[..take])?;
        bytes -= take as u64;
    }
    Ok(())
}
