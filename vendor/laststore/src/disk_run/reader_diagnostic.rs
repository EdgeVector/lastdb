//! Checkpoint-only record lengths, without decoded bodies or a key inventory.

use super::bloom::Filter;
use super::codec::{corrupt, get_len, get_slice};
use super::footer::{get_refs_bounded, BlockRef};
use super::reader::{IndexBlock, RunReader};
use super::{INDEX_COUNTER_BASE, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::Read;
use std::path::Path;

pub(crate) type SummaryRecord = (Vec<u8>, Option<u64>);

pub(crate) struct SummaryCursor {
    reader: RunReader,
    allowance: u64,
    index: Option<IndexBlock>,
    next_index: usize,
    next_block: usize,
    payload: Vec<u8>,
    at: usize,
    key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    seen: bool,
}

const FRAME_WORKSPACE: u64 = 8 * 1024 * 1024;

impl SummaryCursor {
    pub(crate) const fn frame_workspace_bytes() -> u64 {
        FRAME_WORKSPACE
    }

    pub(crate) fn open(path: &Path, key: &[u8; 32], allowance: u64) -> Result<Self> {
        let reader = RunReader::open_bounded(path, key, allowance / 8)?;
        if u64::from(reader.footer.max_payload) > allowance / 8 {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(Self {
            reader,
            allowance,
            index: None,
            next_index: 0,
            next_block: 0,
            payload: Vec::new(),
            at: 0,
            key: Vec::new(),
            first_key: None,
            seen: false,
        })
    }

    pub(crate) fn next_record(&mut self) -> Result<Option<SummaryRecord>> {
        if self.at == self.payload.len() && !self.load_block()? {
            return Ok(None);
        }
        let shared = get_len(&self.payload, &mut self.at, self.key.len())?;
        let suffix = get_len(&self.payload, &mut self.at, MAX_RUN_KEY_BYTES - shared)?;
        let length = shared + suffix;
        if (length.max(self.key.capacity()) as u64).saturating_mul(2) > self.allowance / 4 {
            return Err(Error::ReadBudgetExceeded);
        }
        let suffix = get_slice(&self.payload, &mut self.at, suffix)?;
        let ascending = !self.seen || compare_key(&self.key, shared, suffix).is_lt();
        if !ascending || length == 0 {
            return Err(corrupt("diagnostic record order"));
        }
        self.key.truncate(shared);
        self.key
            .try_reserve_exact(length.saturating_sub(self.key.len()))
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.key.extend_from_slice(suffix);
        if self.first_key.take().is_some_and(|first| first != self.key) {
            return Err(corrupt("diagnostic block first key"));
        }
        self.seen = true;
        let tag = get_len(&self.payload, &mut self.at, MAX_RUN_VALUE_BYTES + 1)?;
        let bytes = tag.checked_sub(1);
        if let Some(bytes) = bytes {
            get_slice(&self.payload, &mut self.at, bytes)?;
        }
        Ok(Some((self.key.clone(), bytes.map(|n| n as u64))))
    }

    fn load_block(&mut self) -> Result<bool> {
        self.payload = Vec::new();
        loop {
            if self
                .index
                .as_ref()
                .is_none_or(|index| self.next_block == index.blocks.len())
            {
                self.index = None;
                if self.next_index == self.reader.index_count() {
                    return Ok(false);
                }
                self.index = Some(
                    self.reader
                        .stream_index(self.next_index, self.allowance / 4)?,
                );
                self.next_index += 1;
                self.next_block = 0;
            }
            let index = self.index.as_ref().expect("diagnostic index opened");
            let Some(reference) = index.blocks.get(self.next_block) else {
                continue;
            };
            if self.seen && reference.first_key <= self.key {
                return Err(corrupt("diagnostic cross-block key order"));
            }
            if reference.first_key.len() as u64 > self.allowance / 8 {
                return Err(Error::ReadBudgetExceeded);
            }
            self.next_block += 1;
            self.payload = self.reader.read_frame(
                reference.offset,
                reference.len,
                reference.number,
                self.reader.block_limit(),
            )?;
            self.at = 0;
            self.key.clear();
            self.seen = false;
            self.first_key = Some(reference.first_key.clone());
            if self.payload.is_empty() {
                return Err(corrupt("diagnostic empty block"));
            }
            return Ok(true);
        }
    }
}

fn compare_key(before: &[u8], shared: usize, suffix: &[u8]) -> Ordering {
    before[shared..].cmp(suffix)
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
                > lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes
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
        let mut at = 0;
        let blocks = get_refs_bounded(&payload, &mut at, available)?;
        if blocks.first().map(|b| &b.first_key) != Some(&entry.first_key) {
            return Err(corrupt("index block does not start at its top entry"));
        }
        let start = at;
        Filter::read(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("trailing bytes in index block"));
        }
        let allocated = blocks.capacity() * std::mem::size_of::<BlockRef>()
            + blocks.iter().map(|b| b.first_key.capacity()).sum::<usize>();
        if (payload.capacity() + allocated + payload.len() - start) as u64 > available {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(IndexBlock {
            blocks,
            filter_bytes: payload[start..].to_vec(),
        })
    }
}

pub(in crate::disk_run) fn preflight_frame(retained: u64) -> Result<()> {
    let resources = lastdb_model::storage_policy::BACKUP_RESOURCES;
    let minimum = 2 * resources.stream_buffer_bytes + 16;
    if retained
        .checked_add(minimum)
        .is_none_or(|n| n > resources.working_memory_bytes)
    {
        Err(Error::ReadBudgetExceeded)
    } else {
        Ok(())
    }
}
