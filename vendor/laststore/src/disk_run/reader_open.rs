//! RunReader open paths, summary verification, and checkpoint-only length cursors.

use super::codec::{corrupt, get_len, get_slice};
use super::footer::{Footer, Top, FOOTER_BYTES};
use super::reader::stream::Block;
use super::reader::{IndexBlock, RunReader};
use super::{
    RunSummary, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::frame::PreparedFrameKey;
use crate::{Error, Result};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use uuid::Uuid;

const TAIL_BYTES: usize = 512;
const FRAME_WORKSPACE: u64 = 8 * 1024 * 1024;

impl RunReader {
    pub(crate) fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_inner(path, data_key, None, 0)
    }

    pub(crate) fn open_bounded(path: &Path, data_key: &[u8; 32], bytes: u64) -> Result<Self> {
        Self::open_inner(path, data_key, Some(bytes), 0)
    }

    pub(crate) fn open_bounded_with_reserve(
        path: &Path,
        data_key: &[u8; 32],
        bytes: u64,
        reserved: u64,
    ) -> Result<Self> {
        Self::open_inner(path, data_key, Some(bytes), reserved)
    }

    fn open_inner(
        path: &Path,
        data_key: &[u8; 32],
        bytes: Option<u64>,
        reserved: u64,
    ) -> Result<Self> {
        let named = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(RUN_SUFFIX))
            .and_then(|stem| Uuid::parse_str(stem).ok())
            .ok_or_else(|| corrupt("run file name is not <run id>.ldrun"))?;
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let mut raw = [0u8; FOOTER_BYTES];
        let tail = if bytes.is_none() {
            let count = len.min(TAIL_BYTES as u64) as usize;
            let mut tail = vec![0u8; count];
            file.read_exact_at(&mut tail, len - count as u64)?;
            raw.copy_from_slice(&tail[count - FOOTER_BYTES..]);
            Some(tail)
        } else {
            file.read_exact_at(&mut raw, len - FOOTER_BYTES as u64)?;
            None
        };
        let footer = Footer::decode(&raw)?;
        if bytes.is_some_and(|n| u64::from(footer.top_len).saturating_mul(4) > n) {
            return Err(Error::ReadBudgetExceeded);
        }
        if footer.run_id != named {
            return Err(corrupt("run file name and run id disagree"));
        }
        if footer
            .top_offset
            .checked_add(u64::from(footer.top_len))
            .and_then(|e| e.checked_add(FOOTER_BYTES as u64))
            != Some(len)
        {
            return Err(corrupt("top block is not before the footer"));
        }
        let frame_key = bytes
            .is_none()
            .then(|| PreparedFrameKey::new(data_key, footer.run_id).map(Box::new))
            .transpose()?;
        let mut reader = Self {
            reserved_memory: reserved,
            file,
            data_key: *data_key,
            frame_key,
            footer,
            top: Top::default(),
        };
        reader.top = reader.read_top(bytes, tail.as_deref(), len)?;
        if !reader.top.agrees_with(&footer) {
            return Err(corrupt("footer and top block disagree"));
        }
        Ok(reader)
    }

    fn read_top(&self, admission: Option<u64>, tail: Option<&[u8]>, len: u64) -> Result<Top> {
        let f = self.footer;
        let payload = if let Some(adm) = admission {
            self.read_metadata_frame(f.top_offset, f.top_len, TOP_COUNTER, MAX_TOP_BYTES, adm)?
        } else if let Some(frame) = tail.and_then(|t| {
            let at = usize::try_from(f.top_offset.checked_sub(len.checked_sub(t.len() as u64)?)?)
                .ok()?;
            t.get(at..at.checked_add(f.top_len as usize)?)
        }) {
            self.decode_checked_frame(frame, TOP_COUNTER, MAX_TOP_BYTES, None)?
        } else {
            self.read_frame(f.top_offset, f.top_len, TOP_COUNTER, MAX_TOP_BYTES, None)?
        };
        Top::decode_bounded(&payload, admission.unwrap_or(u64::MAX))
    }

    pub(crate) fn verified_summary(&self, path: &Path) -> Result<RunSummary> {
        let admission = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        let mut summary = RunSummary {
            path: path.to_owned(),
            run_id: self.footer.run_id,
            records: 0,
            tombstones: 0,
            record_bytes: 0,
            tombstone_bytes: 0,
            file_bytes: std::fs::metadata(path)?.len(),
            first_key: None,
            last_key: None,
        };
        let mut blocks = 0u64;
        for number in 0..self.index_count() {
            let index = self.stream_index(number, admission)?;
            let retained = self.metadata_bytes()
                + index.footprint()
                + summary.last_key.as_ref().map_or(0, |k| k.capacity() as u64);
            for reference in &index.blocks {
                let mut block = Block::new(self, reference, admission / 4, retained)?;
                while let Some(value) = block.checked_next(reference)? {
                    if summary
                        .last_key
                        .as_ref()
                        .is_some_and(|old| old >= &block.key)
                    {
                        return Err(corrupt("imported mutation run key order"));
                    }
                    summary.records = summary
                        .records
                        .checked_add(1)
                        .ok_or(Error::ReadBudgetExceeded)?;
                    summary.record_bytes = summary
                        .record_bytes
                        .checked_add(block.key.len() as u64)
                        .and_then(|n| n.checked_add(block.remaining))
                        .ok_or(Error::ReadBudgetExceeded)?;
                    if !value {
                        summary.tombstones += 1;
                        summary.tombstone_bytes += block.key.len() as u64;
                    }
                    if summary.first_key.is_none() {
                        summary.first_key = Some(block.key.clone());
                    }
                    summary.last_key = Some(block.key.clone());
                }
                blocks += 1;
            }
        }
        let expected_first = self.top.indexes.first().map(|i| i.first_key.as_slice());
        let expected_last = (self.footer.records > 0).then_some(self.top.last_key.as_slice());
        if summary.records != self.footer.records
            || blocks != u64::from(self.footer.data_blocks)
            || summary.first_key.as_deref() != expected_first
            || summary.last_key.as_deref() != expected_last
        {
            return Err(corrupt("imported mutation run complete summary"));
        }
        Ok(summary)
    }
}

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

    pub(crate) fn next_record(&mut self) -> Result<Option<(Vec<u8>, Option<u64>)>> {
        if self.at == self.payload.len() && !self.load_block()? {
            return Ok(None);
        }
        let shared = get_len(&self.payload, &mut self.at, self.key.len())?;
        let suffix_len = get_len(&self.payload, &mut self.at, MAX_RUN_KEY_BYTES - shared)?;
        let length = shared + suffix_len;
        if (length.max(self.key.capacity()) as u64).saturating_mul(2) > self.allowance / 4 {
            return Err(Error::ReadBudgetExceeded);
        }
        let suffix = get_slice(&self.payload, &mut self.at, suffix_len)?;
        if length == 0 || self.seen && self.key[shared..].cmp(suffix).is_ge() {
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
        let bytes = get_len(&self.payload, &mut self.at, MAX_RUN_VALUE_BYTES + 1)?.checked_sub(1);
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
                .is_none_or(|idx| self.next_block == idx.blocks.len())
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
            let Some(reference) = self
                .index
                .as_ref()
                .expect("index")
                .blocks
                .get(self.next_block)
            else {
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
                None,
            )?;
            (self.at, self.seen) = (0, false);
            self.key.clear();
            self.first_key = Some(reference.first_key.clone());
            if self.payload.is_empty() {
                return Err(corrupt("diagnostic empty block"));
            }
            return Ok(true);
        }
    }
}
