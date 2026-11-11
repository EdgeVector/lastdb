//! RunReader open paths, bounded value materialization, and mutation-run summary verification.

use super::codec::corrupt;
use super::footer::{BlockRef, Footer, Top, FOOTER_BYTES};
use super::reader::stream::{copy_exact, Block};
use super::reader::RunReader;
use super::{RunSummary, MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER};
use crate::frame::PreparedFrameKey;
use crate::{Error, Result};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use uuid::Uuid;

const TAIL_BYTES: usize = 512;

impl RunReader {
    /// Opens a run and checks its footer and top block.
    pub fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
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
            .and_then(|stem| Uuid::parse_str(stem).ok());
        let Some(named) = named else {
            return Err(corrupt("run file name is not <run id>.ldrun"));
        };
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let (footer, tail) = read_footer(&file, len, bytes.is_none())?;
        if bytes.is_some_and(|n| u64::from(footer.top_len).saturating_mul(4) > n) {
            return Err(Error::ReadBudgetExceeded);
        }
        if footer.run_id != named {
            return Err(corrupt("run file name and run id disagree"));
        }
        let end = footer
            .top_offset
            .checked_add(u64::from(footer.top_len))
            .and_then(|end| end.checked_add(FOOTER_BYTES as u64));
        if end != Some(len) {
            return Err(corrupt("top block is not before the footer"));
        }
        let mut reader = Self {
            reserved_memory: reserved,
            file,
            data_key: *data_key,
            frame_key: if bytes.is_none() {
                Some(Box::new(PreparedFrameKey::new(data_key, footer.run_id)?))
            } else {
                None
            },
            footer,
            top: Top {
                records: 0,
                data_blocks: 0,
                max_payload: 0,
                indexes: Vec::new(),
                last_key: Vec::new(),
            },
        };
        reader.top = reader.read_top(bytes, tail.as_deref(), len)?;
        if !reader.top.agrees_with(&footer) {
            return Err(corrupt("footer and top block disagree"));
        }
        Ok(reader)
    }

    fn read_top(&self, admission: Option<u64>, tail: Option<&[u8]>, len: u64) -> Result<Top> {
        let footer = self.footer;
        let payload = match admission {
            Some(admission) => self.read_metadata_frame(
                footer.top_offset,
                footer.top_len,
                TOP_COUNTER,
                MAX_TOP_BYTES,
                admission,
            )?,
            None => match cached_top(tail, len, &footer) {
                Some(frame) => {
                    self.decode_checked_frame(frame, TOP_COUNTER, MAX_TOP_BYTES, None)?
                }
                None => self.read_frame(
                    footer.top_offset,
                    footer.top_len,
                    TOP_COUNTER,
                    MAX_TOP_BYTES,
                )?,
            },
        };
        match admission {
            Some(admission) => Top::decode_bounded(&payload, admission),
            None => Top::decode(&payload),
        }
    }

    pub(crate) fn verified_summary(&self, path: &Path) -> Result<RunSummary> {
        let admission = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        let mut summary = RunSummary {
            path: path.to_owned(),
            run_id: self.run_id(),
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
                + (index.blocks.capacity() * std::mem::size_of::<BlockRef>()) as u64
                + index
                    .blocks
                    .iter()
                    .map(|r| r.first_key.capacity() as u64)
                    .sum::<u64>()
                + index.filter_bytes.capacity() as u64
                + summary
                    .last_key
                    .as_ref()
                    .map_or(0, |key| key.capacity() as u64);
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
        if summary.records != self.records()
            || blocks != u64::from(self.footer.data_blocks)
            || summary.first_key.as_deref() != self.first_key()
            || summary.last_key.as_deref() != self.last_key()
        {
            return Err(corrupt("imported mutation run complete summary"));
        }
        Ok(summary)
    }
}

pub(crate) struct StreamBody {
    pub(in crate::disk_run) block: Block,
    pub(in crate::disk_run) bytes: u64,
    pub(in crate::disk_run) memory_available: u64,
}

impl StreamBody {
    pub(crate) fn allocation_limit(&self) -> u64 {
        self.memory_available
    }

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

fn read_footer(file: &File, len: u64, ordinary: bool) -> Result<(Footer, Option<Vec<u8>>)> {
    let mut raw = [0u8; FOOTER_BYTES];
    let tail = if ordinary {
        let count = len.min(TAIL_BYTES as u64) as usize;
        let mut tail = vec![0u8; count];
        file.read_exact_at(&mut tail, len - count as u64)?;
        raw.copy_from_slice(&tail[count - FOOTER_BYTES..]);
        Some(tail)
    } else {
        file.read_exact_at(&mut raw, len - FOOTER_BYTES as u64)?;
        None
    };
    Ok((Footer::decode(&raw)?, tail))
}

fn cached_top<'a>(tail: Option<&'a [u8]>, len: u64, footer: &Footer) -> Option<&'a [u8]> {
    let tail = tail?;
    let start = len.checked_sub(tail.len() as u64)?;
    let offset = usize::try_from(footer.top_offset.checked_sub(start)?).ok()?;
    let end = offset.checked_add(footer.top_len as usize)?;
    tail.get(offset..end)
}
