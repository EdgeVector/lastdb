//! Writes one run file from records in strictly ascending key order.

use super::bloom::{key_hash, put_filter};
use super::codec::{corrupt, put_record, record_len};
use super::footer::{put_refs, BlockRef, Footer, MergeSpace, Top, FOOTER_BYTES};
use super::{
    INDEX_COUNTER_BASE, MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES, MAX_TOP_BYTES,
    RUN_SUFFIX, TOP_COUNTER,
};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{encode_frame, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const BLOCK_BYTES: usize = 16 * 1024;
const INDEX_BYTES: usize = 16 * 1024;
const BLOOM_BITS_PER_KEY: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunSummary {
    pub(crate) path: PathBuf,
    pub(crate) run_id: Uuid,
    pub(crate) records: u64,
    pub(crate) tombstones: u64,
    pub(crate) record_bytes: u64,
    pub(crate) tombstone_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) first_key: Option<Vec<u8>>,
    pub(crate) last_key: Option<Vec<u8>>,
}

pub(crate) struct PreparedRun {
    writer: RunWriter,
    pub(crate) summary: RunSummary,
}

impl PreparedRun {
    pub(crate) fn finish(mut self) -> Result<RunSummary> {
        sync_dirty_file(&self.writer.file)?;
        let path = &self.summary.path;
        fs::rename(&self.writer.tmp, path)?;
        if let Some(space) = &self.writer.space {
            space.output(path);
        }
        sync_directory(&self.writer.dir)?;
        self.writer.finished = true;
        Ok(self.summary)
    }
}

pub(crate) struct RunWriter {
    dir: PathBuf,
    tmp: PathBuf,
    file: File,
    data_key: [u8; 32],
    run_id: Uuid,
    offset: u64,
    block: Vec<u8>,
    block_prev: Vec<u8>,
    block_first: Vec<u8>,
    data_blocks: u64,
    index: Vec<BlockRef>,
    index_bytes: usize,
    index_hashes: Vec<u64>,
    top: Vec<BlockRef>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    records: u64,
    tombstones: u64,
    record_bytes: u64,
    tombstone_bytes: u64,
    max_payload: usize,
    failed: bool,
    finished: bool,
    space: Option<MergeSpace>,
}

impl RunWriter {
    pub(crate) fn create(dir: &Path, data_key: &[u8; 32]) -> Result<Self> {
        let run_id = Uuid::new_v4();
        let tmp = dir.join(format!(".{run_id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            tmp,
            file,
            data_key: *data_key,
            run_id,
            offset: 0,
            block: Vec::with_capacity(BLOCK_BYTES + 1024),
            block_prev: Vec::new(),
            block_first: Vec::new(),
            data_blocks: 0,
            index: Vec::new(),
            index_bytes: 0,
            index_hashes: Vec::new(),
            top: Vec::new(),
            first_key: None,
            last_key: None,
            records: 0,
            tombstones: 0,
            record_bytes: 0,
            tombstone_bytes: 0,
            max_payload: 0,
            failed: false,
            finished: false,
            space: None,
        })
    }

    pub(crate) fn create_reserved(
        dir: &Path,
        data_key: &[u8; 32],
        space: &MergeSpace,
    ) -> Result<Self> {
        let mut writer = Self::create(dir, data_key)?;
        writer.space = Some(space.clone());
        Ok(writer)
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.offset + self.block.len() as u64
    }

    pub(crate) fn push(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.refuse_after_failure()?;
        if key.is_empty() || key.len() > MAX_RUN_KEY_BYTES {
            return Err(Error::Config("disk run: key size out of range".into()));
        }
        if value.is_some_and(|v| v.len() > MAX_RUN_VALUE_BYTES) {
            return Err(Error::Config(
                "disk run: value larger than the run limit".into(),
            ));
        }
        if self.last_key.as_deref().is_some_and(|last| last >= key) {
            return Err(corrupt("keys pushed out of order"));
        }
        if !self.block.is_empty()
            && self.block.len() + record_len(&self.block_prev, key, value) > BLOCK_BYTES
        {
            self.flush_block()?;
        }
        if self.block.is_empty() {
            self.block_prev.clear();
            self.block_first = key.to_vec();
        }
        put_record(&mut self.block, &self.block_prev, key, value);
        self.block_prev.clear();
        self.block_prev.extend_from_slice(key);
        self.index_hashes.push(key_hash(key));
        self.records += 1;
        self.record_bytes += (key.len() + value.map_or(0, <[u8]>::len)) as u64;
        if value.is_none() {
            self.tombstones += 1;
            self.tombstone_bytes += key.len() as u64;
        }
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
        Ok(())
    }

    fn refuse_after_failure(&self) -> Result<()> {
        if self.failed {
            Err(Error::WriteCapacity(
                "disk run: an earlier block write failed; start a new run".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(space) = &self.space {
            space.admit_run(bytes.len() as u64)?;
        }
        self.file.write_all(bytes)?;
        if let Some(space) = &self.space {
            space.materialize(bytes.len() as u64)?;
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let written = (|| {
            let header = FrameHeader {
                chunk_uuid: self.run_id,
                shard: 0,
                start_csn: 0,
                counter,
            };
            let frame = encode_frame(&self.data_key, header, payload)?;
            let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
            self.write_bytes(&frame)?;
            let offset = self.offset;
            self.offset += frame.len() as u64;
            Ok((offset, len))
        })();
        self.failed |= written.is_err();
        written
    }

    fn flush_block(&mut self) -> Result<()> {
        let payload = std::mem::take(&mut self.block);
        let (offset, len) = self.write_frame(self.data_blocks, &payload)?;
        self.max_payload = self.max_payload.max(payload.len());
        self.block = payload;
        self.block.clear();
        let first_key = std::mem::take(&mut self.block_first);
        self.index_bytes += first_key.len() + 16;
        self.index.push(BlockRef {
            first_key,
            number: self.data_blocks,
            offset,
            len,
        });
        self.data_blocks += 1;
        if self.index_bytes >= INDEX_BYTES || self.index.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let mut payload = Vec::with_capacity(self.index_bytes + self.index_hashes.len() * 2);
        put_refs(&mut payload, &self.index);
        put_filter(&mut payload, &self.index_hashes, BLOOM_BITS_PER_KEY);
        let number = self.top.len() as u64;
        let (offset, len) = self.write_frame(INDEX_COUNTER_BASE + number, &payload)?;
        self.max_payload = self.max_payload.max(payload.len());
        self.top.push(BlockRef {
            first_key: self.index[0].first_key.clone(),
            number,
            offset,
            len,
        });
        self.index.clear();
        self.index_hashes.clear();
        self.index_bytes = 0;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RunSummary> {
        self.prepare_finish()?.finish()
    }

    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        self.refuse_after_failure()?;
        if !self.block.is_empty() {
            self.flush_block()?;
        }
        if !self.index.is_empty() {
            self.flush_index()?;
        }
        let max_payload =
            u32::try_from(self.max_payload).map_err(|_| corrupt("block over 4 GiB"))?;
        let data_blocks =
            u32::try_from(self.data_blocks).map_err(|_| corrupt("too many data blocks"))?;
        let indexes = std::mem::take(&mut self.top);
        let top_entries = indexes.len();
        let top = Top {
            records: self.records,
            data_blocks: self.data_blocks,
            max_payload: u64::from(max_payload),
            indexes,
            last_key: self.last_key.clone().unwrap_or_default(),
        }
        .encode();
        if top.len() > MAX_TOP_BYTES || top_entries > MAX_INDEX_ENTRIES {
            return Err(Error::WriteCapacity(
                "disk run: top block over its read limit; write smaller runs".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top)?;
        let footer = Footer {
            run_id: self.run_id,
            top_offset,
            top_len,
            records: self.records,
            max_payload,
            data_blocks,
        };
        self.write_bytes(&footer.encode())?;
        let summary = RunSummary {
            path: self.dir.join(format!("{}{RUN_SUFFIX}", self.run_id)),
            run_id: self.run_id,
            records: self.records,
            tombstones: self.tombstones,
            record_bytes: self.record_bytes,
            tombstone_bytes: self.tombstone_bytes,
            file_bytes: self.offset + FOOTER_BYTES as u64,
            first_key: self.first_key.take(),
            last_key: self.last_key.take(),
        };
        Ok(PreparedRun {
            writer: self,
            summary,
        })
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = fs::remove_file(&self.tmp);
        }
    }
}
