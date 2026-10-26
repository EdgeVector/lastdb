//! Writes one run file from records in strictly ascending key order.

use super::bloom::{key_hash, put_filter};
use super::codec::{corrupt, put_record, record_len};
use super::footer::{put_refs, BlockRef, Footer, Top};
use super::{
    RunOptions, INDEX_COUNTER_BASE, MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES,
    MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::commit_timing as timing;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{encode_frame, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// What a finished run holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    /// The complete run file.
    pub path: PathBuf,
    /// Run id; it also selects the run's derived block key.
    pub run_id: Uuid,
    /// Records, tombstones included.
    pub records: u64,
    /// Tombstones among the records.
    pub tombstones: u64,
    /// Key and value bytes of the records before compression; a tombstone
    /// counts its key.
    pub record_bytes: u64,
    /// Key bytes of the tombstones.
    pub tombstone_bytes: u64,
    /// Size of the run file.
    pub file_bytes: u64,
    /// Smallest key, or `None` for an empty run.
    pub first_key: Option<Vec<u8>>,
    /// Largest key, or `None` for an empty run.
    pub last_key: Option<Vec<u8>>,
}

/// Builds one run in a hidden temporary file, then makes it durable and
/// renames it to `<run id>.ldrun`. A writer dropped before
/// [`RunWriter::finish`] deletes its temporary file. After a crash the
/// `.<run id>.tmp` file stays; the owner of the run directory removes such
/// files when it opens the directory.
pub struct RunWriter {
    dir: PathBuf,
    tmp: PathBuf,
    file: File,
    data_key: [u8; 32],
    run_id: Uuid,
    options: RunOptions,
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
    /// Set when a block write fails: the block's records are gone and the
    /// file offset is unknown, so the writer refuses every later call.
    failed: bool,
    finished: bool,
}

impl RunWriter {
    /// Starts a new run in `dir` with a fresh random run id.
    pub fn create(dir: &Path, data_key: &[u8; 32], options: RunOptions) -> Result<Self> {
        let _timing = timing::stage("v2_run_create_us");
        let sizes = 512..=MAX_RUN_VALUE_BYTES;
        if !sizes.contains(&options.block_bytes) || !sizes.contains(&options.index_bytes) {
            return Err(Error::Config("disk run: block sizes out of range".into()));
        }
        let run_id = Uuid::new_v4();
        let tmp = dir.join(format!(".{run_id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        timing::count("v2_run_files_created", 1);
        Ok(Self {
            dir: dir.to_path_buf(),
            tmp,
            file,
            data_key: *data_key,
            run_id,
            options,
            offset: 0,
            block: Vec::with_capacity(options.block_bytes + 1024),
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
        })
    }

    /// Run id of the file this writer builds.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Records pushed so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Bytes written to the file so far, plus the open block before its
    /// compression. The finished file adds its index, top block and footer.
    pub fn bytes_written(&self) -> u64 {
        self.offset + self.block.len() as u64
    }

    /// Appends one record. `None` writes a tombstone. Keys must be strictly
    /// ascending across the whole run.
    pub fn push(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
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
            && self.block.len() + record_len(&self.block_prev, key, value)
                > self.options.block_bytes
        {
            self.flush_block()?;
        }
        if self.block.is_empty() {
            self.block_prev.clear();
            self.block_first = key.to_vec();
        }
        let record_timing = timing::stage("v2_record_encoding_us");
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
        drop(record_timing);
        Ok(())
    }

    fn refuse_after_failure(&self) -> Result<()> {
        if self.failed {
            return Err(Error::WriteCapacity(
                "disk run: an earlier block write failed; start a new run".into(),
            ));
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let written = self.write_frame_once(counter, payload);
        self.failed |= written.is_err();
        written
    }

    fn write_frame_once(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let header = FrameHeader {
            chunk_uuid: self.run_id,
            shard: 0,
            start_csn: 0,
            counter,
        };
        let frame = timing::during("v2_frame_encode_us", || {
            encode_frame(&self.data_key, header, payload)
        })?;
        let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
        timing::during("v2_run_write_us", || self.file.write_all(&frame))?;
        timing::count("v2_run_frames", 1);
        timing::count("v2_run_frame_input_bytes", payload.len() as u64);
        timing::count("v2_run_written_bytes", frame.len() as u64);
        let offset = self.offset;
        self.offset += frame.len() as u64;
        Ok((offset, len))
    }

    fn flush_block(&mut self) -> Result<()> {
        timing::count("v2_run_data_blocks", 1);
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
        if self.index_bytes >= self.options.index_bytes || self.index.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let index_timing = timing::stage("v2_index_encoding_us");
        let mut payload = Vec::with_capacity(self.index_bytes + self.index_hashes.len() * 2);
        put_refs(&mut payload, &self.index);
        put_filter(
            &mut payload,
            &self.index_hashes,
            self.options.bloom_bits_per_key,
        );
        drop(index_timing);
        timing::count("v2_run_index_blocks", 1);
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

    /// Writes the last blocks, the top block and the footer, makes the file
    /// durable, and renames it into place.
    ///
    /// An error before the rename leaves no run. An error from the final
    /// directory sync comes after the rename: the run file is complete but
    /// its name may not be durable, so the caller must treat that run as
    /// unknown rather than absent and must not publish it.
    pub fn finish(mut self) -> Result<RunSummary> {
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
        let top = Top {
            records: self.records,
            data_blocks: self.data_blocks,
            max_payload: u64::from(max_payload),
            indexes: std::mem::take(&mut self.top),
            last_key: self.last_key.clone().unwrap_or_default(),
        };
        let top_entries = top.indexes.len();
        let top = timing::during("v2_top_encoding_us", || top.encode());
        if top.len() > MAX_TOP_BYTES || top_entries > MAX_INDEX_ENTRIES {
            return Err(Error::WriteCapacity(
                "disk run: top block over its read limit; write smaller runs".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top)?;
        timing::count("v2_run_top_blocks", 1);
        let footer = Footer {
            run_id: self.run_id,
            top_offset,
            top_len,
            records: self.records,
            max_payload,
            data_blocks,
        };
        timing::during("v2_run_write_us", || self.file.write_all(&footer.encode()))?;
        timing::count("v2_run_written_bytes", super::footer::FOOTER_BYTES as u64);
        timing::count("v2_run_file_flushes", 1);
        timing::during("v2_run_file_flush_us", || sync_dirty_file(&self.file))?;
        let path = self.dir.join(format!("{}{RUN_SUFFIX}", self.run_id));
        timing::during("v2_run_rename_us", || fs::rename(&self.tmp, &path))?;
        timing::count("v2_run_directory_flushes", 1);
        timing::during("v2_run_directory_flush_us", || {
            sync_directory(&File::open(&self.dir)?)
        })?;
        self.finished = true;
        timing::count("v2_run_records", self.records);
        timing::count("v2_run_tombstones", self.tombstones);
        timing::count("v2_run_record_bytes", self.record_bytes);
        Ok(RunSummary {
            path,
            run_id: self.run_id,
            records: self.records,
            tombstones: self.tombstones,
            record_bytes: self.record_bytes,
            tombstone_bytes: self.tombstone_bytes,
            file_bytes: self.offset + super::footer::FOOTER_BYTES as u64,
            first_key: self.first_key.take(),
            last_key: self.last_key.take(),
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
