//! Run file writer and merge space reservations.

use super::codec::{corrupt, put_record, record_len};
use super::footer::{put_refs, BlockRef, Footer, Top, FOOTER_BYTES};
use super::{
    RunMeta, INDEX_COUNTER_BASE, MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES,
    MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::bloom;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::encode_frame;
use crate::space::{reserve_space, SpacePolicy, SpaceReservation};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

const BLOCK_BYTES: usize = 16 * 1024;
const INDEX_BYTES: usize = 16 * 1024;
const BLOOM_BITS_PER_KEY: u32 = 10;
const PUBLICATION_BYTES: u64 = 4096;
const OUTPUT_CHUNK_BYTES: u64 = 1 << 20;

#[derive(Clone)]
pub(crate) struct MergeSpace(Arc<Mutex<PendingMergeSpace>>);

struct PendingMergeSpace {
    policy: SpacePolicy,
    reservation: SpaceReservation,
    outputs: Vec<PathBuf>,
    keep_outputs: bool,
}

impl MergeSpace {
    pub(crate) fn new(root: &Path, policy: &SpacePolicy) -> Result<Self> {
        let mut merge_policy = policy.clone();
        merge_policy.replacement_reserve_bytes = 0;
        let reservation = reserve_space(root, &merge_policy, PUBLICATION_BYTES)?;
        Ok(Self(Arc::new(Mutex::new(PendingMergeSpace {
            policy: merge_policy,
            reservation,
            outputs: Vec::new(),
            keep_outputs: false,
        }))))
    }

    fn lock(&self) -> MutexGuard<'_, PendingMergeSpace> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn admit(&self, bytes: u64, chunked: bool) -> Result<()> {
        let ovf = || Error::WriteCapacity("disk run: replacement reservation overflow".into());
        let req = if chunked {
            bytes.checked_add(PUBLICATION_BYTES).ok_or_else(ovf)?
        } else {
            bytes
        };
        let mut pending = self.lock();
        if req <= pending.reservation.reserved_bytes {
            return Ok(());
        }
        let extra = if chunked {
            let chunk = pending
                .reservation
                .reserved_bytes
                .checked_add(OUTPUT_CHUNK_BYTES)
                .ok_or_else(ovf)?
                .max(req);
            let nospc = rustix::io::Errno::NOSPC.raw_os_error();
            let policy = pending.policy.clone();
            match pending.reservation.reserve_extension(&policy, chunk) {
                Err(Error::Io(io)) if io.raw_os_error() == Some(nospc) => {
                    pending.reservation.reserve_extension(&policy, req)?
                }
                res => res?,
            }
        } else {
            let policy = pending.policy.clone();
            pending.reservation.reserve_extension(&policy, req)?
        };
        pending.reservation.absorb(extra);
        Ok(())
    }

    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.lock().reservation.materialize(bytes)
    }

    pub(crate) fn output(&self, path: &Path) {
        self.lock().outputs.push(path.to_path_buf());
    }

    pub(crate) fn keep_outputs(&self) {
        self.lock().keep_outputs = true;
    }
}

impl Drop for PendingMergeSpace {
    fn drop(&mut self) {
        if !self.keep_outputs {
            for path in &self.outputs {
                let _ = fs::remove_file(path);
            }
        }
    }
}

pub(crate) struct PreparedRun {
    pub(crate) writer: RunWriter,
    destination: PathBuf,
}

impl PreparedRun {
    pub(crate) fn finish(mut self) -> Result<RunMeta> {
        sync_dirty_file(&self.writer.file)?;
        fs::rename(&self.writer.tmp_path, &self.destination)?;
        if let Some(space) = &self.writer.space {
            space.output(&self.destination);
        }
        sync_directory(&self.writer.dir)?;
        self.writer.committed = true;
        Ok(std::mem::take(&mut self.writer.meta))
    }
}

pub(crate) struct RunWriter {
    dir: PathBuf,
    tmp_path: PathBuf,
    file: File,
    key: [u8; 32],
    space: Option<MergeSpace>,
    pub(crate) meta: RunMeta,
    bytes_written: u64,
    data_blocks: u64,
    max_payload_bytes: usize,
    block_buf: Vec<u8>,
    block_first_key: Vec<u8>,
    index_refs: Vec<BlockRef>,
    index_hashes: Vec<u64>,
    index_est_bytes: usize,
    top_refs: Vec<BlockRef>,
    write_failed: bool,
    committed: bool,
}

impl RunWriter {
    pub(crate) fn open(dir: &Path, key: &[u8; 32], space: Option<&MergeSpace>) -> Result<Self> {
        let id = Uuid::new_v4();
        let tmp_path = dir.join(format!(".{id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            tmp_path,
            file,
            key: *key,
            space: space.cloned(),
            meta: RunMeta {
                id,
                ..RunMeta::default()
            },
            bytes_written: 0,
            data_blocks: 0,
            max_payload_bytes: 0,
            block_buf: Vec::with_capacity(BLOCK_BYTES + 1024),
            block_first_key: Vec::new(),
            index_refs: Vec::new(),
            index_hashes: Vec::new(),
            index_est_bytes: 0,
            top_refs: Vec::new(),
            write_failed: false,
            committed: false,
        })
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written + self.block_buf.len() as u64
    }

    fn require_healthy(&self) -> Result<()> {
        (!self.write_failed)
            .then_some(())
            .ok_or_else(|| Error::WriteCapacity("disk run: earlier block write failed".into()))
    }

    pub(crate) fn push(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.require_healthy()?;
        if key.is_empty() || key.len() > MAX_RUN_KEY_BYTES {
            return Err(Error::Config("disk run: key size out of range".into()));
        }
        if value.is_some_and(|v| v.len() > MAX_RUN_VALUE_BYTES) {
            return Err(Error::Config(
                "disk run: value larger than the run limit".into(),
            ));
        }
        if self.meta.records > 0 && self.meta.last_key.as_slice() >= key {
            return Err(corrupt("keys pushed out of order"));
        }
        if !self.block_buf.is_empty()
            && self.block_buf.len() + record_len(&self.meta.last_key, key, value) > BLOCK_BYTES
        {
            self.flush_block()?;
        }
        let prev = if self.block_buf.is_empty() {
            self.block_first_key = key.to_vec();
            &[][..]
        } else {
            self.meta.last_key.as_slice()
        };
        put_record(&mut self.block_buf, prev, key, value);
        self.index_hashes.push(bloom::key_hash(key));
        self.meta.note_record(key, value.map(|v| v.len() as u64))
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let len = bytes.len() as u64;
        if let Some(space) = &self.space {
            space.admit(len, true)?;
        }
        self.file.write_all(bytes)?;
        if let Some(space) = &self.space {
            space.materialize(len)?;
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let res = (|| {
            let frame = encode_frame(&self.key, self.meta.id, counter, payload)?;
            let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
            self.write_bytes(&frame)?;
            let offset = self.bytes_written;
            self.bytes_written = offset + frame.len() as u64;
            self.max_payload_bytes = self.max_payload_bytes.max(payload.len());
            Ok((offset, len))
        })();
        self.write_failed |= res.is_err();
        res
    }

    fn flush_block(&mut self) -> Result<()> {
        let mut payload = std::mem::take(&mut self.block_buf);
        let first_key = std::mem::take(&mut self.block_first_key);
        let (number, key_len) = (self.data_blocks, first_key.len());
        let (offset, len) = self.write_frame(number, &payload)?;
        payload.clear();
        self.block_buf = payload;
        self.index_est_bytes += key_len + 16;
        self.data_blocks = number + 1;
        self.index_refs.push(BlockRef {
            first_key,
            number,
            offset,
            len,
        });
        if self.index_est_bytes >= INDEX_BYTES || self.index_refs.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let mut payload = Vec::with_capacity(self.index_est_bytes + self.index_hashes.len() * 2);
        put_refs(&mut payload, &self.index_refs);
        bloom::put_filter(&mut payload, &self.index_hashes, BLOOM_BITS_PER_KEY);
        let number = self.top_refs.len() as u64;
        let first_key = self.index_refs[0].first_key.clone();
        let (offset, len) = self.write_frame(INDEX_COUNTER_BASE + number, &payload)?;
        self.top_refs.push(BlockRef {
            first_key,
            number,
            offset,
            len,
        });
        self.index_refs.clear();
        self.index_hashes.clear();
        self.index_est_bytes = 0;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RunMeta> {
        self.prepare_finish()?.finish()
    }

    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        self.require_healthy()?;
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }
        if !self.index_refs.is_empty() {
            self.flush_index()?;
        }
        let max_payload_bytes =
            u32::try_from(self.max_payload_bytes).map_err(|_| corrupt("over 4 GiB"))?;
        let data_blocks =
            u32::try_from(self.data_blocks).map_err(|_| corrupt("too many blocks"))?;
        let top = Top {
            records: self.meta.records,
            data_blocks: self.data_blocks,
            max_payload_bytes: u64::from(max_payload_bytes),
            indexes: std::mem::take(&mut self.top_refs),
            last_key: self.meta.last_key.clone(),
        };
        let top_bytes = top.encode();
        if top.indexes.len() > MAX_INDEX_ENTRIES || top_bytes.len() > MAX_TOP_BYTES {
            return Err(Error::WriteCapacity(
                "disk run: top block over read limit".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top_bytes)?;
        let (run_id, records) = (self.meta.id, self.meta.records);
        let footer = Footer {
            run_id,
            top_offset,
            top_len,
            records,
            max_payload_bytes,
            data_blocks,
        };
        self.write_bytes(&footer.encode())?;
        if records == 0 {
            return Err(Error::Config(
                "disk run home: an empty run has no place".into(),
            ));
        }
        self.meta.file_bytes = self.bytes_written + FOOTER_BYTES as u64;
        let destination = self.dir.join(format!("{run_id}{RUN_SUFFIX}"));
        Ok(PreparedRun {
            writer: self,
            destination,
        })
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}
