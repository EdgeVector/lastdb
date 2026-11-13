//! Writes one run file from records in strictly ascending key order.

use super::codec::{corrupt, put_record, record_len};
use super::footer::{put_refs, BlockRef, Footer, Top, FOOTER_BYTES};
use super::merge::MergeSpace;
use super::{
    RunMeta, INDEX_COUNTER_BASE, MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES,
    MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER,
};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::encode_frame;
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const BLOCK_BYTES: usize = 16 * 1024;
const INDEX_BYTES: usize = 16 * 1024;
const BLOOM_BITS_PER_KEY: u32 = 10;

pub(crate) mod bloom {
    use crate::disk_run::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
    use crate::Result;

    const PROBES: u64 = 7;
    const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

    pub(crate) fn key_hash(key: &[u8]) -> u64 {
        mix(key.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        }))
    }

    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn probes(hash: u64, bits: u64) -> impl Iterator<Item = u64> {
        let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    pub(super) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bits_per_key: u32) {
        let bytes = if bits_per_key == 0 || hashes.is_empty() {
            0
        } else {
            (hashes.len() * bits_per_key as usize)
                .div_ceil(8)
                .clamp(8, MAX_FILTER_BYTES)
        };
        put_varint(out, bytes as u64);
        put_varint(out, PROBES);
        let start = out.len();
        out.resize(start + bytes, 0);
        if bytes > 0 {
            let (bits, filter) = ((bytes * 8) as u64, &mut out[start..]);
            for &hash in hashes {
                for bit in probes(hash, bits) {
                    filter[(bit / 8) as usize] |= 1 << (bit % 8);
                }
            }
        }
    }

    pub(crate) fn read_filter<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
        let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
        (get_varint(bytes, at)? == PROBES)
            .then(|| get_slice(bytes, at, len))
            .ok_or_else(|| corrupt("bloom probes"))?
    }

    pub(crate) fn may_hold(filter: &[u8], hash: u64) -> bool {
        filter.is_empty()
            || probes(hash, (filter.len() * 8) as u64)
                .all(|b| filter[(b / 8) as usize] & (1 << (b % 8)) != 0)
    }
}

pub(crate) struct PreparedRun {
    writer: RunWriter,
    path: PathBuf,
    pub(crate) meta: RunMeta,
}

impl PreparedRun {
    pub(crate) fn finish(mut self) -> Result<RunMeta> {
        sync_dirty_file(&self.writer.file)?;
        fs::rename(&self.writer.tmp, &self.path)?;
        if let Some(space) = &self.writer.space {
            space.output(&self.path);
        }
        sync_directory(&self.writer.dir)?;
        self.writer.s.finished = true;
        Ok(self.meta)
    }
}

#[derive(Default)]
struct State {
    offset: u64,
    data_blocks: u64,
    index_bytes: usize,
    records: u64,
    tombstones: u64,
    record_bytes: u64,
    tombstone_bytes: u64,
    max_payload: usize,
    block: Vec<u8>,
    block_first: Vec<u8>,
    index: Vec<BlockRef>,
    index_hashes: Vec<u64>,
    top: Vec<BlockRef>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    failed: bool,
    finished: bool,
}

pub(crate) struct RunWriter {
    dir: PathBuf,
    tmp: PathBuf,
    file: File,
    data_key: [u8; 32],
    run_id: Uuid,
    space: Option<MergeSpace>,
    s: State,
}

impl RunWriter {
    pub(crate) fn create(dir: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_new(dir, data_key, None)
    }

    pub(crate) fn create_reserved(dir: &Path, key: &[u8; 32], space: &MergeSpace) -> Result<Self> {
        Self::open_new(dir, key, Some(space.clone()))
    }

    fn open_new(dir: &Path, data_key: &[u8; 32], space: Option<MergeSpace>) -> Result<Self> {
        let run_id = Uuid::new_v4();
        let tmp = dir.join(format!(".{run_id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        let mut s = State::default();
        s.block.reserve(BLOCK_BYTES + 1024);
        Ok(Self {
            dir: dir.to_path_buf(),
            tmp,
            file,
            data_key: *data_key,
            run_id,
            space,
            s,
        })
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.s.offset + self.s.block.len() as u64
    }

    pub(crate) fn push(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if self.s.failed {
            return Err(Error::WriteCapacity(
                "disk run: an earlier block write failed; start a new run".into(),
            ));
        }
        if key.is_empty() || key.len() > MAX_RUN_KEY_BYTES {
            return Err(Error::Config("disk run: key size out of range".into()));
        }
        if value.is_some_and(|v| v.len() > MAX_RUN_VALUE_BYTES) {
            return Err(Error::Config(
                "disk run: value larger than the run limit".into(),
            ));
        }
        if self.s.last_key.as_deref().is_some_and(|last| last >= key) {
            return Err(corrupt("keys pushed out of order"));
        }
        let last = self.s.last_key.as_deref().unwrap_or(&[]);
        if !self.s.block.is_empty()
            && self.s.block.len() + record_len(last, key, value) > BLOCK_BYTES
        {
            self.flush_block()?;
        }
        let s = &mut self.s;
        let prev = if s.block.is_empty() {
            s.block_first = key.to_vec();
            &[][..]
        } else {
            s.last_key.as_deref().unwrap_or(&[])
        };
        put_record(&mut s.block, prev, key, value);
        s.index_hashes.push(bloom::key_hash(key));
        s.records += 1;
        s.record_bytes += (key.len() + value.map_or(0, <[u8]>::len)) as u64;
        if value.is_none() {
            s.tombstones += 1;
            s.tombstone_bytes += key.len() as u64;
        }
        s.first_key.get_or_insert_with(|| key.to_vec());
        s.last_key = Some(key.to_vec());
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(sp) = &self.space {
            sp.admit_run(bytes.len() as u64)?;
        }
        self.file.write_all(bytes)?;
        if let Some(sp) = &self.space {
            sp.materialize(bytes.len() as u64)?;
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let written = (|| {
            let frame = encode_frame(&self.data_key, self.run_id, counter, payload)?;
            let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
            self.write_bytes(&frame)?;
            let offset = self.s.offset;
            self.s.offset += frame.len() as u64;
            Ok((offset, len))
        })();
        self.s.failed |= written.is_err();
        written
    }

    fn flush_block(&mut self) -> Result<()> {
        let mut payload = std::mem::take(&mut self.s.block);
        let (offset, len) = self.write_frame(self.s.data_blocks, &payload)?;
        self.s.max_payload = self.s.max_payload.max(payload.len());
        payload.clear();
        self.s.block = payload;
        let first_key = std::mem::take(&mut self.s.block_first);
        self.s.index_bytes += first_key.len() + 16;
        let number = self.s.data_blocks;
        self.s.data_blocks += 1;
        self.s.index.push(BlockRef {
            first_key,
            number,
            offset,
            len,
        });
        if self.s.index_bytes >= INDEX_BYTES || self.s.index.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let mut payload = Vec::with_capacity(self.s.index_bytes + self.s.index_hashes.len() * 2);
        put_refs(&mut payload, &self.s.index);
        bloom::put_filter(&mut payload, &self.s.index_hashes, BLOOM_BITS_PER_KEY);
        let number = self.s.top.len() as u64;
        let (offset, len) = self.write_frame(INDEX_COUNTER_BASE + number, &payload)?;
        self.s.max_payload = self.s.max_payload.max(payload.len());
        self.s.top.push(BlockRef {
            first_key: self.s.index[0].first_key.clone(),
            number,
            offset,
            len,
        });
        self.s.index.clear();
        self.s.index_hashes.clear();
        self.s.index_bytes = 0;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RunMeta> {
        self.prepare_finish()?.finish()
    }

    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        if self.s.failed {
            return Err(Error::WriteCapacity(
                "disk run: an earlier block write failed; start a new run".into(),
            ));
        }
        if !self.s.block.is_empty() {
            self.flush_block()?;
        }
        if !self.s.index.is_empty() {
            self.flush_index()?;
        }
        let max_payload =
            u32::try_from(self.s.max_payload).map_err(|_| corrupt("block over 4 GiB"))?;
        let data_blocks =
            u32::try_from(self.s.data_blocks).map_err(|_| corrupt("too many data blocks"))?;
        let indexes = std::mem::take(&mut self.s.top);
        let top_entries = indexes.len();
        let top = Top {
            records: self.s.records,
            data_blocks: self.s.data_blocks,
            max_payload: u64::from(max_payload),
            indexes,
            last_key: self.s.last_key.clone().unwrap_or_default(),
        }
        .encode();
        if top.len() > MAX_TOP_BYTES || top_entries > MAX_INDEX_ENTRIES {
            return Err(Error::WriteCapacity(
                "disk run: top block over its read limit; write smaller runs".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top)?;
        let (run_id, records) = (self.run_id, self.s.records);
        self.write_bytes(
            &Footer {
                run_id,
                top_offset,
                top_len,
                records,
                max_payload,
                data_blocks,
            }
            .encode(),
        )?;
        let (Some(first_key), Some(last_key)) = (self.s.first_key.take(), self.s.last_key.take())
        else {
            return Err(Error::Config(
                "disk run home: an empty run has no place".into(),
            ));
        };
        let path = self.dir.join(format!("{run_id}{RUN_SUFFIX}"));
        let meta = RunMeta {
            run_id,
            records,
            tombstones: self.s.tombstones,
            record_bytes: self.s.record_bytes,
            tombstone_bytes: self.s.tombstone_bytes,
            file_bytes: self.s.offset + FOOTER_BYTES as u64,
            first_key,
            last_key,
        };
        Ok(PreparedRun {
            writer: self,
            path,
            meta,
        })
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.s.finished {
            let _ = fs::remove_file(&self.tmp);
        }
    }
}
