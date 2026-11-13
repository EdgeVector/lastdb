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

    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn key_hash(key: &[u8]) -> u64 {
        mix(key.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        }))
    }

    fn probes(hash: u64, bits: u64) -> impl Iterator<Item = u64> {
        let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    pub(super) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bpk: u32) {
        let bytes = if bpk == 0 || hashes.is_empty() {
            0
        } else {
            (hashes.len() * bpk as usize)
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
                for b in probes(hash, bits) {
                    filter[(b / 8) as usize] |= 1 << (b % 8);
                }
            }
        }
    }

    pub(crate) fn read_filter<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
        let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
        if get_varint(bytes, at)? != PROBES {
            return Err(corrupt("bloom probes"));
        }
        get_slice(bytes, at, len)
    }

    pub(crate) fn may_hold(filter: &[u8], hash: u64) -> bool {
        filter.is_empty()
            || probes(hash, (filter.len() * 8) as u64)
                .all(|b| filter[(b / 8) as usize] & (1 << (b % 8)) != 0)
    }
}

pub(crate) struct PreparedRun(pub(crate) RunWriter, PathBuf);

impl PreparedRun {
    pub(crate) fn finish(mut self) -> Result<RunMeta> {
        sync_dirty_file(&self.0.file)?;
        fs::rename(&self.0.paths.1, &self.1)?;
        if let Some(space) = &self.0.space {
            space.output(&self.1);
        }
        sync_directory(&self.0.paths.0)?;
        self.0.state.1 = true;
        Ok(std::mem::take(&mut self.0.meta))
    }
}

pub(crate) struct RunWriter {
    paths: (PathBuf, PathBuf),
    file: File,
    data_key: [u8; 32],
    space: Option<MergeSpace>,
    pub(crate) meta: RunMeta,
    counts: (u64, u64, usize),
    block: (Vec<u8>, Vec<u8>),
    index: (Vec<BlockRef>, Vec<u64>, usize),
    top: Vec<BlockRef>,
    state: (bool, bool),
}

impl RunWriter {
    pub(crate) fn open(dir: &Path, key: &[u8; 32], space: Option<&MergeSpace>) -> Result<Self> {
        let run_id = Uuid::new_v4();
        let tmp = dir.join(format!(".{run_id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        Ok(Self {
            paths: (dir.to_path_buf(), tmp),
            file,
            data_key: *key,
            space: space.cloned(),
            meta: RunMeta {
                run_id,
                ..RunMeta::default()
            },
            counts: (0, 0, 0),
            block: (Vec::with_capacity(BLOCK_BYTES + 1024), Vec::new()),
            index: (Vec::new(), Vec::new(), 0),
            top: Vec::new(),
            state: (false, false),
        })
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.counts.0 + self.block.0.len() as u64
    }

    fn require_healthy(&self) -> Result<()> {
        (!self.state.0)
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
        if !self.block.0.is_empty()
            && self.block.0.len() + record_len(&self.meta.last_key, key, value) > BLOCK_BYTES
        {
            self.flush_block()?;
        }
        let prev = if self.block.0.is_empty() {
            self.block.1 = key.to_vec();
            &[][..]
        } else {
            self.meta.last_key.as_slice()
        };
        put_record(&mut self.block.0, prev, key, value);
        self.index.1.push(bloom::key_hash(key));
        self.meta.note_record(key, value.map(|v| v.len() as u64))
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(sp) = &self.space {
            sp.admit(bytes.len() as u64, true)?;
        }
        self.file.write_all(bytes)?;
        if let Some(sp) = &self.space {
            sp.materialize(bytes.len() as u64)?;
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let res = (|| {
            let frame = encode_frame(&self.data_key, self.meta.run_id, counter, payload)?;
            let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
            self.write_bytes(&frame)?;
            let offset = self.counts.0;
            self.counts.0 += frame.len() as u64;
            Ok((offset, len))
        })();
        self.state.0 |= res.is_err();
        res
    }

    fn flush_block(&mut self) -> Result<()> {
        let mut payload = std::mem::take(&mut self.block.0);
        let (offset, len) = self.write_frame(self.counts.1, &payload)?;
        self.counts.2 = self.counts.2.max(payload.len());
        payload.clear();
        self.block.0 = payload;
        let (first_key, number) = (std::mem::take(&mut self.block.1), self.counts.1);
        self.index.2 += first_key.len() + 16;
        self.counts.1 += 1;
        self.index.0.push(BlockRef {
            first_key,
            number,
            offset,
            len,
        });
        if self.index.2 >= INDEX_BYTES || self.index.0.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let mut payload = Vec::with_capacity(self.index.2 + self.index.1.len() * 2);
        put_refs(&mut payload, &self.index.0);
        bloom::put_filter(&mut payload, &self.index.1, BLOOM_BITS_PER_KEY);
        let number = self.top.len() as u64;
        let (offset, len) = self.write_frame(INDEX_COUNTER_BASE + number, &payload)?;
        self.counts.2 = self.counts.2.max(payload.len());
        self.top.push(BlockRef {
            first_key: self.index.0[0].first_key.clone(),
            number,
            offset,
            len,
        });
        self.index.0.clear();
        self.index.1.clear();
        self.index.2 = 0;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RunMeta> {
        self.prepare_finish()?.finish()
    }

    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        self.require_healthy()?;
        if !self.block.0.is_empty() {
            self.flush_block()?;
        }
        if !self.index.0.is_empty() {
            self.flush_index()?;
        }
        let max_payload = u32::try_from(self.counts.2).map_err(|_| corrupt("over 4 GiB"))?;
        let data_blocks = u32::try_from(self.counts.1).map_err(|_| corrupt("too many blocks"))?;
        let indexes = std::mem::take(&mut self.top);
        let top_entries = indexes.len();
        let top = Top {
            counts: (self.meta.records, self.counts.1, u64::from(max_payload)),
            indexes,
            last_key: self.meta.last_key.clone(),
        }
        .encode();
        if top.len() > MAX_TOP_BYTES || top_entries > MAX_INDEX_ENTRIES {
            return Err(Error::WriteCapacity(
                "disk run: top block over read limit".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top)?;
        let (run_id, records) = (self.meta.run_id, self.meta.records);
        let footer = Footer {
            run_id,
            top_offset,
            top_len,
            records,
            max_payload,
            data_blocks,
        };
        self.write_bytes(&footer.encode())?;
        if records == 0 {
            return Err(Error::Config(
                "disk run home: an empty run has no place".into(),
            ));
        }
        self.meta.file_bytes = self.counts.0 + FOOTER_BYTES as u64;
        let path = self.paths.0.join(format!("{run_id}{RUN_SUFFIX}"));
        Ok(PreparedRun(self, path))
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.state.1 {
            let _ = fs::remove_file(&self.paths.1);
        }
    }
}
