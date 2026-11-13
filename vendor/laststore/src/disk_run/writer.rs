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
pub(crate) struct MergeSpace(Arc<Mutex<Pending>>);

struct Pending(SpacePolicy, SpaceReservation, Vec<PathBuf>, bool);

impl MergeSpace {
    pub(crate) fn new(root: &Path, policy: &SpacePolicy) -> Result<Self> {
        let mut pol = policy.clone();
        pol.replacement_reserve_bytes = 0;
        let p = Pending(
            pol.clone(),
            reserve_space(root, &pol, PUBLICATION_BYTES)?,
            Vec::new(),
            false,
        );
        Ok(Self(Arc::new(Mutex::new(p))))
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn admit(&self, bytes: u64, chunked: bool) -> Result<()> {
        let ovf = || Error::WriteCapacity("disk run: replacement reservation overflow".into());
        let req = if chunked {
            bytes.checked_add(PUBLICATION_BYTES).ok_or_else(ovf)?
        } else {
            bytes
        };
        let mut p = self.lock();
        if req <= p.1 .2 {
            return Ok(());
        }
        let extra = if chunked {
            let chunk =
                p.1 .2
                    .checked_add(OUTPUT_CHUNK_BYTES)
                    .ok_or_else(ovf)?
                    .max(req);
            let nospc = rustix::io::Errno::NOSPC.raw_os_error();
            match p.1.reserve_extension(&p.0, chunk) {
                Err(Error::Io(io)) if io.raw_os_error() == Some(nospc) => {
                    p.1.reserve_extension(&p.0, req)?
                }
                res => res?,
            }
        } else {
            p.1.reserve_extension(&p.0, req)?
        };
        p.1.absorb(extra);
        Ok(())
    }

    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.lock().1.materialize(bytes)
    }
    pub(crate) fn output(&self, path: &Path) {
        self.lock().2.push(path.to_path_buf());
    }
    pub(crate) fn keep_outputs(&self) {
        self.lock().3 = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.3 {
            self.2.iter().for_each(|p| drop(fs::remove_file(p)));
        }
    }
}

pub(crate) struct PreparedRun(pub(crate) RunWriter, PathBuf);

type Paths = (PathBuf, PathBuf);
type IndexBuf = (Vec<BlockRef>, Vec<u64>, usize);

pub(crate) struct RunWriter(
    Paths,
    File,
    [u8; 32],
    Option<MergeSpace>,
    pub(crate) RunMeta,
    (u64, u64, usize),
    (Vec<u8>, Vec<u8>),
    IndexBuf,
    Vec<BlockRef>,
    (bool, bool),
);

impl PreparedRun {
    pub(crate) fn finish(mut self) -> Result<RunMeta> {
        sync_dirty_file(&self.0 .1)?;
        fs::rename(&self.0 .0 .1, &self.1)?;
        if let Some(sp) = &self.0 .3 {
            sp.output(&self.1);
        }
        sync_directory(&self.0 .0 .0)?;
        self.0 .9 .1 = true;
        Ok(std::mem::take(&mut self.0 .4))
    }
}

impl RunWriter {
    pub(crate) fn open(dir: &Path, key: &[u8; 32], space: Option<&MergeSpace>) -> Result<Self> {
        let id = Uuid::new_v4();
        let tmp = dir.join(format!(".{id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        let (meta, blk) = (
            RunMeta(id, [0; 5], Vec::new(), Vec::new()),
            (Vec::with_capacity(BLOCK_BYTES + 1024), Vec::new()),
        );
        Ok(Self(
            (dir.to_path_buf(), tmp),
            file,
            *key,
            space.cloned(),
            meta,
            (0, 0, 0),
            blk,
            Default::default(),
            Vec::new(),
            (false, false),
        ))
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.5 .0 + self.6 .0.len() as u64
    }

    fn require_healthy(&self) -> Result<()> {
        (!self.9 .0)
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
        if self.4 .1[0] > 0 && self.4 .3.as_slice() >= key {
            return Err(corrupt("keys pushed out of order"));
        }
        if !self.6 .0.is_empty()
            && self.6 .0.len() + record_len(&self.4 .3, key, value) > BLOCK_BYTES
        {
            self.flush_block()?;
        }
        let prev = if self.6 .0.is_empty() {
            self.6 .1 = key.to_vec();
            &[][..]
        } else {
            self.4 .3.as_slice()
        };
        put_record(&mut self.6 .0, prev, key, value);
        self.7 .1.push(bloom::key_hash(key));
        self.4.note_record(key, value.map(|v| v.len() as u64))
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let len = bytes.len() as u64;
        if let Some(sp) = &self.3 {
            sp.admit(len, true)?;
        }
        self.1.write_all(bytes)?;
        if let Some(sp) = &self.3 {
            sp.materialize(len)?;
        }
        Ok(())
    }

    fn write_frame(&mut self, counter: u64, payload: &[u8]) -> Result<(u64, u32)> {
        let res = (|| {
            let frame = encode_frame(&self.2, self.4 .0, counter, payload)?;
            let len = u32::try_from(frame.len()).map_err(|_| corrupt("block over 4 GiB"))?;
            self.write_bytes(&frame)?;
            let off = self.5 .0;
            self.5 = (
                off + frame.len() as u64,
                self.5 .1,
                self.5 .2.max(payload.len()),
            );
            Ok((off, len))
        })();
        self.9 .0 |= res.is_err();
        res
    }

    fn flush_block(&mut self) -> Result<()> {
        let mut payload = std::mem::take(&mut self.6 .0);
        let first = std::mem::take(&mut self.6 .1);
        let (num, klen) = (self.5 .1, first.len());
        let (off, len) = self.write_frame(num, &payload)?;
        payload.clear();
        self.6 .0 = payload;
        self.7 .2 += klen + 16;
        self.5 .1 = num + 1;
        self.7 .0.push(BlockRef(first, num, off, len));
        if self.7 .2 >= INDEX_BYTES || self.7 .0.len() >= MAX_INDEX_ENTRIES {
            self.flush_index()?;
        }
        Ok(())
    }

    fn flush_index(&mut self) -> Result<()> {
        let mut payload = Vec::with_capacity(self.7 .2 + self.7 .1.len() * 2);
        put_refs(&mut payload, &self.7 .0);
        bloom::put_filter(&mut payload, &self.7 .1, BLOOM_BITS_PER_KEY);
        let (num, first) = (self.8.len() as u64, self.7 .0[0].0.clone());
        let (off, len) = self.write_frame(INDEX_COUNTER_BASE + num, &payload)?;
        self.8.push(BlockRef(first, num, off, len));
        self.7 = Default::default();
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RunMeta> {
        self.prepare_finish()?.finish()
    }

    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        self.require_healthy()?;
        if !self.6 .0.is_empty() {
            self.flush_block()?;
        }
        if !self.7 .0.is_empty() {
            self.flush_index()?;
        }
        let max_p = u32::try_from(self.5 .2).map_err(|_| corrupt("over 4 GiB"))?;
        let blocks = u32::try_from(self.5 .1).map_err(|_| corrupt("too many blocks"))?;
        let top = Top(
            (self.4 .1[0], self.5 .1, u64::from(max_p)),
            std::mem::take(&mut self.8),
            self.4 .3.clone(),
        );
        let top_bytes = top.encode();
        if top.1.len() > MAX_INDEX_ENTRIES || top_bytes.len() > MAX_TOP_BYTES {
            return Err(Error::WriteCapacity(
                "disk run: top block over read limit".into(),
            ));
        }
        let (top_off, top_len) = self.write_frame(TOP_COUNTER, &top_bytes)?;
        let (id, records) = (self.4 .0, self.4 .1[0]);
        self.write_bytes(&Footer(id, top_off, top_len, records, max_p, blocks).encode())?;
        if records == 0 {
            return Err(Error::Config(
                "disk run home: an empty run has no place".into(),
            ));
        }
        self.4 .1[4] = self.5 .0 + FOOTER_BYTES as u64;
        let dest = self.0 .0.join(format!("{id}{RUN_SUFFIX}"));
        Ok(PreparedRun(self, dest))
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        if !self.9 .1 {
            let _ = fs::remove_file(&self.0 .1);
        }
    }
}
