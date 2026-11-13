//! Bounded run ranges, merge space reservations, and ordered k-way merge of runs.

use super::reader::{IndexBlock, RunReader};
use super::writer::RunWriter;
use super::Found;
use crate::space::{reserve_space, SpacePolicy, SpaceReservation};
use crate::{Error, Result};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

const PUBLICATION_BYTES: u64 = 4096;
const OUTPUT_CHUNK_BYTES: u64 = 1 << 20;

#[derive(Clone)]
pub(crate) struct MergeSpace(Arc<Mutex<Pending>>);

struct Pending {
    policy: SpacePolicy,
    ticket: SpaceReservation,
    outputs: Vec<PathBuf>,
    keep: bool,
}

impl MergeSpace {
    pub(crate) fn new(root: &Path, policy: &SpacePolicy) -> Result<Self> {
        let mut policy = policy.clone();
        policy.replacement_reserve_bytes = 0;
        let ticket = reserve_space(root, &policy, PUBLICATION_BYTES)?;
        Ok(Self(Arc::new(Mutex::new(Pending {
            policy,
            ticket,
            outputs: Vec::new(),
            keep: false,
        }))))
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn admit_run(&self, bytes: u64) -> Result<()> {
        let mut p = self.lock();
        let ovf = || Error::WriteCapacity("disk run: replacement reservation overflow".into());
        let req = bytes.checked_add(PUBLICATION_BYTES).ok_or_else(ovf)?;
        if req <= p.ticket.bytes() {
            return Ok(());
        }
        let chunk = p
            .ticket
            .bytes()
            .checked_add(OUTPUT_CHUNK_BYTES)
            .ok_or_else(ovf)?
            .max(req);
        let nospc = rustix::io::Errno::NOSPC.raw_os_error();
        let extra = match p.ticket.reserve_extension(&p.policy, chunk) {
            Err(Error::Io(io)) if io.raw_os_error() == Some(nospc) => {
                p.ticket.reserve_extension(&p.policy, req)
            }
            other => other,
        }?;
        p.ticket.absorb(extra);
        Ok(())
    }

    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.lock().ticket.materialize(bytes)
    }

    pub(crate) fn admit_current(&self, bytes: u64) -> Result<()> {
        let mut p = self.lock();
        if bytes > p.ticket.bytes() {
            let extra = p.ticket.reserve_extension(&p.policy, bytes)?;
            p.ticket.absorb(extra);
        }
        Ok(())
    }

    pub(crate) fn output(&self, path: &Path) {
        self.lock().outputs.push(path.to_path_buf());
    }

    pub(crate) fn keep_outputs(&self) {
        self.lock().keep = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.keep {
            self.outputs.iter().for_each(|p| {
                let _ = fs::remove_file(p);
            });
        }
    }
}

pub(crate) struct RunRange {
    reader: Option<Arc<RunReader>>,
    tail: std::vec::IntoIter<PathBuf>,
    key: [u8; 32],
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    next_index: usize,
    index: Option<IndexBlock>,
    next_block: usize,
    positioned: bool,
    current: std::vec::IntoIter<(Vec<u8>, Found)>,
    done: bool,
}

impl RunRange {
    pub(super) fn new(reader: Arc<RunReader>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let (lo, up) = (lower.map(<[u8]>::to_vec), upper.map(<[u8]>::to_vec));
        let mut s = Self::sequence(vec![], &[0; 32], lo, up);
        s.set_reader(reader);
        s
    }

    pub(crate) fn sequence(
        paths: Vec<PathBuf>,
        key: &[u8; 32],
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            reader: None,
            tail: paths.into_iter(),
            key: *key,
            lower,
            upper,
            next_index: 0,
            index: None,
            next_block: 0,
            positioned: false,
            current: Vec::new().into_iter(),
            done: false,
        }
    }

    fn set_reader(&mut self, r: Arc<RunReader>) {
        self.next_index = match &self.lower {
            Bound::Included(k) | Bound::Excluded(k) => r.index_for(k).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        (self.index, self.next_block, self.positioned, self.reader) = (None, 0, false, Some(r));
    }

    fn above_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(u) => key > u.as_slice(),
            Bound::Excluded(u) => key >= u.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        loop {
            let Some(reader) = &self.reader else {
                let Some(path) = self.tail.next() else {
                    return Ok(false);
                };
                self.set_reader(Arc::new(RunReader::open(&path, &self.key)?));
                continue;
            };
            if self
                .index
                .as_ref()
                .is_none_or(|idx| self.next_block >= idx.blocks.len())
            {
                if self.next_index >= reader.top.indexes.len() {
                    self.reader = None;
                    continue;
                }
                let idx = reader.read_index(self.next_index, None)?;
                self.next_index += 1;
                self.next_block =
                    match (&self.lower, !std::mem::replace(&mut self.positioned, true)) {
                        (Bound::Included(k) | Bound::Excluded(k), true) => {
                            idx.block_pos(k).unwrap_or(0)
                        }
                        _ => 0,
                    };
                self.index = Some(idx);
                continue;
            }
            let block = &self.index.as_ref().unwrap().blocks[self.next_block];
            if self.above_upper(&block.first_key) {
                return Ok(false);
            }
            self.current = reader.read_data(block, None)?.into_iter();
            self.next_block += 1;
            return Ok(true);
        }
    }
}

impl Iterator for RunRange {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            for (key, found) in self.current.by_ref() {
                if match &self.lower {
                    Bound::Included(l) => key < *l,
                    Bound::Excluded(l) => key <= *l,
                    Bound::Unbounded => false,
                } {
                    continue;
                }
                if self.above_upper(&key) {
                    self.done = true;
                    return None;
                }
                return Some(Ok((key, found)));
            }
            match self.load_next_block() {
                Ok(true) => {}
                res => {
                    self.done = true;
                    return res.err().map(Err);
                }
            }
        }
        None
    }
}

pub(crate) struct MergeIter {
    sources: Vec<RunRange>,
    heads: Vec<Option<(Vec<u8>, Found)>>,
    started: bool,
    done: bool,
}

impl MergeIter {
    pub(crate) fn new(sources: Vec<RunRange>) -> Self {
        Self {
            heads: vec![None; sources.len()],
            sources,
            started: false,
            done: false,
        }
    }

    fn advance(&mut self, i: usize) -> Result<()> {
        self.heads[i] = self.sources[i].next().transpose()?;
        Ok(())
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let step = (|| -> Result<Option<(Vec<u8>, Found)>> {
            if !std::mem::replace(&mut self.started, true) {
                for s in 0..self.sources.len() {
                    self.advance(s)?;
                }
            }
            let Some((_, winner)) = self
                .heads
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|(k, _)| (k, i)))
                .min_by(|(a, i), (b, j)| a.cmp(b).then(i.cmp(j)))
            else {
                return Ok(None);
            };
            let record = self.heads[winner].take().unwrap();
            for s in winner + 1..self.heads.len() {
                if self.heads[s].as_ref().is_some_and(|(k, _)| *k == record.0) {
                    self.advance(s)?;
                }
            }
            self.advance(winner)?;
            Ok(Some(record))
        })();
        self.done |= !matches!(step, Ok(Some(_)));
        step.transpose()
    }
}

pub(crate) fn merge_runs(
    sources: Vec<RunRange>,
    writer: &mut RunWriter,
    keep_tombstones: bool,
) -> Result<u64> {
    let mut written = 0;
    for record in MergeIter::new(sources) {
        let (key, found) = record?;
        match found {
            Found::Value(v) => writer.push(&key, Some(&v))?,
            Found::Tombstone if keep_tombstones => writer.push(&key, None)?,
            Found::Tombstone => continue,
        }
        written += 1;
    }
    Ok(written)
}
