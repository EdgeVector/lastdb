//! Bounded run ranges, merge space reservations, and ordered k-way merge of runs.

use super::codec::corrupt;
use super::reader::{IndexBlock, RunReader};
use super::writer::RunWriter;
use super::Found;
use crate::space::{reserve_space, SpacePolicy, SpaceReservation};
use crate::{Error, Result};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

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

fn overflow() -> Error {
    Error::WriteCapacity("disk run: replacement reservation overflow".into())
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

    pub(crate) fn admit(&self, bytes: u64, chunked: bool) -> Result<()> {
        let req = if chunked {
            bytes.checked_add(PUBLICATION_BYTES).ok_or_else(overflow)?
        } else {
            bytes
        };
        let mut p = self.lock();
        if req <= p.ticket.bytes {
            return Ok(());
        }
        let extra = if chunked {
            let chunk = p
                .ticket
                .bytes
                .checked_add(OUTPUT_CHUNK_BYTES)
                .ok_or_else(overflow)?
                .max(req);
            let nospc = rustix::io::Errno::NOSPC.raw_os_error();
            match p.ticket.reserve_extension(&p.policy, chunk) {
                Err(Error::Io(io)) if io.raw_os_error() == Some(nospc) => {
                    p.ticket.reserve_extension(&p.policy, req)
                }
                other => other,
            }?
        } else {
            p.ticket.reserve_extension(&p.policy, req)?
        };
        p.ticket.absorb(extra);
        Ok(())
    }

    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.lock().ticket.materialize(bytes)
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
            for p in &self.outputs {
                let _ = fs::remove_file(p);
            }
        }
    }
}

pub(crate) struct RunRange {
    reader: Option<Arc<RunReader>>,
    tail: std::vec::IntoIter<PathBuf>,
    key: [u8; 32],
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    cur: (usize, Option<IndexBlock>, usize, bool),
    records: std::vec::IntoIter<(Vec<u8>, Found)>,
    done: bool,
}

impl RunRange {
    pub(super) fn new(reader: Arc<RunReader>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let mut s = Self::sequence(
            vec![],
            &[0; 32],
            lower.map(<[u8]>::to_vec),
            upper.map(<[u8]>::to_vec),
        );
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
            bounds: (lower, upper),
            cur: (0, None, 0, false),
            records: Vec::new().into_iter(),
            done: false,
        }
    }

    fn set_reader(&mut self, r: Arc<RunReader>) {
        let next_idx = match &self.bounds.0 {
            Bound::Included(k) | Bound::Excluded(k) => r.index_for(k).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        (self.cur, self.reader) = ((next_idx, None, 0, false), Some(r));
    }

    fn above_upper(&self, key: &[u8]) -> bool {
        match &self.bounds.1 {
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
                .cur
                .1
                .as_ref()
                .is_none_or(|idx| self.cur.2 >= idx.0.len())
            {
                if self.cur.0 >= reader.top.indexes.len() {
                    self.reader = None;
                    continue;
                }
                let idx = reader.read_index(self.cur.0, None, None)?;
                self.cur.0 += 1;
                let first = !std::mem::replace(&mut self.cur.3, true);
                self.cur.2 = if let (Bound::Included(k) | Bound::Excluded(k), true) =
                    (&self.bounds.0, first)
                {
                    idx.block_pos(k).unwrap_or(0)
                } else {
                    0
                };
                self.cur.1 = Some(idx);
                continue;
            }
            let block = &self.cur.1.as_ref().unwrap().0[self.cur.2];
            if self.above_upper(&block.first_key) {
                return Ok(false);
            }
            self.records = reader.read_data(block, None)?.into_iter();
            self.cur.2 += 1;
            return Ok(true);
        }
    }
}

impl Iterator for RunRange {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            for (key, found) in self.records.by_ref() {
                if match &self.bounds.0 {
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

type Head = Option<(Vec<u8>, Found)>;

pub(crate) struct MergeIter {
    sources: Vec<RunRange>,
    heads: Option<Vec<Head>>,
    done: bool,
}

impl MergeIter {
    pub(crate) fn new(sources: Vec<RunRange>) -> Self {
        Self {
            sources,
            heads: None,
            done: false,
        }
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let step = (|| -> Result<Option<(Vec<u8>, Found)>> {
            let heads = if let Some(h) = &mut self.heads {
                h
            } else {
                self.heads.insert(
                    self.sources
                        .iter_mut()
                        .map(|s| s.next().transpose())
                        .collect::<Result<_>>()?,
                )
            };
            let Some((_, winner)) = heads
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|(k, _)| (k, i)))
                .min_by(|(a, i), (b, j)| a.cmp(b).then(i.cmp(j)))
            else {
                return Ok(None);
            };
            let record = heads[winner].take().unwrap();
            for (slot, src) in heads[winner + 1..]
                .iter_mut()
                .zip(&mut self.sources[winner + 1..])
            {
                if slot.as_ref().is_some_and(|(k, _)| *k == record.0) {
                    *slot = src.next().transpose()?;
                }
            }
            heads[winner] = self.sources[winner].next().transpose()?;
            Ok(Some(record))
        })();
        self.done |= !matches!(step, Ok(Some(_)));
        step.transpose()
    }
}

pub(crate) fn merge_runs(sources: Vec<RunRange>, writer: &mut RunWriter) -> Result<u64> {
    let mut written = 0;
    for record in MergeIter::new(sources) {
        let (key, found) = record?;
        writer.push(
            &key,
            if let Found::Value(v) = &found {
                Some(v.as_slice())
            } else {
                None
            },
        )?;
        written += 1;
    }
    Ok(written)
}

pub(crate) fn count_merged_keys(
    key: &[u8; 32],
    retained: u64,
    runs: &[PathBuf],
    base: Vec<PathBuf>,
    deadline: Instant,
) -> (bool, u64) {
    let mut merged_keys = 0u64;
    let complete = (|| -> Result<()> {
        let count = runs.len() + usize::from(!base.is_empty());
        if count == 0 {
            return Ok(());
        }
        if count > 512 || retained > 56 << 20 {
            return Err(Error::ReadBudgetExceeded);
        }
        let allow = ((56 << 20) - retained) / count as u64;
        let mut sources: Vec<_> = runs
            .iter()
            .map(|p| Ok(Arc::new(RunReader::open_bounded(p, key, allow, 0)?).iter()))
            .collect::<Result<_>>()?;
        if !base.is_empty() {
            sources.push(RunRange::sequence(
                base,
                key,
                Bound::Unbounded,
                Bound::Unbounded,
            ));
        }
        for item in MergeIter::new(sources) {
            if merged_keys >= 50_000_000 || Instant::now() >= deadline {
                return Err(Error::ReadBudgetExceeded);
            }
            let (rk, _) = item?;
            if rk
                .get(..2)
                .is_none_or(|p| rk.len() < 2 + u16::from_be_bytes([p[0], p[1]]) as usize)
            {
                return Err(corrupt("diagnostic prefix"));
            }
            merged_keys += 1;
        }
        Ok(())
    })()
    .is_ok();
    (complete, merged_keys)
}
