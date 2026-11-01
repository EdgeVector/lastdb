//! The in-memory state of one reader of a time-bucketed queue.
//!
//! The reader keeps the buckets it knows and one range walk (`RangeWalk`) for
//! each of them. The walk is the cursor: it remembers the least key that the
//! reader has not examined, and it moves over deleted keys without a restart.
//! Every read is bounded by key space (`AtomStore::walk_step`), so a bucket
//! that holds many deleted keys cannot stop the reader. A restart loses the
//! state, and the reader then walks each live bucket once from its start. It
//! never reads a bucket that is not in the directory.
//!
//! The reader decides nothing about what an entry means. A caller reads a page
//! of due entries, releases them, and then calls [`QueueReader::finish`], so the
//! cursor moves only past keys that the caller handled.

use super::keys::TimeQueue;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// Directory rows that one read asks for.
const DIRECTORY_PAGE: usize = 256;
/// Bounded reads that one [`QueueReader::read_due`] makes, at most. A read that
/// finds no live entry passes a run of released keys, up to the key budget of
/// one read call; after this many such reads the call returns an empty page that
/// does not wait, and the cursor keeps the place for the next call. So one call
/// costs a bounded time however many released keys a bucket holds.
const READS_PER_PAGE: usize = 16;

#[derive(Debug)]
struct Bucket {
    /// The directory row as the last refresh read it. A retirement deletes the
    /// row only while it still holds these bytes.
    token: Vec<u8>,
    /// The cursor: every key below the next one it reads was handled.
    walk: RangeWalk,
    /// The expiry of the first entry that was not due when the walk met it. The
    /// walk waits at that key until the clock reaches it.
    waiting_until: Option<u64>,
}

/// One page of due entries of one bucket, read and not yet handled.
#[derive(Debug)]
pub(crate) struct DuePage {
    pub(crate) bucket: u64,
    /// Keys and values of the entries whose expiry is at or before the clock,
    /// oldest first. A key whose expiry cannot be read counts as due: the
    /// caller reports it and the cursor moves past it, so one bad key cannot
    /// hold back the rest.
    pub(crate) rows: Vec<(String, Vec<u8>)>,
    /// The walk after the read.
    walk: RangeWalk,
    /// The first key that is not due, and its expiry.
    stop: Option<(String, u64)>,
}

impl DuePage {
    /// The page ended at a key that is not due. The bucket waits for its time.
    pub(crate) fn waits(&self) -> bool {
        self.stop.is_some()
    }
}

/// The reader state of one queue.
#[derive(Debug)]
pub(crate) struct QueueReader {
    queue: TimeQueue,
    page: usize,
    /// No bucket below this start is alive. A refresh reads the directory from
    /// here, so the rows deleted below it are not read again.
    dir_floor: u64,
    buckets: BTreeMap<u64, Bucket>,
}

impl QueueReader {
    /// A reader that asks for pages of `page` keys.
    pub(crate) fn new(queue: TimeQueue, page: usize) -> Self {
        Self {
            queue,
            page: page.max(1),
            dir_floor: 0,
            buckets: BTreeMap::new(),
        }
    }

    fn fresh_walk(&self, bucket: u64) -> RangeWalk {
        let (start, end) = self.queue.bucket_range(bucket);
        RangeWalk::new(start.as_bytes(), end.as_bytes(), self.page)
    }

    /// Start every bucket walk again from the start of its bucket, and read the
    /// directory again from its start. Call it now and then, for a clock that
    /// stepped back: an entry may then sort below a cursor. It reads live
    /// buckets only, never one that the directory has dropped.
    pub(crate) fn restart(&mut self) {
        self.dir_floor = 0;
        let starts: Vec<u64> = self.buckets.keys().copied().collect();
        for bucket in starts {
            let walk = self.fresh_walk(bucket);
            if let Some(state) = self.buckets.get_mut(&bucket) {
                state.walk = walk;
                state.waiting_until = None;
            }
        }
    }

    /// Read the directory rows of the buckets that started at or before `now`.
    ///
    /// A bucket that the directory no longer lists leaves the state. A bucket
    /// whose token changed starts its walk again: an entry came after the walk
    /// began, and a walk that had finished would never see it.
    pub(crate) async fn refresh(&mut self, store: &AtomStore, now: u64) -> Result<(), SchemaError> {
        let (start, end) = self.queue.directory_range(self.dir_floor, now);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), DIRECTORY_PAGE);
        let mut listed: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        while !walk.done() {
            let step = store.walk_step(&mut walk).await?;
            for (key, value) in step.rows {
                let bucket = String::from_utf8(key)
                    .ok()
                    .and_then(|key| self.queue.directory_bucket(&key));
                if let Some(bucket) = bucket {
                    listed.insert(bucket, value);
                }
            }
        }
        let floor = self.dir_floor;
        self.buckets
            .retain(|bucket, _| *bucket < floor || *bucket > now || listed.contains_key(bucket));
        for (bucket, token) in listed {
            let fresh = self.fresh_walk(bucket);
            match self.buckets.get_mut(&bucket) {
                Some(state) if state.token == token => {}
                Some(state) => {
                    state.token = token;
                    state.walk = fresh;
                    state.waiting_until = None;
                }
                None => {
                    self.buckets.insert(
                        bucket,
                        Bucket {
                            token,
                            walk: fresh,
                            waiting_until: None,
                        },
                    );
                }
            }
        }
        // A bucket in the current slice can still get a row, so the floor does
        // not pass its start.
        let current = self.queue.bucket_of(now);
        self.dir_floor = self
            .buckets
            .keys()
            .next()
            .copied()
            .map_or(current, |lowest| lowest.min(current));
        Ok(())
    }

    /// Buckets that may hold due entries, oldest first.
    pub(crate) fn candidates(&self, now: u64) -> Vec<u64> {
        self.buckets
            .iter()
            .filter(|(_, state)| state.waiting_until.is_none_or(|until| until <= now))
            .map(|(bucket, _)| *bucket)
            .collect()
    }

    /// True when the walk of `bucket` has read every key of it.
    pub(crate) fn exhausted(&self, bucket: u64) -> bool {
        self.buckets
            .get(&bucket)
            .is_some_and(|state| state.walk.done())
    }

    /// The directory key and the token to retire `bucket` with.
    pub(crate) fn retire_token(&self, bucket: u64) -> Option<(String, Vec<u8>)> {
        let state = self.buckets.get(&bucket)?;
        Some((self.queue.directory_key(bucket), state.token.clone()))
    }

    /// Leave `bucket`. A later refresh adds it again if the directory still
    /// lists it.
    pub(crate) fn forget(&mut self, bucket: u64) {
        self.buckets.remove(&bucket);
    }

    /// Buckets in the state.
    pub(crate) fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Read the next entries of `bucket`: the entries of one non-empty read, up
    /// to the first key that is not due at `now`. The read holds no write gate. It
    /// changes nothing in the state, so a caller that fails keeps its place.
    ///
    /// The call makes at most [`READS_PER_PAGE`] reads. A page with no row that
    /// does not wait and whose walk is not done passed only released keys:
    /// [`Self::finish`] moves the cursor past them, and the bucket is not done.
    pub(crate) async fn read_due(
        &self,
        store: &AtomStore,
        bucket: u64,
        now: u64,
    ) -> Result<DuePage, SchemaError> {
        let state = self.buckets.get(&bucket).ok_or_else(|| {
            SchemaError::InvalidData(format!("time queue bucket {bucket} is not in the reader"))
        })?;
        let mut walk = state.walk.clone();
        let mut rows = Vec::new();
        let mut stop = None;
        let mut reads = 0usize;
        while rows.is_empty() && stop.is_none() && !walk.done() && reads < READS_PER_PAGE {
            reads += 1;
            let step = store.walk_step(&mut walk).await?;
            for (raw, value) in step.rows {
                let Ok(key) = String::from_utf8(raw) else {
                    continue;
                };
                match self.queue.entry_expiry(&key) {
                    Some(expiry) if expiry > now => {
                        stop = Some((key, expiry));
                        break;
                    }
                    _ => rows.push((key, value)),
                }
            }
        }
        Ok(DuePage {
            bucket,
            rows,
            walk,
            stop,
        })
    }

    /// The caller handled the keys of `page` that sort before `key`, and not
    /// `key` itself: the cursor goes to `key`, and the bucket waits until
    /// `retry_at` (an expiry in nanoseconds) before the next read.
    pub(crate) fn finish_before(&mut self, page: &DuePage, key: &str, retry_at: u64) {
        let (_, end) = self.queue.bucket_range(page.bucket);
        if let Some(state) = self.buckets.get_mut(&page.bucket) {
            state.walk = RangeWalk::new(key.as_bytes(), end.as_bytes(), self.page);
            state.waiting_until = Some(retry_at);
        }
    }

    /// The caller handled every key of `page`: move the cursor past them. A key
    /// that was not due stays the next key, and the bucket waits for its time.
    pub(crate) fn finish(&mut self, page: DuePage) {
        let walk = match &page.stop {
            Some((key, _)) => {
                let (_, end) = self.queue.bucket_range(page.bucket);
                RangeWalk::new(key.as_bytes(), end.as_bytes(), self.page)
            }
            None => page.walk,
        };
        if let Some(state) = self.buckets.get_mut(&page.bucket) {
            state.walk = walk;
            state.waiting_until = page.stop.map(|(_, expiry)| expiry);
        }
    }
}
