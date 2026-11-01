//! A survey of a queue: the number of entries, and the buckets that the
//! directory does not list.
//!
//! The verifier and the recount need to know which buckets hold entries that
//! the janitor cannot find. The survey reads in steps of one bounded range read,
//! so a caller can run it in the budget of a verifier step.
//!
//! Where the survey looks:
//!
//! 1. **The listed buckets.** It reads the whole directory, then walks the
//!    entries of each listed bucket.
//! 2. **The unlisted buckets.** The slices that can hold a live entry are known
//!    by arithmetic (`TimeQueue::net_slices`). A slice that has no directory row
//!    and holds a live key is an unlisted bucket. The survey walks it too and
//!    counts its entries as entries without a directory row.
//!
//! The survey reads at many roots. A bucket that a writer lists between the
//! check of its row and the walk looks unlisted for a moment, so the survey
//! reads the rows of the unlisted buckets again at the end
//! ([`QueueSurvey::confirmed_unlisted`]) and keeps only those that still have
//! none.

use super::keys::TimeQueue;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use std::collections::{BTreeMap, HashSet, VecDeque};

/// Directory rows that one read asks for.
const DIRECTORY_PAGE: usize = 1_000;
/// Listed buckets the survey walks, at most.
const MAX_BUCKETS: usize = 100_000;
/// Entry keys of one unlisted bucket that the survey keeps for the report.
const KEPT_KEYS: usize = 8;

/// Where the survey is.
#[derive(Debug, PartialEq, Eq)]
enum Stage {
    /// It has not read the directory.
    Listing,
    /// It walks the buckets it listed.
    Walking,
    Done,
}

/// An unlisted bucket, with the entries the survey met in it.
#[derive(Debug, Default, Clone)]
pub(crate) struct UnlistedBucket {
    /// Entries in the bucket.
    pub(crate) entries: u64,
    /// A few of their keys.
    pub(crate) sample: Vec<String>,
}

/// The state of a survey.
#[derive(Debug)]
pub(crate) struct QueueSurvey {
    queue: TimeQueue,
    lifetime_nanos: u64,
    now_nanos: u64,
    page_rows: usize,
    stage: Stage,
    /// Buckets to walk: the bucket, and whether the directory lists it.
    todo: VecDeque<(u64, bool)>,
    walk: Option<(u64, bool, RangeWalk)>,
    unlisted: BTreeMap<u64, UnlistedBucket>,
    /// Entries read.
    entries: u64,
    /// False when the directory lists more buckets than the survey walks.
    complete: bool,
    /// Buckets whose probe gave up.
    unknown: u64,
}

impl QueueSurvey {
    /// A survey of `queue` at `now_nanos`, for entries that live `lifetime_nanos`.
    pub(crate) fn new(
        queue: TimeQueue,
        now_nanos: u64,
        lifetime_nanos: u64,
        page_rows: usize,
    ) -> Self {
        Self {
            queue,
            lifetime_nanos,
            now_nanos,
            page_rows: page_rows.max(16),
            stage: Stage::Listing,
            todo: VecDeque::new(),
            walk: None,
            unlisted: BTreeMap::new(),
            entries: 0,
            complete: true,
            unknown: 0,
        }
    }

    pub(crate) fn complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn entries(&self) -> u64 {
        self.entries
    }

    /// Buckets whose probe gave up. The survey did not walk them.
    pub(crate) fn unknown(&self) -> u64 {
        self.unknown
    }

    /// Read one more piece: the directory and the unlisted slices on the first
    /// call, then one page of one bucket. Returns true when the survey is done.
    pub(crate) async fn step(&mut self, store: &AtomStore) -> Result<bool, SchemaError> {
        match self.stage {
            Stage::Listing => {
                self.list(store).await?;
                self.stage = Stage::Walking;
            }
            Stage::Walking => self.walk_page(store).await?,
            Stage::Done => {}
        }
        Ok(self.stage == Stage::Done)
    }

    async fn list(&mut self, store: &AtomStore) -> Result<(), SchemaError> {
        let (start, end) = self.queue.directory_range(0, u64::MAX);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), DIRECTORY_PAGE);
        let mut listed: HashSet<u64> = HashSet::new();
        while !walk.done() {
            for (key, _) in store.walk_step(&mut walk).await?.rows {
                let bucket = String::from_utf8(key)
                    .ok()
                    .and_then(|key| self.queue.directory_bucket(&key));
                if let Some(bucket) = bucket {
                    if listed.len() >= MAX_BUCKETS {
                        self.complete = false;
                        break;
                    }
                    listed.insert(bucket);
                }
            }
        }
        let mut order: Vec<u64> = listed.iter().copied().collect();
        order.sort_unstable();
        self.todo
            .extend(order.into_iter().map(|bucket| (bucket, true)));
        let slices = self.queue.net_slices(self.now_nanos, self.lifetime_nanos);
        let slices: Vec<u64> = slices
            .into_iter()
            .filter(|bucket| !listed.contains(bucket))
            .collect();
        let found = self.queue.find_unlisted(store, &slices).await?;
        self.unknown += found.unknown as u64;
        self.todo
            .extend(found.unlisted.into_iter().map(|bucket| (bucket, false)));
        Ok(())
    }

    async fn walk_page(&mut self, store: &AtomStore) -> Result<(), SchemaError> {
        if self.walk.is_none() {
            let Some((bucket, listed)) = self.todo.pop_front() else {
                self.stage = Stage::Done;
                return Ok(());
            };
            let (start, end) = self.queue.bucket_range(bucket);
            let walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), self.page_rows);
            self.walk = Some((bucket, listed, walk));
        }
        let Some((bucket, listed, walk)) = self.walk.as_mut() else {
            return Ok(());
        };
        let (bucket, listed) = (*bucket, *listed);
        let step = store.walk_step(walk).await?;
        let finished = walk.done();
        for (key, _) in step.rows {
            let Ok(key) = String::from_utf8(key) else {
                continue;
            };
            self.entries += 1;
            if !listed {
                let bucket = self.unlisted.entry(bucket).or_default();
                bucket.entries += 1;
                if bucket.sample.len() < KEPT_KEYS {
                    bucket.sample.push(key);
                }
            }
        }
        if finished {
            self.walk = None;
        }
        Ok(())
    }

    /// The unlisted buckets that still have no directory row. A bucket that a
    /// writer listed while the survey walked leaves the list.
    pub(crate) async fn confirmed_unlisted(
        &self,
        store: &AtomStore,
    ) -> Result<Vec<(u64, UnlistedBucket)>, SchemaError> {
        if self.unlisted.is_empty() {
            return Ok(Vec::new());
        }
        let buckets: Vec<u64> = self.unlisted.keys().copied().collect();
        let rows = store
            .raw()
            .inner()
            .exists_many(
                buckets
                    .iter()
                    .map(|bucket| self.queue.directory_key(*bucket).into_bytes())
                    .collect(),
            )
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read bucket rows: {error}")))?;
        Ok(buckets
            .into_iter()
            .zip(rows)
            .filter(|(_, listed)| !listed)
            .filter_map(|(bucket, _)| self.unlisted.get(&bucket).map(|b| (bucket, b.clone())))
            .collect())
    }
}
