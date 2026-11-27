//! The memory layer of the commit log: logged commits that no run holds yet.
//!
//! Each commit becomes one sorted segment. A new segment combines with the newer segments
//! below its size, so a layer of n records keeps about log2(n) segments.

use crate::disk_run::Found;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Sorted, unique keys with the newest body of each key. `None` is a tombstone.
type Segment = Vec<(Vec<u8>, Option<Arc<[u8]>>)>;

/// Logged commits, newest segment first. A seal writes the `frozen` part into a run while
/// new commits enter the `active` part.
#[derive(Debug, Clone, Default)]
pub(super) struct Memory {
    active: Vec<Arc<Segment>>,
    frozen: Vec<Arc<Segment>>,
    active_commits: u64,
    frozen_commits: u64,
    active_bytes: u64,
    frozen_bytes: u64,
    /// Logical bytes the active tombstones hide. A seal and the merges after it reclaim them.
    active_hidden: u64,
}

fn combine(newer: &Segment, older: &Segment) -> Segment {
    let mut out = Vec::with_capacity(newer.len() + older.len());
    let (mut a, mut b) = (newer.iter().peekable(), older.iter().peekable());
    loop {
        let next = match (a.peek(), b.peek()) {
            (Some(x), Some(y)) => match x.0.cmp(&y.0) {
                Ordering::Less => a.next(),
                Ordering::Greater => b.next(),
                Ordering::Equal => {
                    b.next();
                    a.next()
                }
            },
            (Some(_), None) => a.next(),
            (None, Some(_)) => b.next(),
            (None, None) => return out,
        };
        out.extend(next.cloned());
    }
}

fn found(body: Option<Arc<[u8]>>) -> Found {
    body.map_or(Found::Tombstone, |v| Found::Value(v.to_vec()))
}

impl Memory {
    /// Logged commits that no published CURRENT counts.
    pub(super) fn commits(&self) -> u64 {
        self.active_commits + self.frozen_commits
    }

    /// Commits in the part a seal is writing.
    pub(super) fn frozen_commits(&self) -> u64 {
        self.frozen_commits
    }

    /// Log bytes of the active part. Tombstone sizes are [`Self::active_hidden`], not part of this.
    pub(super) fn active_log_bytes(&self) -> u64 {
        self.active_bytes
    }

    /// Logical bytes the active tombstones hide. Known deletes pass the value length; the rest
    /// use the mean logical size of one live key.
    pub(super) fn active_hidden(&self) -> u64 {
        self.active_hidden
    }

    /// Log bytes of both parts.
    pub(super) fn bytes(&self) -> u64 {
        self.active_bytes.saturating_add(self.frozen_bytes)
    }

    /// True while a seal has a frozen part to write.
    pub(super) fn has_frozen(&self) -> bool {
        !self.frozen.is_empty()
    }

    /// Add one commit: sorted records with unique keys, its log bytes, and the logical bytes
    /// its tombstones hide.
    pub(super) fn push(&mut self, records: Vec<(Vec<u8>, Found)>, cost: u64, hidden: u64) {
        let mut seg: Segment = records
            .into_iter()
            .map(|(key, f)| match f {
                Found::Value(v) => (key, Some(Arc::from(v))),
                Found::Tombstone => (key, None),
            })
            .collect();
        while self
            .active
            .first()
            .is_some_and(|older| older.len() <= seg.len())
        {
            let older = self.active.remove(0);
            seg = combine(&seg, &older);
        }
        self.active.insert(0, Arc::new(seg));
        self.active_commits += 1;
        self.active_bytes = self.active_bytes.saturating_add(cost);
        self.active_hidden = self.active_hidden.saturating_add(hidden);
    }

    /// Move the active part to the frozen part. False when a frozen part is still unwritten
    /// or nothing is active.
    pub(super) fn freeze(&mut self) -> bool {
        if !self.frozen.is_empty() || self.active.is_empty() {
            return false;
        }
        self.frozen = std::mem::take(&mut self.active);
        self.frozen_commits = std::mem::take(&mut self.active_commits);
        self.frozen_bytes = std::mem::take(&mut self.active_bytes);
        self.active_hidden = 0;
        true
    }

    /// This layer after a published run holds the frozen part.
    pub(super) fn without_frozen(&self) -> Self {
        Self {
            active: self.active.clone(),
            active_commits: self.active_commits,
            active_bytes: self.active_bytes,
            active_hidden: self.active_hidden,
            ..Self::default()
        }
    }

    fn newest_first(&self) -> impl DoubleEndedIterator<Item = &Arc<Segment>> {
        self.active.iter().chain(&self.frozen)
    }

    /// The newest logged record of `key`, a value or a tombstone.
    pub(super) fn get(&self, key: &[u8]) -> Option<Found> {
        self.newest_first().find_map(|seg| {
            let at = seg.binary_search_by(|(k, _)| k.as_slice().cmp(key)).ok()?;
            Some(found(seg[at].1.clone()))
        })
    }

    fn merged<'a>(
        segs: impl DoubleEndedIterator<Item = &'a Arc<Segment>>,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> BTreeMap<Vec<u8>, Option<Arc<[u8]>>> {
        let mut out = BTreeMap::new();
        for seg in segs.rev() {
            let from = seg.partition_point(|(k, _)| k.as_slice() < lower);
            for (key, body) in &seg[from..] {
                if upper.is_some_and(|u| key.as_slice() >= u) {
                    break;
                }
                out.insert(key.clone(), body.clone());
            }
        }
        out
    }

    /// The newest logged record of each key in `[lower, upper)`, tombstones included.
    pub(super) fn range(&self, lower: &[u8], upper: &[u8]) -> Vec<(Vec<u8>, Found)> {
        Self::merged(self.newest_first(), lower, Some(upper))
            .into_iter()
            .map(|(key, body)| (key, found(body)))
            .collect()
    }

    /// The newest record of each key in the frozen part, in key order.
    pub(super) fn frozen_records(&self) -> BTreeMap<Vec<u8>, Option<Arc<[u8]>>> {
        Self::merged(self.frozen.iter(), &[], None)
    }
}
