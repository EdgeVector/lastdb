//! Bare molecule membership. A present key need not have a fetched AtomEntry.

pub use super::compact_key::CompactRangeKey;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

/// Current eligibility known from the winning tip metadata, without that tip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyVisibility(u8);

impl KeyVisibility {
    pub fn from_tombstoned(tombstoned: bool) -> Self {
        Self(u8::from(tombstoned))
    }

    pub fn tombstoned(self) -> bool {
        self.0 & 1 != 0
    }
}

/// Original stored coordinates suffice to rebuild ordinary per-key requests.
#[derive(Clone, Debug)]
pub struct CoreStoredKey {
    pub hash: String,
    pub range: String,
}

#[derive(Debug, Default)]
pub struct MoleculeKeys {
    hashes: HashMap<Arc<str>, BTreeMap<CompactRangeKey, KeyVisibility>>,
    count: usize,
}

impl MoleculeKeys {
    pub fn len(&self) -> usize {
        self.count
    }

    /// No key is present. The type is public API now, and `len` needs this pair.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn hash_count(&self) -> usize {
        self.hashes.len()
    }

    pub fn contains_hash(&self, hash: &str) -> bool {
        self.hashes.contains_key(hash)
    }

    pub fn contains(&self, hash: &str, range: &CompactRangeKey) -> bool {
        self.hashes
            .get(hash)
            .is_some_and(|ranges| ranges.contains_key(range))
    }

    pub fn insert(
        &mut self,
        hash: &str,
        range: CompactRangeKey,
        visibility: KeyVisibility,
    ) -> bool {
        let previous = if let Some(ranges) = self.hashes.get_mut(hash) {
            ranges.insert(range, visibility)
        } else {
            let mut ranges = BTreeMap::new();
            ranges.insert(range, visibility);
            self.hashes.insert(Arc::from(hash), ranges);
            None
        };
        if previous.is_none() {
            self.count += 1;
        }
        previous.is_none()
    }

    pub fn remove(&mut self, hash: &str, range: &CompactRangeKey) -> bool {
        let Some(ranges) = self.hashes.get_mut(hash) else {
            return false;
        };
        let removed = ranges.remove(range).is_some();
        self.count -= usize::from(removed);
        // The hash allocation can remain after the last key. Its high-water
        // charge therefore remains until the complete domain is released.
        removed
    }

    pub fn ranges(
        &self,
        hash: &str,
        start: Bound<CompactRangeKey>,
        end: Bound<CompactRangeKey>,
    ) -> impl Iterator<Item = (&CompactRangeKey, &KeyVisibility)> {
        self.hashes
            .get(hash)
            .into_iter()
            .flat_map(move |ranges| ranges.range((start.clone(), end.clone())))
    }

    /// Merge the ordered hash streams. A capped preview does not visit every
    /// key: each hash seeks once, then only selected streams advance.
    pub fn page(
        &self,
        limit: usize,
        after: Option<(&CompactRangeKey, &str)>,
        include_tombstones: bool,
    ) -> (Vec<CoreStoredKey>, bool) {
        if limit == 0 {
            return (Vec::new(), self.count == 0);
        }
        let mut streams: Vec<_> = self
            .hashes
            .iter()
            .map(|(hash, ranges)| {
                let start = after.map_or(Bound::Unbounded, |(range, _)| {
                    Bound::Included(range.clone())
                });
                (hash, ranges.range((start, Bound::Unbounded)))
            })
            .collect();
        let mut first = BinaryHeap::with_capacity(streams.len());
        for (index, (hash, ranges)) in streams.iter_mut().enumerate() {
            if let Some(range) = next_visible(ranges, hash, after, include_tombstones) {
                first.push(Reverse((range, *hash, index)));
            }
        }
        let mut keys = Vec::with_capacity(limit.min(self.count));
        while keys.len() < limit {
            let Some(Reverse((range, hash, index))) = first.pop() else {
                break;
            };
            keys.push(CoreStoredKey {
                hash: hash.to_string(),
                range: range.to_storage(),
            });
            let (hash, ranges) = &mut streams[index];
            if let Some(range) = next_visible(ranges, hash, after, include_tombstones) {
                first.push(Reverse((range, *hash, index)));
            }
        }
        (keys, first.is_empty())
    }
}

fn next_visible<'a>(
    ranges: &mut std::collections::btree_map::Range<'a, CompactRangeKey, KeyVisibility>,
    hash: &str,
    after: Option<(&CompactRangeKey, &str)>,
    include_tombstones: bool,
) -> Option<&'a CompactRangeKey> {
    ranges
        .find(|(range, visibility)| {
            (include_tombstones || !visibility.tombstoned())
                && after.is_none_or(|bound| (*range, hash) > bound)
        })
        .map(|(range, _)| range)
}
