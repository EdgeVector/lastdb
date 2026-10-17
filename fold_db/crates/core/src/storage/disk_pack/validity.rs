//! Bounded dependency metadata, with no retained values or storage objects.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

mod headers;
mod history;
mod proof;
pub(super) use headers::{changed, header_version, ChangedCoordinate};
pub(super) use proof::ValidatedWitness;
mod witnesses;

const MAX_DEPENDENCIES: usize = 10_000;
const MAX_DEPENDENCY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RangeDependency {
    collection: String,
    start: Vec<u8>,
    end: Vec<u8>,
}

#[derive(Clone, Default)]
struct DependencySet {
    points: BTreeMap<(String, String), Option<String>>,
    headers: BTreeMap<(String, String), u64>,
    ranges: BTreeSet<RangeDependency>,
    bytes: usize,
}

impl DependencySet {
    fn intersects(&self, change: &ChangedCoordinate) -> bool {
        let (collection, key, digest) = (
            change.collection.as_str(),
            change.key.as_str(),
            &change.digest,
        );
        if change.header.is_some()
            && self
                .headers
                .contains_key(&(collection.to_string(), key.to_string()))
        {
            return false;
        }
        let selected = self.points.get(&(collection.to_string(), key.to_string()));
        if selected == Some(digest) {
            return false;
        }
        selected.is_some()
            || self.ranges.iter().any(|range| {
                range.collection == collection
                    && range.start.as_slice() <= key.as_bytes()
                    && key.as_bytes() < range.end.as_slice()
            })
    }
}

/// Exact reads for one captured call, including absences and requested ranges.
#[derive(Default)]
pub(super) struct Dependencies {
    reads: DependencySet,
    overflow: bool,
    cached: Option<u64>,
    inherited: BTreeSet<u64>,
}

impl Dependencies {
    /// Isolate one observed present source without cloning the call's other reads.
    pub(super) fn single_point(&self, collection: &str, key: &str) -> Option<Self> {
        if self.overflow {
            return None;
        }
        let coordinate = (collection.to_string(), key.to_string());
        let digest = self.reads.points.get(&coordinate)?.as_ref()?.clone();
        let mut source = Self::default();
        if !source.room(
            192usize
                .saturating_add(collection.len())
                .saturating_add(key.len()),
        ) {
            return None;
        }
        source.reads.points.insert(coordinate, Some(digest));
        Some(source)
    }

    pub(super) fn disable(&mut self) {
        self.overflow = true;
        self.cached = None;
        self.reads = DependencySet::default();
    }

    pub(super) fn points(&mut self, coordinates: &[(String, String)], values: &[Option<Vec<u8>>]) {
        for ((collection, key), value) in coordinates.iter().zip(values) {
            self.point(collection, key, value.as_deref());
        }
    }

    pub(super) fn rows(&mut self, collection: &str, rows: &[(String, Vec<u8>)]) {
        for (key, value) in rows {
            self.point(collection, key, Some(value));
        }
    }

    fn point(&mut self, collection: &str, key: &str, value: Option<&[u8]>) {
        if self.overflow {
            return;
        }
        let coordinate = (collection.to_string(), key.to_string());
        let digest = value.map(crate::hex::sha256_hex);
        if let Some(previous) = self.reads.points.get(&coordinate) {
            if previous != &digest {
                self.disable();
            }
            return;
        }
        if self.room(
            128usize
                .saturating_add(collection.len())
                .saturating_add(key.len())
                .saturating_add(64),
        ) {
            self.reads.points.insert(coordinate, digest);
        }
    }

    pub(super) fn range(&mut self, collection: &str, start: &[u8], end: &[u8]) {
        if self.overflow {
            return;
        }
        let dependency = RangeDependency {
            collection: collection.to_string(),
            start: start.to_vec(),
            end: end.to_vec(),
        };
        if self.reads.ranges.contains(&dependency) {
            return;
        }
        if self.room(
            128usize
                .saturating_add(collection.len())
                .saturating_add(start.len())
                .saturating_add(end.len()),
        ) {
            self.reads.ranges.insert(dependency);
        }
    }

    fn room(&mut self, bytes: usize) -> bool {
        self.cached = None;
        if self
            .reads
            .points
            .len()
            .saturating_add(self.reads.ranges.len())
            >= MAX_DEPENDENCIES
            || self.reads.bytes.saturating_add(bytes) > MAX_DEPENDENCY_BYTES
        {
            self.disable();
            return false;
        }
        self.reads.bytes += bytes;
        true
    }
}

struct Changes {
    revision: u64,
    coordinates: Vec<ChangedCoordinate>,
    bytes: usize,
}

struct Witness {
    revision: u64,
    reads: Arc<DependencySet>,
    verified: Option<(u64, bool)>,
}

#[derive(Default)]
pub(super) struct Validity {
    floor: u64,
    history: VecDeque<Changes>,
    history_keys: usize,
    history_bytes: usize,
    witnesses: BTreeMap<u64, Witness>,
    witness_bytes: usize,
    next: u64,
}
