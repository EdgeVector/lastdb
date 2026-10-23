//! Snapshot-local live references. No mutable side file participates in recovery.

use super::bytes::u64_at;
use super::dependency::{Coordinate, MAX_DEPENDENCIES};
use super::generation::Generation;
use super::{artifact, corrupt, Current, ReadCall};
use crate::Result;
use std::collections::BTreeMap;
use std::path::Path;

mod bootstrap;
mod changes;
mod overflow;
mod payload;
mod scan;

pub(super) use changes::Changes;
pub(super) use scan::{add_directory_empty_root, observe_new_parts};

/// Each page resolves its references before another page enters the ledger.
/// Live counts and signed part deltas each admit 65536 entries (16 MiB at a
/// conservative 256 bytes/entry). Pending descriptors/extents admit 4096 entries
/// (1 MiB). A commit-local cache retains at most 26 pages and 25 part-header
/// identities; bootstrap uses a height-bounded stack. No payload is decoded.
/// Ordinary metadata copies exclude payload references. Only changed keys send
/// old/new extents to the resolver; full seeds account complete locator pages.
pub(super) const MAX_REFERENCE_CHANGES: usize = 4096;

/// Share checked immutable pages and part headers only within one publication.
/// The caller drops this cache before final count application and receipt write.
/// Its captured directory root is unused: resolution needs only immutable file
/// identities and the publication's fixed generation and feature checks.
pub(super) struct ReferenceCache {
    call: ReadCall,
}

impl ReferenceCache {
    pub(super) fn new(root: &Path, current: &Current) -> Self {
        Self {
            call: artifact::metadata_call(root, current.clone()),
        }
    }

    /// Work is charged afresh for every pending ledger, including cache hits.
    pub(super) fn resolve(&mut self, changes: &mut Changes) -> Result<()> {
        resolve_with_call(&mut self.call, changes, None, &mut Work::publication())
    }
}

/// A pending extent has at most u32::MAX blocks; an overflow chain has at most
/// five pages. This ceiling exceeds every valid pending ledger plus every page
/// of 65536 private parts. It therefore adds no post-ack resource restriction.
const MAX_PUBLICATION_WORK: u64 = MAX_REFERENCE_CHANGES as u64 * (u32::MAX as u64 + 5)
    + MAX_DEPENDENCIES as u64 * super::PAGES_PER_PART;
/// A complete snapshot has at most 65536 parts of 512 pages each.
/// Even ignoring packed-page offsets, a locator leaf fits at most 97 values
/// (40 descriptor bytes plus 128 value bytes); any page fits at most 409 keys.
/// Charge a maximal u32 block extent per value, five overflow pages per key,
/// each key visit, and the page itself. This loose bound remains below u64::MAX
/// for graphs created by valid writers: nonempty tree pages have one selected
/// path, while directory entries may share only the empty allocator template.
/// It imposes no separate legacy-open, directory, or record quota; bounded
/// caches and page-local ledgers limit retained memory.
const MAX_SEED_WORK: u64 = MAX_DEPENDENCIES as u64
    * super::PAGES_PER_PART
    * ((super::PAGE_BYTES / (40 + 128)) as u64 * u32::MAX as u64
        + (super::PAGE_BYTES / 40) as u64 * 6
        + 2);

#[derive(Default)]
pub(super) struct LiveParts {
    counts: BTreeMap<Coordinate, u64>,
}

impl LiveParts {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn from_receipt(receipt: &Generation) -> Result<Self> {
        if receipt.coordinates.len() != receipt.records.len()
            || receipt.coordinates.len() > MAX_DEPENDENCIES
        {
            return Err(corrupt("live-part receipt dimensions"));
        }
        let mut live = Self::new();
        for (coordinate, record) in receipt.coordinates.iter().zip(&receipt.records) {
            let count = u64_at(record, 24);
            if count == 0 || live.counts.insert(*coordinate, count).is_some() {
                return Err(corrupt("live-part receipt count or duplicate"));
            }
        }
        Ok(live)
    }

    /// Seed an uncounted root once, or a newly constructed complete snapshot.
    /// Every reachable part must belong to the caller's verified allowed set.
    pub(super) fn bootstrap(
        root: &Path,
        current: &Current,
        allowed: &[Coordinate],
    ) -> Result<Self> {
        bootstrap::collect(root, current, allowed)
    }

    pub(super) fn apply(&mut self, root: &Path, current: &Current, changes: Changes) -> Result<()> {
        self.apply_checked(root, current, changes, None, &mut Work::publication())
    }

    fn apply_checked(
        &mut self,
        root: &Path,
        current: &Current,
        changes: Changes,
        allowed: Option<&[Coordinate]>,
        work: &mut Work,
    ) -> Result<()> {
        let mut changes = changes;
        resolve_pending(root, current, &mut changes, allowed, work)?;
        let parts = changes.parts;
        let mut call = artifact::metadata_call(root, current.clone());
        let resolver = Resolver::new(&mut call, allowed, work);
        // Apply removals first: an equal-size replacement must not exceed the
        // part ceiling merely because its positive coordinates sort first.
        for (&coordinate, &delta) in parts.iter().filter(|(_, delta)| **delta < 0) {
            resolver.allow(coordinate)?;
            self.adjust(coordinate, delta)?;
        }
        for (coordinate, delta) in parts.into_iter().filter(|(_, delta)| *delta > 0) {
            resolver.allow(coordinate)?;
            self.adjust(coordinate, delta)?;
        }
        Ok(())
    }

    fn adjust(&mut self, coordinate: Coordinate, delta: i64) -> Result<()> {
        let previous = self.counts.get(&coordinate).copied().unwrap_or(0);
        let count = if delta < 0 {
            previous.checked_sub(delta.unsigned_abs())
        } else {
            previous.checked_add(delta as u64)
        }
        .ok_or_else(|| corrupt("live-part reference count underflow or overflow"))?;
        if count == 0 {
            self.counts.remove(&coordinate);
        } else {
            if previous == 0 && self.counts.len() >= MAX_DEPENDENCIES {
                return Err(super::resource());
            }
            self.counts.insert(coordinate, count);
        }
        Ok(())
    }

    pub(super) fn coordinates(&self) -> Vec<Coordinate> {
        self.counts.keys().copied().collect()
    }

    pub(super) fn reference_count(&self, coordinate: &Coordinate) -> Result<u64> {
        self.counts
            .get(coordinate)
            .copied()
            .filter(|count| *count > 0)
            .ok_or_else(|| corrupt("missing live-part reference count"))
    }
}

fn resolve_pending(
    root: &Path,
    current: &Current,
    changes: &mut Changes,
    allowed: Option<&[Coordinate]>,
    work: &mut Work,
) -> Result<()> {
    let mut call = artifact::metadata_call(root, current.clone());
    resolve_with_call(&mut call, changes, allowed, work)
}

/// Reduce page-local references before another ledger enters the count maps.
/// Payload spans never survive a resolution; only bounded file caches do.
fn resolve_with_call(
    call: &mut ReadCall,
    changes: &mut Changes,
    allowed: Option<&[Coordinate]>,
    work: &mut Work,
) -> Result<()> {
    let mut resolver = Resolver::new(call, allowed, work);
    for (key, delta) in std::mem::take(&mut changes.overflows) {
        overflow::resolve(&mut resolver, key, delta, &mut changes.parts)?;
    }
    for (span, delta) in std::mem::take(&mut changes.locations) {
        payload::resolve(&mut resolver, span, delta, &mut changes.parts)?;
    }
    Ok(())
}

struct Work {
    steps: u64,
    limit: u64,
}

impl Work {
    fn publication() -> Self {
        Self {
            steps: 0,
            limit: MAX_PUBLICATION_WORK,
        }
    }

    fn seed() -> Self {
        Self {
            steps: 0,
            limit: MAX_SEED_WORK,
        }
    }

    fn charge(&mut self) -> Result<()> {
        self.steps = self.steps.checked_add(1).ok_or_else(super::resource)?;
        if self.steps > self.limit {
            return Err(super::resource());
        }
        Ok(())
    }
}

struct Resolver<'a> {
    allowed: Option<&'a [Coordinate]>,
    work: &'a mut Work,
    call: &'a mut ReadCall,
}

impl<'a> Resolver<'a> {
    fn new(call: &'a mut ReadCall, allowed: Option<&'a [Coordinate]>, work: &'a mut Work) -> Self {
        Self {
            allowed,
            work,
            call,
        }
    }

    fn step(&mut self) -> Result<()> {
        self.work.charge()?;
        if self.call.pages.len() >= 24 || self.call.parts.len() >= 24 {
            *self.call = artifact::metadata_call(&self.call.root, self.call.current.clone());
        }
        Ok(())
    }

    fn allow(&self, coordinate: Coordinate) -> Result<()> {
        if self
            .allowed
            .is_some_and(|allowed| allowed.binary_search(&coordinate).is_err())
        {
            return Err(corrupt("live root references a part outside its receipt"));
        }
        Ok(())
    }
}

fn add_delta(
    parts: &mut BTreeMap<Coordinate, i64>,
    coordinate: Coordinate,
    delta: i64,
) -> Result<()> {
    let previous = parts.get(&coordinate).copied().unwrap_or(0);
    let next = previous
        .checked_add(delta)
        .ok_or_else(|| corrupt("live-part resolved delta overflow"))?;
    if next == 0 {
        parts.remove(&coordinate);
    } else {
        if previous == 0 && parts.len() >= MAX_DEPENDENCIES {
            return Err(super::resource());
        }
        parts.insert(coordinate, next);
    }
    Ok(())
}
