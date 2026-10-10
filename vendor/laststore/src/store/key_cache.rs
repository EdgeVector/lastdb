use super::*;

/// A group's ids and sidecar state, taken while its handle was authoritative.
pub(super) struct GroupIdSnapshot {
    pub(super) keys: GroupKeys,
    pub(super) sidecar: Option<(PathBuf, Vec<keysidecar::SegmentStamp>, GroupResidue)>,
    pub(super) cache_keys: bool,
}

/// Ids of one hash group, retained after its handle leaves the warm set.
pub(super) type GroupKeys = Arc<BTreeSet<String>>;

/// Per-entry accounting overhead charged on top of the id bytes: the `String`
/// header plus its allocation and the owning `BTreeSet` node slot.
pub(super) const KEY_CACHE_ENTRY_OVERHEAD: u64 = 64;

/// Ids of groups that have been evicted from the warm set.
///
/// A keys-only pass visits **every** group in the collection, and rebuilding
/// one group's index costs a full segment read + decrypt + parse — while the
/// ids alone are a small fraction of that segment. Keeping the ids of evicted
/// groups lets a repeat walk answer from memory and pay no shard load at all.
///
/// Invariant, and the whole reason this is safe: **a key is never present here
/// and in [`ShardWarmSet::handles`] at the same time.** Entries are written
/// only when a handle is evicted, and dropped when a handle becomes resident.
/// Readers never admit sidecar results: a sidecar answers one keys-only pass
/// from disk and is not retained here.
/// Every mutation path (`put`, `delete`, compaction) must first obtain a
/// handle through [`LastStore::shard_handle_at`], which drops the snapshot, so
/// a stale entry cannot survive a write even if a new writer is added later.
///
/// A pin is the write authority. `pinned` records that under this lock so
/// [`LastStore::retain_group_ids`] can skip `insert` without taking the pin
/// table while it already holds `shards`. `pin_gens` stays after the pin is
/// reaped so an in-flight retain whose snapshot predates the pin cannot land.
#[derive(Default)]
pub(super) struct KeyIndexCache {
    pub(super) entries: HashMap<ShardKey, GroupKeys>,
    pub(super) order: VecDeque<ShardKey>,
    pub(super) bytes_by_key: HashMap<ShardKey, u64>,
    pub(super) bytes: u64,
    /// Groups whose pin is the write authority. `insert` must not land while
    /// this contains the key. Cleared when the pin is reaped.
    pub(super) pinned: HashSet<ShardKey>,
    /// Bumped when a key first enters `pinned`. Kept after reap so a retain
    /// that sampled this generation before the pin cannot insert a pre-pin
    /// snapshot.
    pub(super) pin_gens: HashMap<ShardKey, u64>,
}

/// Where one group's ids came from for a keys-only pass.
pub(super) struct ScanHandle<'a> {
    pub(super) store: &'a LastStore,
    pub(super) key: ShardKey,
    pub(super) handle: Option<ShardHandle>,
}

impl ScanHandle<'_> {
    pub(super) fn handle(&self) -> &ShardHandle {
        self.handle.as_ref().expect("scan handle released once")
    }
}

impl Drop for ScanHandle<'_> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let _ = self.store.refresh_scan_handle(&self.key, &handle);
        // Do not remove a published handle while this lease still owns a
        // reference. A concurrent writer must either retain this authority or
        // load it only after the lease reference is gone.
        let _ = self.store.retire_leased_scan_handle(&self.key, handle);
        let _ = self.store.finish_scan_admission(&self.key);
    }
}

pub(super) enum GroupKeySource<'a> {
    /// The live shard handle — authoritative, and already resident.
    Live(ScanHandle<'a>),
    /// Ids retained from when the group was last evicted. No segment read.
    Cached(GroupKeys),
}

/// Whether a group's ascending id stream can still contribute to a page.
#[derive(PartialEq, Eq)]
pub(super) enum Offer {
    /// The id was considered; keep walking this group.
    Considered,
    /// The page is full and this id is at or above its ceiling. Because the
    /// group's stream ascends, nothing later in it can enter the page either.
    StopGroup,
}

/// Bounded k-way merge of the ascending id streams of several groups.
///
/// Retains only the `limit` smallest ids offered so far. A page is therefore
/// `O(groups + limit)` rather than `O(ids in the band)`: once the buffer is
/// full, [`Self::offer`] answers [`Offer::StopGroup`] for any id at or above
/// the ceiling, and each remaining group stops after the first id it cannot
/// place. Under `HashGroup` the walk visits every group, and a paged walk used
/// to merge *every* id of the band into a map and then truncate — so each page
/// of a 1.6M-key walk re-read the whole keyspace, and paging through it was
/// quadratic in band size (fold: the atom-partition rekey, whose 1,500-page
/// resume never finished).
///
/// `usize::MAX` (the unpaged walks) never fills, so those keep merging
/// everything, exactly as before.
pub(super) struct BoundedMerge {
    pub(super) ids: BTreeSet<String>,
    pub(super) limit: usize,
}

/// One prefix page: listed ids, plus the bodies copied during that same open.
pub(super) struct UnpublishedPrefixPage {
    pub(super) ids: Vec<String>,
    pub(super) bodies: HashMap<String, Vec<u8>>,
}

impl BoundedMerge {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            ids: BTreeSet::new(),
            limit,
        }
    }

    /// Offer one id from a group's ascending stream.
    ///
    /// Dropping the id at or above a full page's ceiling is exact, not a
    /// heuristic: an id is in the `limit` smallest of the union only if fewer
    /// than `limit` ids are smaller than it, and the buffer already holds
    /// `limit` ids that are.
    pub(super) fn offer(&mut self, id: &str) -> Offer {
        if self.ids.len() == self.limit {
            match self.ids.last() {
                Some(ceiling) if id >= ceiling.as_str() => return Offer::StopGroup,
                _ => {}
            }
            self.ids.insert(id.to_string());
            self.ids.pop_last();
        } else {
            self.ids.insert(id.to_string());
        }
        Offer::Considered
    }

    pub(super) fn into_vec(self) -> Vec<String> {
        self.ids.into_iter().collect()
    }
}

/// Merge the ids of one group that fall under `prefix` into `merged`.
///
/// Shared by both key sources so the live index and a cached id set cannot
/// drift in cursor or prefix semantics.
pub(super) fn collect_prefix<'a, I: Iterator<Item = &'a String>>(
    ids: I,
    prefix: &str,
    skip_exact: Option<&str>,
    merged: &mut BoundedMerge,
) -> u64 {
    let mut visited = 0u64;
    for id in ids {
        if !id.starts_with(prefix) {
            break;
        }
        visited += 1;
        if skip_exact == Some(id.as_str()) {
            continue;
        }
        if merged.offer(id) == Offer::StopGroup {
            break;
        }
    }
    visited
}

pub(super) fn fold_max_u64_id_suffix<'a, I: Iterator<Item = &'a String>>(
    ids: I,
    prefix: &str,
    marker: &str,
    greatest: &mut Option<u64>,
) -> u64 {
    let mut visited = 0;
    for id in ids {
        if !id.starts_with(prefix) {
            break;
        }
        visited += 1;
        let Some((_, suffix)) = id.rsplit_once(marker) else {
            continue;
        };
        let Ok(value) = suffix.parse::<u64>() else {
            continue;
        };
        *greatest = Some(greatest.map_or(value, |current| current.max(value)));
    }
    visited
}

impl KeyIndexCache {
    pub(super) fn get(&self, key: &ShardKey) -> Option<GroupKeys> {
        self.entries.get(key).cloned()
    }

    /// Groups whose ids are currently retained.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Charged bytes across those retained id lists.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn forget(&mut self, key: &ShardKey) {
        if self.entries.remove(key).is_some() {
            let was = self.bytes_by_key.remove(key).unwrap_or_default();
            self.bytes = self.bytes.saturating_sub(was);
            self.order.retain(|k| k != key);
        }
    }

    pub(super) fn is_pinned(&self, key: &ShardKey) -> bool {
        self.pinned.contains(key)
    }

    pub(super) fn pin_gen(&self, key: &ShardKey) -> u64 {
        self.pin_gens.get(key).copied().unwrap_or(0)
    }

    /// Record that `key` has a pin. Drops any cached ids. Bumps `pin_gens`
    /// the first time this pin generation is recorded.
    pub(super) fn mark_pinned(&mut self, key: &ShardKey) {
        self.forget(key);
        if self.pinned.insert(key.clone()) {
            let gen = self.pin_gens.entry(key.clone()).or_insert(0);
            *gen = gen.saturating_add(1);
        }
    }

    /// Drop the pin record and any cached ids. Keeps `pin_gens` so a retain
    /// that sampled the prior generation cannot insert after reap.
    pub(super) fn forget_pin(&mut self, key: &ShardKey) {
        self.pinned.remove(key);
        self.forget(key);
    }

    pub(super) fn insert(&mut self, key: ShardKey, keys: GroupKeys, budget: u64) {
        if self.pinned.contains(&key) {
            return;
        }
        self.forget(&key);
        let cost = keys
            .iter()
            .map(|id| id.len() as u64 + KEY_CACHE_ENTRY_OVERHEAD)
            .sum::<u64>();
        // A single group larger than the whole budget is not worth evicting
        // everything else for.
        if cost > budget {
            return;
        }
        self.bytes = self.bytes.saturating_add(cost);
        self.bytes_by_key.insert(key.clone(), cost);
        self.entries.insert(key.clone(), keys);
        self.order.push_back(key);
        while self.bytes > budget {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest).is_some() {
                let was = self.bytes_by_key.remove(&oldest).unwrap_or_default();
                self.bytes = self.bytes.saturating_sub(was);
            }
        }
    }
}
