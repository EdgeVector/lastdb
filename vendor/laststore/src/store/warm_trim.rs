use super::*;

impl LastStore {
    /// Reclaim warm-set bytes that are pure read cache, before any index is
    /// evicted to make room.
    ///
    /// A group is charged for two very different things. Its `index` costs a
    /// full segment parse to rebuild and is what the warm set exists to keep;
    /// its `seg_bytes` and `values` are copies of bytes already on disk and
    /// cost a `pread` to reproduce. Charging both to one budget let a single
    /// large group evict hundreds of small groups' indexes to make room for
    /// bytes the OS page cache already holds — measured on the primary, one
    /// `atoms` group is ~12.5 MB of sealed segments behind a ~45 KB index, so
    /// ~330 of them consumed the whole 4 GiB budget and collapsed the resident
    /// set from 4,444 groups to ~861. Every group displaced that way is a cold
    /// load on the next write.
    ///
    /// Trimming keeps the expensive half and drops the cheap one. It runs down
    /// to a low-water mark rather than exactly to budget, so a store hovering
    /// at the limit does not re-walk the recency order on every put.
    pub(super) fn trim_hash_group_read_caches(&self, budget: u64) -> Result<()> {
        // No byte budget means nothing to trim *toward*: the caller may still be
        // over the handle cap, but read caches hold no descriptors, so walking
        // the recency order here would reclaim nothing it needs.
        if budget == 0 {
            return Ok(());
        }
        // 15/16 of budget. Hysteresis: without it a store parked at the limit
        // pays a full LRU walk per operation to reclaim a handful of bytes.
        let low_water = budget - (budget / 16);
        let order = {
            let warm = self.shards.lock().expect("poison");
            // Nothing reproducible left to give up: go straight to eviction
            // rather than locking every resident handle to be told so. This is
            // the steady state once indexes alone fill the budget, and it is
            // reached on every operation, so the check has to be this cheap.
            if warm.resident_bytes <= budget || warm.trimmable_bytes == 0 {
                return Ok(());
            }
            warm.trimmable_lru_order()
        };
        for key in order {
            // Segment-log handles are not warm-set members and are not charged.
            if key.2.is_none() {
                continue;
            }
            let handle = {
                let warm = self.shards.lock().expect("poison");
                if warm.resident_bytes <= low_water {
                    return Ok(());
                }
                match warm.handles.get(&key) {
                    Some(handle) => Arc::clone(handle),
                    // Evicted by another thread since the order was snapshotted.
                    None => continue,
                }
            };
            // Never hold the warm-set lock across a handle lock: `put` and
            // `get` take the handle first and the warm set after.
            let trimmed = {
                let mut sh = handle.lock().expect("poison");
                trim_shard_read_caches(&mut sh)
            };
            if trimmed {
                let residency = estimate_shard_residency(&handle);
                // A trim only shrinks the group; growth here is a concurrent
                // write's, and that writer charges it through the gate.
                let (index_budget, body_budget) = self.index_and_body_budget();
                self.shards.lock().expect("poison").recharge_if_fits(
                    &key,
                    &handle,
                    residency,
                    index_budget,
                    body_budget,
                );
            }
        }
        Ok(())
    }

    /// Drop every reproducible body while host pressure is high.
    ///
    /// Unlike [`Self::trim_hash_group_read_caches`], a budget of 0 does not
    /// return immediately. The pass stops when `trimmable_bytes` is 0.
    /// Indexes stay. A non-growing charge still fits, so the smaller charge
    /// is recorded even when the set is over the body budget.
    pub(super) fn trim_all_hash_group_bodies(&self) -> Result<()> {
        let order = {
            let warm = self.shards.lock().expect("poison");
            if warm.trimmable_bytes == 0 {
                return Ok(());
            }
            warm.trimmable_lru_order()
        };
        for key in order {
            if key.2.is_none() {
                continue;
            }
            let handle = {
                let warm = self.shards.lock().expect("poison");
                if warm.trimmable_bytes == 0 {
                    return Ok(());
                }
                match warm.handles.get(&key) {
                    Some(handle) => Arc::clone(handle),
                    None => continue,
                }
            };
            let trimmed = {
                let mut sh = handle.lock().expect("poison");
                trim_shard_read_caches(&mut sh)
            };
            if trimmed {
                let residency = estimate_shard_residency(&handle);
                let (index_budget, body_budget) = self.index_and_body_budget();
                self.shards.lock().expect("poison").recharge_if_fits(
                    &key,
                    &handle,
                    residency,
                    index_budget,
                    body_budget,
                );
            }
        }
        Ok(())
    }

    /// Is the warm set inside the byte budget?
    ///
    /// The descriptor budget is deliberately **not** part of this test. Eviction
    /// is the wrong instrument for reclaiming a descriptor: it gives up a
    /// group's whole index to close one file, and the group has to be cold-read
    /// back the next time anything touches it. Descriptors are reclaimed on
    /// their own by [`Self::reclaim_append_descriptors`], which closes the file
    /// and leaves the group resident. Bytes govern residency; descriptors govern
    /// descriptors.
    pub(super) fn hash_group_warm_set_within_budgets(&self, warm: &ShardWarmSet) -> bool {
        let budget = self.effective_warm_bytes();
        budget == 0 || warm.resident_bytes <= budget
    }

    /// Is the store holding more append descriptors than the cap allows?
    ///
    /// `false` when no cap is configured, matching the field docs.
    pub(super) fn over_append_descriptor_cap(&self) -> bool {
        let cap = self.opts.hash_group_warm_max_handles;
        cap != 0 && self.open_append_handles.load(Ordering::Relaxed) > cap
    }

    /// Close least-recently-used append handles until the descriptor budget is
    /// satisfied, leaving every group resident.
    ///
    /// Safe for the same reason [`Self::flush`]'s reclaim is: buffered bytes
    /// live in `open_buf`, not in the `File`, and `spill_open` reopens the
    /// segment on demand. So this costs the next writer to that group one
    /// `open(2)` and costs a reader nothing at all — where evicting the group
    /// would cost a full segment read, decrypt and index rebuild.
    ///
    /// Reclaims down to a low-water mark rather than to the cap, so a store
    /// sitting at the limit does not run a reclaim pass per append.
    ///
    /// `try_lock` on candidates: a group whose lock is held is being used right
    /// now, which is both the wrong LRU choice and a lock this path must not
    /// wait behind. Skipping it is correct on both counts.
    pub(super) fn reclaim_append_descriptors(&self) -> Result<()> {
        if !self.over_append_descriptor_cap() {
            return Ok(());
        }
        let cap = self.opts.hash_group_warm_max_handles;
        // 15/16 of the cap: enough headroom that the pass amortizes, small
        // enough that it never gives back a meaningful share of the budget.
        let target = cap - (cap / 16).max(1);
        let mut candidates: Vec<ShardKey> = {
            let warm = self.shards.lock().expect("poison");
            warm.order.values().cloned().collect()
        };
        let pin_keys: Vec<ShardKey> = self
            .pins
            .lock()
            .expect("poison")
            .handles
            .keys()
            .cloned()
            .collect();
        candidates.extend(pin_keys);
        for key in candidates {
            if self.open_append_handles.load(Ordering::Relaxed) <= target {
                break;
            }
            let handle = self
                .shards
                .lock()
                .expect("poison")
                .handles
                .get(&key)
                .cloned()
                .or_else(|| self.pin_table_handle(&key));
            let Some(handle) = handle else { continue };
            let Ok(mut sh) = handle.try_lock() else {
                continue;
            };
            if sh.open_file.is_none() {
                continue;
            }
            // Sync before closing: the descriptor goes away either way, and a
            // group whose buffered bytes never reached disk would have to be
            // rewritten by the next spill from `open_buf`. Losing the sync is
            // not a correctness problem, but paying it here keeps the reclaim
            // from silently deferring durability work to the next flush.
            Self::sync_open(&mut sh)?;
            // Close an fd only after the tail is in `file_len`. Do not remove
            // a held pin: try_lock already skipped a locked handle, and this
            // path only drops the descriptor.
            if sh.file_len < sh.open_len {
                continue;
            }
            sh.open_file = None;
        }
        Ok(())
    }
}
