use super::*;

impl LastStore {
    pub(super) fn evict_hash_group_warm_set(&self) -> Result<()> {
        self.evict_hash_group_warm_set_inner(false, None, None)
            .map(|_| ())
    }

    /// Enforce the full warm limit with elastic scan probation.
    ///
    /// Scan entries may use every byte that point-read entries leave free. If
    /// a scan crosses the shared limit, it rejects its own newest entry before
    /// it considers a point-read entry. This preserves the point working set
    /// without the fixed sub-limit that forced hot scans to reload groups even
    /// when the shared budget still had space.
    // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
    pub(super) fn evict_hash_group_warm_set_inner(
        &self,
        scan_only: bool,
        reject_scan_candidate: Option<&ShardKey>,
        resident_limit: Option<u64>,
    ) -> Result<u64> {
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return Ok(0);
        }
        // Descriptors first, and independently: this is the cheap reclaim, it
        // keeps every group resident, and it runs even on a home whose byte
        // budget is disabled.
        self.reclaim_append_descriptors()?;
        if resident_limit.is_none() && self.effective_warm_bytes() == 0 {
            return Ok(0);
        }
        // Give up reproducible bytes before giving up an index.
        // Clear pressure trims toward the configured ceiling. That value is
        // not the `fits` budget. Under pressure the body budget is 0, so
        // every Plain body leaves before a group drop. Do not full-trim on
        // the clear path: `evict_to_bytes(resident - 1)` must drop one group,
        // not trim bodies instead.
        let pressure_high = self.host_pressure_high();
        if pressure_high {
            self.trim_all_hash_group_bodies()?;
        } else {
            self.trim_hash_group_read_caches(self.opts.hash_group_warm_bytes)?;
        }
        let shed_interactive = pressure_shed_interactive();
        let now_ms = wall_unix_ms();
        // Pinned candidates stay resident for this pass. Aborting the whole
        // walk on the first pinned LRU left the set over budget whenever any
        // in-flight put/get held that group — the live 4 GiB soak's leftover
        // ~4.6 MiB. Skip those keys and keep evicting unpinned groups.
        // Atom-plane groups and fresh interactive indexes use the same skip.
        let mut skipped: HashSet<ShardKey> = HashSet::new();
        let mut groups_evicted = 0u64;
        // Scan entries leave before point entries. Once every scan entry left
        // is pinned (a long scan lease), fall through to the point LRU rather
        // than stop over the limit. Scan-only passes never do: a scan does not
        // displace the point working set.
        let mut scan_entries_done = false;
        loop {
            let candidate = {
                let mut warm = self.shards.lock().expect("poison");
                let total_over = match resident_limit {
                    Some(limit) => warm.resident_bytes > limit,
                    None => !self.hash_group_warm_set_within_budgets(&warm),
                };
                if !total_over {
                    return Ok(groups_evicted);
                }
                let from_scan_order = scan_only || !scan_entries_done && !warm.scan_keys.is_empty();
                let key = if scan_only {
                    // Reject the just-read group when its admission would
                    // overflow the shared budget. A sequential cold scan then
                    // preserves the scan entries that already fit instead of
                    // rotating every entry out in order. Point entries remain
                    // protected because this branch selects scan entries only.
                    reject_scan_candidate
                        .filter(|key| warm.scan_keys.contains(*key))
                        .and_then(|key| warm.take_from_order(key))
                        .or_else(|| warm.pop_scan_lru())
                } else if from_scan_order {
                    match warm.pop_scan_lru() {
                        Some(key) => Some(key),
                        None => {
                            scan_entries_done = true;
                            continue;
                        }
                    }
                } else {
                    warm.pop_lru()
                };
                let Some(key) = key else {
                    return Ok(groups_evicted);
                };
                if !skipped.insert(key.clone()) {
                    warm.touch(&key);
                    if from_scan_order && !scan_only {
                        // Every scan entry left is pinned; try point entries.
                        scan_entries_done = true;
                        continue;
                    }
                    // Cycled through the remaining order; every leftover group
                    // is pinned. Restore recency and leave the overshoot until
                    // a caller drops its handle.
                    return Ok(groups_evicted);
                }
                let Some(handle) = warm.handles.get(&key).cloned() else {
                    continue;
                };
                if key.2.is_none()
                    || Arc::strong_count(&handle) > 2
                    || warm.eviction_protected(&key, pressure_high, shed_interactive, now_ms)
                {
                    warm.touch(&key);
                    continue;
                }
                Some((key, handle))
            };

            let Some((key, handle)) = candidate else {
                continue;
            };
            let mut sh = handle.lock().expect("poison");
            let ids = self.snapshot_group_ids_locked(&mut sh, true)?;
            let (evicted, pin_gen) = {
                let mut warm = self.shards.lock().expect("poison");
                let current = warm
                    .handles
                    .get(&key)
                    .is_some_and(|h| Arc::ptr_eq(h, &handle));
                if Arc::strong_count(&handle) > 2 || !current {
                    warm.touch(&key);
                    continue;
                }
                let evicted = warm.remove(&key).is_some();
                // Sample while this lock still owns the remove. A pin can
                // load, flush, and reap after we release, and that bumps
                // `pin_gen`; retain then sees a mismatch and skips the
                // pre-pin snapshot. Lock order: shards then key_index.
                let pin_gen = if evicted {
                    self.key_index.lock().expect("poison").pin_gen(&key)
                } else {
                    0
                };
                (evicted, pin_gen)
            };
            drop(sh);
            if evicted {
                groups_evicted = groups_evicted.saturating_add(1);
                self.eviction_events.fetch_add(1, Ordering::Relaxed);
                self.retain_group_ids(key, ids, pin_gen);
            }
        }
    }

    /// Snapshot a group's ids while the caller holds the handle lock.
    ///
    /// The bulky part of the shard (cached segments, bodies, frames) is what
    /// the warm budget is protecting; the ids are a small fraction of it but
    /// cost a whole segment re-read to rebuild. Sync the open tail when
    /// `force_sync` is set or the handle is dirty.
    ///
    /// Eviction holds this lock across the snapshot, then rechecks the warm
    /// set (`Arc::ptr_eq`, `strong_count <= 2`) before remove. A concurrent
    /// delete either clones this Arc (eviction skips) or waits for the lock.
    /// Do not skip the snapshot on a stamp mismatch: that left the old
    /// key-index entry in place, so a keys walk still returned deleted ids.
    pub(super) fn snapshot_group_ids_locked(
        &self,
        sh: &mut Shard,
        force_sync: bool,
    ) -> Result<GroupIdSnapshot> {
        let write_sidecar = self.sidecar_enabled();
        if force_sync || sh.dirty_ops != 0 || sh.dirty_bytes != 0 {
            Self::sync_open(sh)?;
        }
        let cache_keys = !sh.uses_sorted_index();
        let keys: GroupKeys = Arc::new(if cache_keys {
            sh.live_keys()?
        } else {
            BTreeSet::new()
        });
        let residue = sh.residue;
        // Stamp the segments in the same critical section as the ids, while
        // this handle still owns the group and `sync_open` has just made the
        // files match it. Read stamps after `live_keys` so a concurrent flush
        // cannot stamp this handle's old ids onto a longer file.
        let sidecar = if write_sidecar && cache_keys {
            let stamps = keysidecar::segment_stamps(&sh.dir)?;
            // Matching stamps mean the file on disk already describes exactly
            // these segments, so it already holds these ids: the sidecar is
            // only ever written together with the stamps it is valid for.
            // Rewriting it would burn an encode, an fsync and a rename to
            // reproduce bytes that are already there. Anything else — a roll,
            // an append, no sidecar at all, a corrupt one — leaves
            // `sidecar_stamps` unequal and falls through to the write.
            if sh.sidecar_stamps.as_deref() == Some(stamps.as_slice()) {
                None
            } else {
                Some((sh.dir.clone(), stamps, residue))
            }
        } else {
            None
        };
        Ok(GroupIdSnapshot {
            keys,
            sidecar,
            cache_keys,
        })
    }

    /// Keep a group's ids after its handle is gone: the on-disk sidecar and
    /// the in-memory key cache. Call only once no handle owns `key`.
    ///
    /// `pin_gen` is sampled while the caller still holds `shards` after it
    /// removed the handle. A pin that loads, flushes, and reaps before this
    /// method runs bumps the generation; the insert then sees a mismatch.
    pub(super) fn retain_group_ids(&self, key: ShardKey, ids: GroupIdSnapshot, pin_gen: u64) {
        // A pin is the write authority. `pin_handle_for_write` records it
        // under the key_index lock (pins then key_index). Treat the group as
        // pinned only when that record is set. An external pin-table bit can
        // go stale across a reap and leave `pinned` set after the pin is gone.
        {
            let key_index = self.key_index.lock().expect("poison");
            if key_index.is_pinned(&key) {
                return;
            }
        }
        {
            let key_index = self.key_index.lock().expect("poison");
            if key_index.is_pinned(&key) || key_index.pin_gen(&key) != pin_gen {
                return;
            }
        }
        if let Some((dir, stamps, residue)) = ids.sidecar {
            // Advisory cache: failing to persist it costs the next cold walk
            // one segment read and nothing else, so a write error must not
            // fail the eviction that already succeeded.
            let _ = keysidecar::write(&dir, &stamps, residue, &ids.keys);
        }
        let budget = self.opts.hash_group_key_cache_bytes;
        // Lock order: shards -> key_index. Publishing forgets the entry under
        // the same `shards` lock, so an insert here either sees the new handle
        // and skips, or lands before the publish and is forgotten by it.
        // `holds()` does not see pins; `is_pinned` / `pin_gen` are the pin
        // half of that handshake.
        let warm = self.shards.lock().expect("poison");
        let mut key_index = self.key_index.lock().expect("poison");
        if warm.holds(&key)
            || key_index.is_pinned(&key)
            || key_index.pin_gen(&key) != pin_gen
            || budget == 0
            || !ids.cache_keys
        {
            if !warm.holds(&key) {
                key_index.forget(&key);
            }
            return;
        }
        key_index.insert(key, ids.keys, budget);
    }

    /// End the last lease on a scan handle that never fit the warm set.
    ///
    /// Such a group leaves memory without passing through eviction, so this
    /// does eviction's id work for it: without it a scan that overflows the
    /// budget would never write or repair the group's id sidecar, and every
    /// later walk would pay a full segment read for it.
    pub(super) fn retire_leased_scan_handle(
        &self,
        key: &ShardKey,
        handle: ShardHandle,
    ) -> Result<()> {
        let (ids, pin_gen) = {
            let mut sh = handle.lock().expect("poison");
            // Spill while the lease is still visible: a loader adopts this Arc
            // instead of reading a disk image that lacks these bytes.
            Self::sync_open(&mut sh)?;
            let pin_gen = {
                let mut warm = self.shards.lock().expect("poison");
                let leased_here = warm
                    .leased
                    .get(key)
                    .is_some_and(|leased| std::ptr::eq(leased.as_ptr(), Arc::as_ptr(&handle)));
                if !leased_here || Arc::strong_count(&handle) > 1 {
                    return Ok(());
                }
                warm.leased.remove(key);
                // Same sample point as eviction: while `shards` still owns
                // the remove. Lock order: shards then key_index.
                self.key_index.lock().expect("poison").pin_gen(key)
            };
            (self.snapshot_group_ids_locked(&mut sh, false)?, pin_gen)
        };
        drop(handle);
        self.retain_group_ids(key.clone(), ids, pin_gen);
        Ok(())
    }
}
