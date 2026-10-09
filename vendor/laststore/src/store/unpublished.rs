use super::*;

impl LastStore {
    /// Where a keys-only pass should read one group's ids from.
    ///
    /// A resident handle or a write pin always wins: it is the authority and
    /// may hold writes no snapshot has seen. Check the pin before the warm
    /// set, same order as put/get. Only when the group is *not* resident and
    /// *not* pinned — the case that would otherwise pay a cold segment read
    /// purely to recover ids we already had — may the walk answer from a
    /// cheaper tier: the in-memory key-index cache first, then the on-disk
    /// sidecar.
    pub(super) fn group_key_source(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
    ) -> Result<GroupKeySource<'_>> {
        self.group_key_source_with_window(collection, shard, group, None)
    }

    pub(super) fn group_key_source_with_window(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
        window: Option<keysidecar::KeyWindow<'_>>,
    ) -> Result<GroupKeySource<'_>> {
        if self.opts.layout_mode == LayoutMode::HashGroup {
            let key = (collection.to_string(), shard, group);
            // Pins then shards: same order as put/get. A pin holds unflushed
            // writes the key index and sidecar do not see. Read those ids
            // under the pin lock and return Cached: a Live ScanHandle drops
            // through `retire_leased_scan_handle`, which `sync_open`s the pin
            // and clears `dirty_ops` on a read path.
            if let Some(handle) = self.pin_table_handle(&key) {
                let keys = {
                    let shard = handle.lock().expect("poison");
                    shard.live_keys()?
                };
                self.id_tier_resident.fetch_add(1, Ordering::Relaxed);
                return Ok(GroupKeySource::Cached(Arc::new(keys)));
            }
            let resident = self
                .shards
                .lock()
                .expect("poison")
                .handles
                .contains_key(&key);
            if !resident {
                if self.opts.hash_group_key_cache_bytes > 0 {
                    if let Some(keys) = self.key_index.lock().expect("poison").get(&key) {
                        self.id_tier_key_cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(GroupKeySource::Cached(keys));
                    }
                }
                // On-disk tier. The in-memory cache spans one process and one
                // bounded budget, so the first walk after a restart — or after
                // the cache drops this group — still arrives here. `read_valid`
                // validates the sidecar against the group's segments and
                // answers `None` on any doubt, falling through to the
                // authoritative load below.
                if self.sidecar_enabled() {
                    let dir = self.handle_dir(collection, shard, group);
                    let ids = match window {
                        Some(window) => keysidecar::read_valid_window(&dir, window),
                        None => keysidecar::read_valid(&dir),
                    };
                    if let Some(ids) = ids {
                        self.id_tier_sidecar_hits.fetch_add(1, Ordering::Relaxed);
                        // Never admitted to the in-memory key cache here: a
                        // concurrent authority mutation may have moved past
                        // this sidecar, and a residency recheck alone would
                        // not justify the admission for concurrent readers.
                        return Ok(GroupKeySource::Cached(Arc::new(ids)));
                    }
                }
                self.id_tier_live_scans.fetch_add(1, Ordering::Relaxed);
            } else {
                self.id_tier_resident.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // Not a hash-group home: there are no id tiers to consult, and the
            // scan below is the only path. Counting it as a live scan keeps the
            // sum equal to the number of resolutions on every layout.
            self.id_tier_live_scans.fetch_add(1, Ordering::Relaxed);
        }
        let key = (collection.to_string(), shard, group);
        Ok(GroupKeySource::Live(self.scan_handle_by_key(&key)?))
    }

    pub(super) fn warm_or_leased_handle(
        &self,
        key: &ShardKey,
        admission: WarmAdmission,
        touch: WarmTouch,
    ) -> Result<Option<ShardHandle>> {
        let leased = {
            let mut warm = self.shards.lock().expect("poison");
            if let Some(h) = warm.handles.get(key).cloned() {
                if admission == WarmAdmission::Point {
                    warm.promote_point(key);
                }
                warm.touch(key);
                warm.note_admission(key, admission, touch);
                return Ok(Some(h));
            }
            warm.leased_handle(key)
        };
        if let Some(leased) = leased {
            // A scan lease holds this group unpublished. It is the group's only
            // handle: share it rather than load a second authority.
            if admission == WarmAdmission::Scan {
                return Ok(Some(leased));
            }
            let handle = self.publish_warm_handle(key, leased, admission, touch)?;
            self.evict_hash_group_warm_set()?;
            return Ok(Some(handle));
        }
        Ok(None)
    }

    /// Parse one hash group without publishing it into the warm set.
    ///
    /// A pin, published handle, or live lease is the group's only authority:
    /// this returns that handle and does not call `load_shard`. Otherwise it
    /// parses the group, refuses [`Error::ColdGroupTooLarge`], and returns an
    /// unpublished handle. The caller drops an unpinned `Shard`.
    pub(super) fn open_group_unpublished(&self, key: ShardKey) -> Result<ShardHandle> {
        if let Some(handle) = self.existing_unpublished_authority(&key) {
            return Ok(handle);
        }
        let gate = Self::cold_load_gate_index(&key);
        let _cold_load_guard = self.cold_load_gates[gate].lock().expect("poison");
        if let Some(handle) = self.existing_unpublished_authority(&key) {
            return Ok(handle);
        }
        self.load_unpublished_shard(&key)
    }

    /// Reuse the live authority, or load an existing disk group. An absent
    /// point must not create an empty group. The cold-load gate covers the
    /// authority recheck and disk presence check, as it does for pin writes.
    pub(super) fn open_existing_group_unpublished(
        &self,
        key: ShardKey,
    ) -> Result<Option<ShardHandle>> {
        if let Some(handle) = self.existing_unpublished_authority(&key) {
            return Ok(Some(handle));
        }
        let gate = Self::cold_load_gate_index(&key);
        let _cold_load_guard = self.cold_load_gates[gate].lock().expect("poison");
        if let Some(handle) = self.existing_unpublished_authority(&key) {
            return Ok(Some(handle));
        }
        if !self.handle_dir(&key.0, key.1, key.2).try_exists()? {
            return Ok(None);
        }
        self.load_unpublished_shard(&key).map(Some)
    }

    pub(super) fn existing_unpublished_authority(&self, key: &ShardKey) -> Option<ShardHandle> {
        if let Some(handle) = self.pins.lock().expect("poison").handles.get(key).cloned() {
            return Some(handle);
        }
        let mut warm = self.shards.lock().expect("poison");
        if let Some(handle) = warm.handles.get(key).cloned() {
            return Some(handle);
        }
        warm.leased_handle(key)
    }

    /// True when a pin, warm handle, lease, or on-disk dir exists for `key`.
    ///
    /// [`Self::load_hash`] and [`Self::handles_for_partition`] skip the rest so
    /// an empty fanout slot does not count as a cold load.
    pub(super) fn unpublished_group_present(&self, key: &ShardKey) -> bool {
        self.existing_unpublished_authority(key).is_some()
            || self.handle_dir(&key.0, key.1, key.2).exists()
    }

    pub(super) fn load_unpublished_shard(&self, key: &ShardKey) -> Result<ShardHandle> {
        let (collection, shard, group) = key;
        self.shard_loads.fetch_add(1, Ordering::Relaxed);
        let dir = self.handle_dir(collection, *shard, *group);
        let estimate = estimate_group_on_disk_bytes(&dir);
        self.refuse_if_cold_group_too_large(collection, *shard, *group, &dir, estimate)?;
        let _in_flight = InFlightAdmission::enter(self, estimate);
        let mut loaded = load_shard(
            dir,
            collection.clone(),
            *shard,
            self.opts.data_key,
            self.opts.collection_policy(collection),
            self.opts.sorted_segments,
        )?;
        // Callers hold this group's cold-load gate. The hook runs before the
        // handle is returned, so a delete on the same stripe waits. This handle
        // is not published.
        loaded.max_sorted_tail_bytes = self
            .opts
            .max_open_tail_bytes
            .min(self.opts.max_segment_bytes);
        if loaded.data_key.is_some() {
            loaded.max_sorted_tail_bytes = loaded
                .max_sorted_tail_bytes
                .min(SORTED_ENCRYPTED_TAIL_BYTES);
        }
        if group.is_some() && self.sidecar_enabled() {
            loaded.sidecar_stamps = keysidecar::recorded_stamps(&loaded.dir);
        }
        loaded.fd_gauge = Arc::clone(&self.open_append_handles);
        loaded.frame_compression_stats = Arc::clone(&self.frame_compression_stats);
        {
            let mut meta = self.meta.lock().expect("poison");
            meta.next_csn = meta.next_csn.max(loaded.max_csn.saturating_add(1));
        }
        Ok(Arc::new(Mutex::new(loaded)))
    }

    pub(super) fn refuse_if_cold_group_too_large(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
        dir: &Path,
        estimate: u64,
    ) -> Result<()> {
        let Some(group_index) = group else {
            return Ok(());
        };
        let cap = self.opts.effective_max_cold_group_load_bytes();
        let soft_cap = self.opts.effective_soft_cold_group_load_bytes();
        if soft_cap > 0 && estimate > soft_cap && (cap == 0 || estimate <= cap) {
            eprintln!(
                "LASTSTORE_COLD_GROUP_OVER_SOFT_CAP collection={collection} shard={shard} \
                 group={group_index:#05x} bytes={estimate} soft_cap={soft_cap} hard_cap={cap} \
                 action=load dir={}",
                dir.display()
            );
        }
        if cap > 0 && estimate > cap {
            eprintln!(
                "LASTSTORE_COLD_GROUP_TOO_LARGE collection={collection} shard={shard} \
                 group={group_index:#05x} bytes={estimate} cap={cap} dir={}",
                dir.display()
            );
            return Err(Error::ColdGroupTooLarge {
                collection: collection.to_string(),
                shard,
                group: group_index,
                bytes: estimate,
                cap,
            });
        }
        Ok(())
    }

    pub(super) fn hash_range_gate_passes(&self, prefix: &str) -> bool {
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return false;
        }
        if self.opts.hash_group_key != HashGroupKey::PartitionPrefix {
            return false;
        }
        if self.groups_per_partition_read() > 16 {
            return false;
        }
        partition_of(prefix).ends_with(PARTITION_SEP)
    }

    pub(super) fn groups_per_partition_read(&self) -> u32 {
        match self.opts.layout_mode {
            LayoutMode::SegmentLog => 0,
            LayoutMode::HashGroup => match self.opts.hash_group_key {
                HashGroupKey::PartitionPrefix => self
                    .opts
                    .hash_group_partition_fanout
                    .min(1u32 << self.opts.hash_group_bits),
                HashGroupKey::FullKey => 1u32 << self.opts.hash_group_bits,
            },
        }
    }
}
