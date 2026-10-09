use super::*;

impl LastStore {
    /// Names of collection directories that exist on disk.
    pub fn collections_on_disk(&self) -> Result<Vec<String>> {
        let dir = self.root.join("data");
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        for e in fs::read_dir(dir)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                out.push(e.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    /// Return the deterministic hash-group placement for `id`.
    pub fn place(&self, collection: &str, id: &str) -> HashGroupPlacement {
        let shard = self.shard_of(id);
        let group_id = self.group_of(id);
        let relative_dir = PathBuf::from("data")
            .join(collection)
            .join(self.shard_name(shard))
            .join("g")
            .join(self.group_name(group_id));
        HashGroupPlacement {
            collection: collection.to_string(),
            shard,
            group_id,
            relative_dir,
        }
    }

    /// Return the estimated full owned heap for warm hash-group handles.
    pub fn hash_group_warm_stats(&self) -> HashGroupWarmStats {
        // Lock order is shards then key_index (`retain_group_ids` holds both).
        // Read the key index and release it before taking shards so this
        // sample does not invert that order.
        let (key_cache_groups, key_cache_bytes) = {
            let idx = self.key_index.lock().expect("poison");
            (idx.len(), idx.bytes())
        };
        let warm = self.shards.lock().expect("poison");
        HashGroupWarmStats {
            resident_groups: warm
                .handles
                .keys()
                .filter(|(_, _, group)| group.is_some())
                .count(),
            resident_bytes: warm.resident_bytes,
            budget_bytes: self.effective_warm_bytes(),
            budget_handles: self.opts.hash_group_warm_max_handles,
            open_append_handles: self.open_append_handles.load(Ordering::Relaxed),
            in_flight_cold_load_count: self.in_flight_cold_loads(),
            in_flight_cold_load_bytes: self.in_flight_cold_bytes(),
            eviction_events: self.eviction_events(),
            key_cache_groups,
            key_cache_bytes,
            key_cache_budget_bytes: self.opts.hash_group_key_cache_bytes,
        }
    }

    /// Highest warm resident byte count published since open.
    ///
    /// Unlike [`HashGroupWarmStats::resident_bytes`], this catches a charge
    /// that crossed the budget and was evicted again before anyone sampled it.
    pub fn hash_group_warm_peak_resident_bytes(&self) -> u64 {
        self.shards.lock().expect("poison").peak_resident_bytes
    }

    /// Drop reproducible read caches and correct every resident group's charge.
    ///
    /// This path never opens a cold group. The process-footprint sampler calls
    /// it when measured memory diverges from the budget projection, so a stale
    /// per-group charge cannot leave reclaimable bytes outside the warm limit.
    /// It preserves indexes where the configured budget permits and uses the
    /// normal LRU eviction only after every reproducible cache is gone.
    pub fn trim_hash_group_warm_cache_for_pressure(&self) -> Result<u64> {
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return Ok(0);
        }

        let handles: Vec<(ShardKey, ShardHandle)> = {
            let warm = self.shards.lock().expect("poison");
            warm.handles
                .iter()
                .filter(|(key, _)| key.2.is_some())
                .map(|(key, handle)| (key.clone(), Arc::clone(handle)))
                .collect()
        };
        let mut measured_before = 0u64;
        for (key, handle) in handles {
            // Never hold the warm-set lock across a handle lock. Point reads
            // and writes take these locks in the opposite order.
            let (before, residency) = {
                let mut sh = handle.lock().expect("poison");
                let before = estimate_shard_residency_locked(&sh).total;
                trim_shard_read_caches(&mut sh);
                (before, estimate_shard_residency_locked(&sh))
            };
            let mut warm = self.shards.lock().expect("poison");
            if warm
                .handles
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &handle))
            {
                measured_before = measured_before.saturating_add(before);
                // The handle lock is released, so a concurrent put may have
                // grown the group since the estimate. Its growth is charged by
                // that put through the budget gate, never here.
                let (index_budget, body_budget) = self.index_and_body_budget();
                warm.recharge_if_fits(&key, &handle, residency, index_budget, body_budget);
            }
        }

        self.evict_hash_group_warm_set()?;
        let measured_after = self.hash_group_warm_stats().resident_bytes;
        Ok(measured_before.saturating_sub(measured_after))
    }

    /// Warm-set stats restricted to one collection's resident hash groups.
    ///
    /// `resident_bytes` is the sum of per-handle estimates for that collection
    /// only. `budget_bytes` is still the global warm budget.
    pub fn hash_group_warm_stats_for(&self, collection: &str) -> HashGroupWarmStats {
        // Lock order is shards then key_index (`retain_group_ids` holds both).
        // Read the key index and release it before taking shards so this
        // sample does not invert that order.
        let (key_cache_groups, key_cache_bytes) = {
            let idx = self.key_index.lock().expect("poison");
            (idx.len(), idx.bytes())
        };
        let warm = self.shards.lock().expect("poison");
        let mut resident_groups = 0usize;
        let mut resident_bytes = 0u64;
        for (key, _) in warm.handles.iter() {
            if key.0 == collection && key.2.is_some() {
                resident_groups = resident_groups.saturating_add(1);
                resident_bytes =
                    resident_bytes.saturating_add(*warm.resident_by_key.get(key).unwrap_or(&0));
            }
        }
        HashGroupWarmStats {
            resident_groups,
            resident_bytes,
            budget_bytes: self.effective_warm_bytes(),
            budget_handles: self.opts.hash_group_warm_max_handles,
            open_append_handles: self.open_append_handles.load(Ordering::Relaxed),
            in_flight_cold_load_count: self.in_flight_cold_loads(),
            in_flight_cold_load_bytes: self.in_flight_cold_bytes(),
            eviction_events: self.eviction_events(),
            key_cache_groups,
            key_cache_bytes,
            key_cache_budget_bytes: self.opts.hash_group_key_cache_bytes,
        }
    }

    /// Number of durable hash-group directories on disk for `collection`.
    ///
    /// This is independent of the in-memory warm set: cold groups that have
    /// never been touched still count here and do **not** require a resident
    /// handle.
    pub fn hash_group_disk_group_count(&self, collection: &str) -> Result<usize> {
        match self.opts.layout_mode {
            LayoutMode::SegmentLog => Ok(0),
            LayoutMode::HashGroup => Ok(self.hash_groups_on_disk(collection)?.len()),
        }
    }

    pub(super) fn shard_of(&self, id: &str) -> u16 {
        let bits = self.opts.shard_bits;
        if bits == 0 {
            return 0;
        }
        // Shard on the placement key, not the raw id: a prefix walk can only
        // resolve a shard from a prefix if every row of a partition shares one.
        let h = self.hash_id(self.placement_key(id));
        (h as u16) >> (16 - bits)
    }

    pub(super) fn group_of(&self, id: &str) -> u32 {
        match self.opts.hash_group_key {
            HashGroupKey::FullKey => self.masked_group(id),
            HashGroupKey::PartitionPrefix => {
                let partition = partition_of(id);
                let base = self.masked_group(partition);
                self.group_at_offset(base, self.partition_offset(&id[partition.len()..]))
            }
        }
    }

    /// The id bytes that decide shard placement.
    pub(super) fn placement_key<'a>(&self, id: &'a str) -> &'a str {
        match self.opts.hash_group_key {
            HashGroupKey::FullKey => id,
            HashGroupKey::PartitionPrefix => partition_of(id),
        }
    }

    pub(super) fn masked_group(&self, bytes: &str) -> u32 {
        let mask = (1u64 << self.opts.hash_group_bits) - 1;
        (self.hash_id(bytes) & mask) as u32
    }

    /// Which of the `fanout` slots within a partition `rest` belongs to.
    pub(super) fn partition_offset(&self, rest: &str) -> u32 {
        let fanout = self.opts.hash_group_partition_fanout;
        if fanout <= 1 {
            return 0;
        }
        (self.hash_id(rest) & (u64::from(fanout) - 1)) as u32
    }

    pub(super) fn group_at_offset(&self, base: u32, offset: u32) -> u32 {
        let mask = (1u32 << self.opts.hash_group_bits) - 1;
        base.wrapping_add(offset) & mask
    }

    /// Every group a partition's rows can occupy, in ascending offset order.
    pub(super) fn partition_groups(&self, partition: &str) -> Vec<u32> {
        let base = self.masked_group(partition);
        let fanout = self.opts.hash_group_partition_fanout.max(1);
        let mut groups: Vec<u32> = (0..fanout)
            .map(|offset| self.group_at_offset(base, offset))
            .collect();
        // A fanout at the group-count ceiling can alias; keep the set unique so
        // a walk never loads the same handle twice.
        groups.sort_unstable();
        groups.dedup();
        groups
    }

    pub(super) fn hash_id(&self, id: &str) -> u64 {
        match self.opts.hash_algo {
            HashAlgo::Fnv1a64 => fnv1a64(id.as_bytes()),
        }
    }

    pub(super) fn shard_dir(&self, collection: &str, shard: u16) -> PathBuf {
        self.root
            .join("data")
            .join(collection)
            .join(self.shard_name(shard))
    }
}
