use super::*;
use crate::read_receipt_io::lock as read_lock;

impl LastStore {
    pub(super) fn shard_handle_at(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
        admission: WarmAdmission,
        touch: WarmTouch,
    ) -> Result<ShardHandle> {
        let key = (collection.to_string(), shard, group);
        if let Some(handle) = self.pin_table_handle(&key) {
            return Ok(handle);
        }
        if let Some(handle) = self.warm_or_leased_handle(&key, admission, touch)? {
            return Ok(handle);
        }
        // The first lookup can miss while another thread loads this group.
        // Keep its stripe through publication, then recheck the warm set.
        // A completed delete cannot leave an older, still-loading image that
        // later becomes this group's authority after the delete is evicted.
        let gate = Self::cold_load_gate_index(&key);
        let _cold_load_guard = read_lock(&self.cold_load_gates[gate]).expect("poison");
        if let Some(handle) = self.pin_table_handle(&key) {
            return Ok(handle);
        }
        if let Some(handle) = self.warm_or_leased_handle(&key, admission, touch)? {
            return Ok(handle);
        }
        self.cold_load_handle(collection, shard, group, &key, admission, touch)
    }

    /// Load the group from disk and publish it. The caller holds the cold-load
    /// gate for `key` and has rechecked the pin table and warm set.
    fn cold_load_handle(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
        key: &ShardKey,
        admission: WarmAdmission,
        touch: WarmTouch,
    ) -> Result<ShardHandle> {
        self.shard_loads.fetch_add(1, Ordering::Relaxed);
        crate::read_receipt::update(|receipt| receipt.loader_calls += 1);
        let dir = self.handle_dir(collection, shard, group);
        let estimate = estimate_group_on_disk_bytes(&dir);
        // Refuse a whole-group load the process cannot survive. Hash groups
        // only: a segment-log shard is the whole collection by design. The
        // 2026-09-21 primary restart loop was this exact load — one
        // `metadata` group at 39 GB, pulled in by the first point write after
        // boot, then the 16 GiB memory guard. A refused load is a request
        // error naming the group; the operator drops the group, not the node.
        //
        // That is the HARD cap. Below it, a group over the SOFT cap still loads
        // (Tom, 2026-09-25: a node must stay bootable and upgradeable — a
        // 1.08 GB keep_small group of superseded snapshots made every new
        // build refuse to boot the primary). The store only marks it: the
        // rewrite belongs to the product compactors, which hold the backup
        // publish-target lock and apply the headroom/isolation policy. Before
        // the soft cap they could not compact such a group at all, because
        // compaction loads it first and the load was refused.
        self.refuse_if_cold_group_too_large(collection, shard, group, &dir, estimate)?;
        // Attribution only: charging the full on-disk size into the warm
        // budget before parse would evict indexes to make room for sealed
        // segments that trim immediately drops. The publish gate below makes
        // room against the measured residency; footprint defence covers the
        // rest.
        let _in_flight = InFlightAdmission::enter(self, estimate);
        let mut loaded = load_shard(
            dir,
            collection.to_string(),
            shard,
            self.opts.data_key,
            self.opts.collection_policy(collection),
            self.opts.sorted_segments,
        )?;
        self.configure_loaded_shard(&mut loaded, group);
        let handle = Arc::new(Mutex::new(loaded));
        let mut touch = touch;
        // Compaction and snapshot have no id. One index scan on the cold load
        // marks an atoms group that actually holds the shared plane. The hot
        // hit path above does not scan.
        if !touch.atom && collection == "atoms" {
            let shard = read_lock(&handle).expect("poison");
            if shard.index.keys().any(|id| is_atom_plane_id(id)) {
                touch.atom = true;
            }
        }
        // This handle is about to become the authority for the group, and it is
        // the only way to reach a mutation. Any id snapshot taken when it was
        // last evicted is now superseded — drop it before publishing the
        // handle, so no walk can read a snapshot that a writer has moved past.
        read_lock(&self.key_index).expect("poison").forget(key);
        let handle = self.publish_warm_handle(key, handle, admission, touch)?;
        if admission == WarmAdmission::Point {
            // Room was made before the charge was published. This pass still
            // reclaims append descriptors, and it catches a point charge that
            // had to be published over budget because everything else was
            // pinned.
            self.evict_hash_group_warm_set()?;
        }
        Ok(handle)
    }

    /// Size the open tail, carry the group's recorded sidecar stamps, and join
    /// the store's descriptor and compression gauges before the handle is
    /// reachable.
    fn configure_loaded_shard(&self, loaded: &mut Shard, group: Option<u32>) {
        loaded.max_sorted_tail_bytes = self
            .opts
            .max_open_tail_bytes
            .min(self.opts.max_segment_bytes);
        if loaded.data_key.is_some() {
            loaded.max_sorted_tail_bytes = loaded
                .max_sorted_tail_bytes
                .min(SORTED_ENCRYPTED_TAIL_BYTES);
        }
        // Carry what the group's existing sidecar already claims into the fresh
        // handle. Without this a handle starts with no idea what is on disk, so
        // the eviction below rewrites a byte-identical sidecar (the flush too,
        // for a big group) every time a group cycles through the warm set.
        // Under a scan that overflows the warm budget that is the dominant
        // cost of the read: measured on the primary, a single `list` rewrote
        // sidecars at ~51/s with a 0% content-change rate.
        if group.is_some() && self.sidecar_enabled() {
            loaded.sidecar_stamps = keysidecar::recorded_stamps(&loaded.dir);
        }
        // Join the store's descriptor gauge before the handle is reachable, so
        // no append can open a file this store is not counting.
        loaded.fd_gauge = Arc::clone(&self.open_append_handles);
        loaded.frame_compression_stats = Arc::clone(&self.frame_compression_stats);
        {
            let mut meta = read_lock(&self.meta).expect("poison");
            meta.next_csn = meta.next_csn.max(loaded.max_csn.saturating_add(1));
        }
    }

    /// Publish a handle for `key` into the warm set without crossing the byte
    /// budget.
    ///
    /// Room is made *before* the handle and its charge are published. Publish
    /// then evict left `resident_bytes` over the budget for the whole eviction
    /// pass — sidecar fsyncs included — and the self-metrics sampler read it
    /// there (2026-09-26: 98 of 609 primary samples, up to 325 MB over).
    ///
    /// - A point group gives up its own read caches, then other groups' read
    ///   caches, then other groups in LRU order (scan entries first). If it
    ///   still does not fit — every other group pinned, or this group alone
    ///   larger than the budget — it is published anyway: it is the only
    ///   handle a write can use.
    /// - A scan group gives up its own and other groups' read caches but
    ///   evicts nothing. If it still does not fit, the scan rejects its own
    ///   newest group: the handle serves the lease unpublished and uncharged,
    ///   and the resident working set is left alone, as before.
    ///
    /// Returns the handle the caller must use, which is an already published
    /// or leased handle when another thread admitted the group first.
    pub(super) fn publish_warm_handle(
        &self,
        key: &ShardKey,
        mut handle: ShardHandle,
        admission: WarmAdmission,
        touch: WarmTouch,
    ) -> Result<ShardHandle> {
        let charged = key.2.is_some();
        let mut residency = charged.then(|| estimate_shard_residency(&handle));
        let mut room = WarmRoom::default();
        loop {
            let step = {
                let mut warm = read_lock(&self.shards).expect("poison");
                if let Some(existing) = warm.handles.get(key).cloned() {
                    if admission == WarmAdmission::Point {
                        warm.promote_point(key);
                    }
                    warm.touch(key);
                    warm.note_admission(key, admission, touch);
                    return Ok(existing);
                }
                match warm.leased_handle(key) {
                    Some(leased) if !Arc::ptr_eq(&leased, &handle) => {
                        if admission == WarmAdmission::Scan {
                            return Ok(leased);
                        }
                        Admit::Adopt(leased)
                    }
                    _ => {
                        let (index_budget, body_budget) = self.index_and_body_budget();
                        let residency_now = residency.unwrap_or_default();
                        let total = residency_now.total;
                        let fits = warm.fits(key, residency_now, index_budget, body_budget);
                        // Drain hold closes the point exception. A charge that
                        // does not fit stays leased, so resident cannot climb
                        // back to the configured maximum between eviction steps.
                        // Background molecule tips lease too. Interactive,
                        // unspecified, and atom-plane groups still publish:
                        // the handle is the only one a write can use. That
                        // does not raise the ceiling.
                        let point_over_budget = room.exhausted
                            && admission == WarmAdmission::Point
                            && !self.warm_drain_hold()
                            && (touch.atom || touch.class != AdmitClass::Background);
                        if fits || point_over_budget {
                            if point_over_budget && !fits {
                                self.note_index_over_budget_publish();
                            }
                            warm.leased.remove(key);
                            warm.touch(key);
                            read_lock(&self.key_index).expect("poison").forget(key);
                            warm.handles.insert(key.clone(), Arc::clone(&handle));
                            if admission == WarmAdmission::Scan {
                                warm.mark_scan(key);
                            }
                            if let Some(residency) = residency {
                                warm.recharge(key, residency);
                            }
                            warm.note_admission(key, admission, touch);
                            Admit::Done(Arc::clone(&handle))
                        } else if room.exhausted {
                            // A scan or background group that still does not fit.
                            read_lock(&self.key_index).expect("poison").forget(key);
                            warm.leased.insert(key.clone(), Arc::downgrade(&handle));
                            Admit::Done(Arc::clone(&handle))
                        } else {
                            Admit::MakeRoom(index_budget.checked_sub(total))
                        }
                    }
                }
            };
            match step {
                Admit::Done(handle) => return Ok(handle),
                Admit::Adopt(leased) => {
                    handle = leased;
                    residency = charged.then(|| estimate_shard_residency(&handle));
                    room = WarmRoom::default();
                }
                Admit::MakeRoom(limit) => {
                    let current = residency.unwrap_or_default();
                    if let Some(trimmed) = room.trim_own_caches(&handle, current) {
                        residency = Some(trimmed);
                        continue;
                    }
                    self.make_warm_room_for(admission, limit, &mut room)?;
                }
            }
        }
    }
}
