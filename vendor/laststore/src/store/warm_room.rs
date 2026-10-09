use super::*;

impl LastStore {
    /// Bring resident bytes to `limit` or below: read caches first, then
    /// unpinned groups in LRU order (scan entries before point entries).
    ///
    /// Returns whether the limit was reached. The caller must not hold the
    /// warm-set lock or any handle lock.
    pub(super) fn make_warm_room(&self, limit: u64) -> Result<bool> {
        if self.host_pressure_high() {
            // Body budget is 0. Drop reproducible bytes before any index.
            self.trim_all_hash_group_bodies()?;
        } else {
            self.trim_hash_group_read_caches(limit)?;
        }
        self.evict_hash_group_warm_set_inner(false, None, Some(limit))?;
        Ok(self.shards.lock().expect("poison").resident_bytes <= limit)
    }

    /// Publish `residency` as `key`'s new charge only once it fits the budget.
    ///
    /// The same gate as [`Self::publish_warm_handle`], for a group that is
    /// already resident and has grown. A point group that still does not fit
    /// is charged anyway: refusing would leave bytes that exist uncounted. A
    /// scan group that does not fit leaves the warm set and finishes its lease
    /// unpublished, like a scan group that did not fit at admission.
    ///
    /// No-op when `handle` is no longer the resident authority for `key`, or,
    /// for a scan, when a point operation promoted the group meanwhile.
    // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
    pub(super) fn recharge_within_budget(
        &self,
        key: &ShardKey,
        handle: &ShardHandle,
        mut residency: Residency,
        admission: WarmAdmission,
    ) -> Result<()> {
        let mut room = WarmRoom::default();
        loop {
            enum Step {
                MakeRoom(Option<u64>),
                /// This Arc is no longer the resident handle. Spill before it drops.
                Spill,
                /// Leave the warm set only after the open buffer is durable.
                Unpublish,
            }
            let step = {
                let mut warm = self.shards.lock().expect("poison");
                let current = warm
                    .handles
                    .get(key)
                    .is_some_and(|resident| Arc::ptr_eq(resident, handle));
                let scan_entry = warm.scan_keys.contains(key);
                if !current {
                    Step::Spill
                } else if admission == WarmAdmission::Scan && !scan_entry {
                    return Ok(());
                } else {
                    let (index_budget, body_budget) = self.index_and_body_budget();
                    if warm.fits(key, residency, index_budget, body_budget) {
                        warm.recharge(key, residency);
                        return Ok(());
                    }
                    if room.exhausted {
                        let background_tip = warm.owner_class.get(key).copied()
                            == Some(AdmitClass::Background)
                            && !warm.atom_plane.contains(key);
                        if admission == WarmAdmission::Scan {
                            // Only scan leases hold a scan-admitted group: a point
                            // operation promotes the group when it takes the
                            // handle. So no writer can be using this handle.
                            warm.remove(key);
                            warm.leased.insert(key.clone(), Arc::downgrade(handle));
                            return Ok(());
                        } else if self.warm_drain_hold() {
                            // A grown point charge must not raise the resident sum
                            // while the drain holds. The previous charge stays.
                            return Ok(());
                        } else if background_tip {
                            Step::Unpublish
                        } else {
                            self.note_index_over_budget_publish();
                            warm.recharge(key, residency);
                            return Ok(());
                        }
                    } else {
                        Step::MakeRoom(index_budget.checked_sub(warm.growth(key, residency.total)))
                    }
                }
            };
            match step {
                Step::Spill => {
                    let mut shard = handle.lock().expect("poison");
                    return Self::sync_open(&mut shard);
                }
                Step::Unpublish => {
                    // Hold the shard lock across the remove so an append cannot
                    // land after the spill. An admit can mark this group
                    // interactive or atom during the spill. Remove would clear
                    // that stamp, so re-check before leaving the warm set.
                    let mut shard = handle.lock().expect("poison");
                    Self::sync_open(&mut shard)?;
                    let residency_now = estimate_shard_residency_locked(&shard);
                    let mut warm = self.shards.lock().expect("poison");
                    let still = warm
                        .handles
                        .get(key)
                        .is_some_and(|resident| Arc::ptr_eq(resident, handle));
                    if still {
                        let background_tip = warm.owner_class.get(key).copied()
                            == Some(AdmitClass::Background)
                            && !warm.atom_plane.contains(key);
                        let (index_budget, body_budget) = self.index_and_body_budget();
                        if background_tip
                            && !warm.fits(key, residency_now, index_budget, body_budget)
                        {
                            warm.remove(key);
                            warm.leased.insert(key.clone(), Arc::downgrade(handle));
                        } else if warm.fits(key, residency_now, index_budget, body_budget) {
                            warm.recharge(key, residency_now);
                        } else {
                            self.note_index_over_budget_publish();
                            warm.recharge(key, residency_now);
                        }
                    }
                    return Ok(());
                }
                Step::MakeRoom(limit) => {
                    if let Some(trimmed) = room.trim_own_caches(handle, residency) {
                        residency = trimmed;
                        continue;
                    }
                    self.make_warm_room_for(admission, limit, &mut room)?;
                }
            }
        }
    }

    /// One room-making step of the budget gate, after the charged group's own
    /// read caches are gone.
    ///
    /// A point charge evicts other groups down to `limit`. A scan charge only
    /// trims other groups' read caches: a scan never displaces a resident
    /// group, so if that is not enough the scan rejects its own group. A charge
    /// larger than the whole budget (`limit` is `None`) cannot fit, so nothing
    /// is evicted for it.
    pub(super) fn make_warm_room_for(
        &self,
        admission: WarmAdmission,
        limit: Option<u64>,
        room: &mut WarmRoom,
    ) -> Result<()> {
        let Some(limit) = limit else {
            room.exhausted = true;
            return Ok(());
        };
        match admission {
            WarmAdmission::Point => room.record(self.make_warm_room(limit)?),
            WarmAdmission::Scan => {
                if self.host_pressure_high() {
                    self.trim_all_hash_group_bodies()?;
                } else {
                    self.trim_hash_group_read_caches(limit)?;
                }
                room.exhausted = true;
            }
        }
        Ok(())
    }

    /// Release reproducible bytes from a scan handle, then enforce its
    /// admission segment. A point operation can promote the handle while the
    /// scan uses it; that promotion wins and this path leaves it protected.
    pub(super) fn refresh_scan_handle(&self, key: &ShardKey, handle: &ShardHandle) -> Result<()> {
        if key.2.is_none() {
            return Ok(());
        }
        let scan_admitted = self.shards.lock().expect("poison").scan_keys.contains(key);
        if !scan_admitted {
            return Ok(());
        }

        let residency = {
            let mut shard = handle.lock().expect("poison");
            trim_shard_read_caches(&mut shard);
            estimate_shard_residency_locked(&shard)
        };
        self.recharge_within_budget(key, handle, residency, WarmAdmission::Scan)
    }

    pub(super) fn finish_scan_admission(&self, key: &ShardKey) -> Result<()> {
        self.evict_hash_group_warm_set_inner(true, Some(key), None)?;
        // A point handle can remain above the limit while its caller pins it.
        // Once this scan releases its own handle, enforce the shared limit too.
        // Scan entries leave first, so a scan still cannot displace a point
        // index while the point segment fits by itself.
        self.evict_hash_group_warm_set_inner(false, Some(key), None)
            .map(|_| ())
    }

    pub(super) fn refresh_warm_resident_bytes(
        &self,
        key: &ShardKey,
        handle: &ShardHandle,
    ) -> Result<()> {
        // Segment-log handles are the legacy correctness index, not members of
        // the bounded hash-group warm set. Re-estimating their full index on
        // every point operation turns a sequential scan into O(n^2) work and
        // also pollutes hash-group-only residency metrics.
        if key.2.is_none() {
            return Ok(());
        }
        let resident = {
            let warm = self.shards.lock().expect("poison");
            warm.handles
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, handle))
        };
        if !resident {
            if self
                .pin_table_handle(key)
                .is_some_and(|pin| Arc::ptr_eq(&pin, handle))
            {
                // A pin stays until flush or group-commit. Do not spill it
                // from a point get, and do not charge the warm set.
                return Ok(());
            }
            // Publish left this point handle leased, so the caller's Arc is
            // the last owner. The new bytes are still in `open_buf`. Spill
            // them before that Arc drops, including a background admit with
            // the drain hold clear. That admit is the full-budget path, and
            // a tip stays under the group-commit threshold. A later get
            // reloads the segment. Charging the group would raise the
            // resident sum.
            let mut sh = handle.lock().expect("poison");
            Self::sync_open(&mut sh)?;
            return Ok(());
        }
        let residency = estimate_shard_residency(handle);
        // The gate gives up this group's own slack (the just-grown open buffer,
        // cached bodies) and then other groups *before* it publishes the new
        // charge. Recharge then evict published the overshoot first, so a
        // self-metrics sample in that window reported resident > budget.
        self.recharge_within_budget(key, handle, residency, WarmAdmission::Point)?;
        // Still reclaims append descriptors, and catches a charge that had to
        // be published over budget because everything else was pinned.
        self.evict_hash_group_warm_set()
    }
}
