use super::*;

impl LastStore {
    /// Cold shard loads since open (see [`Self::shard_loads`] field docs).
    ///
    /// A walk should cost about one load per shard it touches. Loads growing
    /// with the number of rows returned means the warm set is thrashing and
    /// reads are re-parsing groups per row.
    pub fn shard_loads(&self) -> u64 {
        self.shard_loads.load(Ordering::Relaxed)
    }

    /// Cold loads currently parsing a group that is not yet published.
    pub fn in_flight_cold_loads(&self) -> u64 {
        self.in_flight_cold_loads.load(Ordering::Relaxed)
    }

    /// Estimated on-disk bytes of those in-flight loads.
    pub fn in_flight_cold_bytes(&self) -> u64 {
        self.in_flight_cold_bytes.load(Ordering::Relaxed)
    }

    /// Groups removed from the warm set since open.
    pub fn eviction_events(&self) -> u64 {
        self.eviction_events.load(Ordering::Relaxed)
    }

    /// How the id tiers answered keys-only group resolutions since open.
    ///
    /// Four relaxed atomic loads and no lock, so this is safe on the
    /// per-request path — the same split [`Self::shard_loads`] keeps against
    /// the mutex-taking [`Self::hash_group_warm_stats`]. Store-wide and
    /// monotonic: read a delta across a request as a load average, not as a
    /// forensic claim about that one request.
    pub fn id_tier_stats(&self) -> IdTierStats {
        IdTierStats {
            resident: self.id_tier_resident.load(Ordering::Relaxed),
            key_cache_hits: self.id_tier_key_cache_hits.load(Ordering::Relaxed),
            sidecar_hits: self.id_tier_sidecar_hits.load(Ordering::Relaxed),
            live_scans: self.id_tier_live_scans.load(Ordering::Relaxed),
        }
    }

    /// Transactions where an applied operation was caught and rolled back.
    ///
    /// The pre-transaction value is captured from the group that `put` /
    /// `delete` already opened, so a successful transaction does not pay an
    /// extra cold load. A nonzero count means the store absorbed an apply
    /// failure (descriptor cap, encode reject, append error) instead of leaving
    /// a mixed-generation row.
    pub fn torn_transaction_rollbacks(&self) -> u64 {
        self.torn_transaction_rollbacks.load(Ordering::Relaxed)
    }

    /// Transactions whose restore or rollback flush failed.
    ///
    /// This, not [`Self::torn_transaction_rollbacks`], is the number that means
    /// a caller may still observe a torn row.
    pub fn torn_transaction_rollback_failures(&self) -> u64 {
        self.torn_transaction_rollback_failures
            .load(Ordering::Relaxed)
    }

    /// Committed transactions whose post-commit cache refresh failed.
    pub fn transaction_residency_refresh_failures(&self) -> u64 {
        self.transaction_residency_refresh_failures
            .load(Ordering::Relaxed)
    }

    /// Effective warm-set byte budget currently enforced by LRU eviction.
    pub fn effective_warm_bytes(&self) -> u64 {
        self.effective_warm_bytes.load(Ordering::Relaxed)
    }

    /// Shrink or grow the effective warm-set budget.
    ///
    /// `0` disables ordinary admit-path eviction, matching
    /// [`LastStoreOptions::hash_group_warm_bytes`]. Callers that need a
    /// measured-footprint trim should follow this with
    /// [`Self::evict_hash_group_warm_set_to_bytes`].
    pub fn set_effective_warm_bytes(&self, bytes: u64) {
        self.effective_warm_bytes.store(bytes, Ordering::Release);
    }

    /// Block the over-budget point publish while a footprint drain holds.
    ///
    /// A point group that still does not fit is normally published anyway,
    /// because it is the only handle a write can use. During a drain that
    /// publish raises `resident_bytes` above the post-step budget and the
    /// next admit refills the set. The handle stays on the leased map,
    /// uncharged, the same way an exhausted scan does.
    pub fn set_warm_drain_hold(&self, hold: bool) {
        self.warm_drain_hold.store(hold, Ordering::Release);
    }

    pub(super) fn warm_drain_hold(&self) -> bool {
        self.warm_drain_hold.load(Ordering::Acquire)
    }

    /// Body budget is 0 for the whole time the host is in pressure.
    ///
    /// This is not the drain-hold flag. Drain hold only closes the
    /// over-budget point publish.
    pub fn set_host_pressure_high(&self, high: bool) {
        self.host_pressure_high.store(high, Ordering::Release);
    }

    /// Whether this store was told that host pressure is high.
    #[must_use]
    pub fn host_pressure_is_high(&self) -> bool {
        self.host_pressure_high()
    }

    pub(super) fn host_pressure_high(&self) -> bool {
        self.host_pressure_high.load(Ordering::Acquire)
    }

    /// Index budget, then body budget.
    ///
    /// Index budget is [`Self::effective_warm_bytes`]. It is never
    /// [`LastStoreOptions::hash_group_warm_bytes`]. Body budget is 0 while
    /// pressure is high, otherwise the same value. A non-hash-group home
    /// has no budget (`0`).
    pub(super) fn index_and_body_budget(&self) -> (u64, u64) {
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return (0, 0);
        }
        let index = self.effective_warm_bytes();
        let body = if self.host_pressure_high() { 0 } else { index };
        (index, body)
    }

    pub(super) fn note_index_over_budget_publish(&self) {
        let previous = self
            .index_over_budget_publishes
            .fetch_add(1, Ordering::Relaxed);
        if previous == 0 {
            eprintln!(
                "LASTSTORE_INDEX_OVER_BUDGET_PUBLISH count=1 action=published_without_raising_ceiling"
            );
        }
    }

    /// Times the point exception published an index that did not fit.
    pub fn index_over_budget_publishes(&self) -> u64 {
        self.index_over_budget_publishes.load(Ordering::Relaxed)
    }

    /// Wall-clock stamp of the last interactive point admit of `id`, if any.
    pub fn interactive_touch_unix_ms(&self, collection: &str, id: &str) -> Option<u64> {
        let key = self.point_key(collection, id);
        self.shards
            .lock()
            .expect("poison")
            .interactive_touch_unix_ms
            .get(&key)
            .copied()
    }

    /// Set the interactive wall-clock stamp. Does not change LRU `next_tick`.
    ///
    /// Tests use this to age a molecule-tip index past 10 minutes. A background
    /// admit must not call it.
    pub fn note_interactive_touch(&self, collection: &str, id: &str, unix_ms: u64) {
        let key = self.point_key(collection, id);
        self.shards
            .lock()
            .expect("poison")
            .note_interactive_touch(&key, unix_ms);
    }

    /// Whether a point admit recorded `id`'s group as background.
    ///
    /// False when the group is interactive, unspecified, atom-plane, or not
    /// resident. Atom-plane groups are not recorded as background.
    pub fn warm_group_class_is_background(&self, collection: &str, id: &str) -> bool {
        let key = self.point_key(collection, id);
        let warm = self.shards.lock().expect("poison");
        warm.owner_class.get(&key).copied() == Some(AdmitClass::Background)
    }
}
