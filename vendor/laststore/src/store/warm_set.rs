use super::*;

#[derive(Default)]
pub(super) struct ShardWarmSet {
    pub(super) handles: HashMap<ShardKey, ShardHandle>,
    /// Groups admitted by a scan or batch read.
    ///
    /// These groups use the part of the full warm byte limit that point-read
    /// groups do not need. When the shared limit is full, a new scan candidate
    /// leaves before an established point-read group.
    pub(super) scan_keys: HashSet<ShardKey>,
    /// Recency order for scan entries only. This avoids a linear search of
    /// the complete point-read set on each scan rejection.
    pub(super) scan_order: BTreeMap<u64, ShardKey>,
    /// Recency order, least recently used first.
    ///
    /// A `VecDeque` here made every touch an O(n) linear scan for the key's
    /// current position. That is invisible while the set holds a few hundred
    /// groups and quadratic once it holds thousands — which is exactly the
    /// state cache trimming restores it to. Keyed by a monotonic tick instead,
    /// so a touch is two map operations and the LRU end is `pop_first`.
    pub(super) order: BTreeMap<u64, ShardKey>,
    pub(super) tick_by_key: HashMap<ShardKey, u64>,
    pub(super) next_tick: u64,
    pub(super) resident_by_key: HashMap<ShardKey, u64>,
    pub(super) resident_bytes: u64,
    /// Highest `resident_bytes` ever published since open.
    ///
    /// A point-in-time stats read cannot see a charge that was published and
    /// evicted again between two samples; this can. It is what proves that no
    /// admission or re-charge published a charge past the budget.
    pub(super) peak_resident_bytes: u64,
    /// Of each group's charge, how much is reproducible read cache.
    ///
    /// Only groups that have some are listed. Without this the trim pass would
    /// have to lock every resident handle to discover there is nothing left to
    /// give up — which is precisely the steady state once indexes alone fill
    /// the budget, so it would run on every single operation.
    pub(super) trimmable_by_key: HashMap<ShardKey, u64>,
    pub(super) trimmable_bytes: u64,
    /// Scan leases whose group did not fit the byte budget.
    ///
    /// A scan rejects its own newest group rather than evict for it, so that
    /// group's handle is never published or charged. It is still the only
    /// handle for the group while a lease holds it: a later load for the same
    /// key adopts it from here instead of reading a second copy from disk, so
    /// the group never has two authorities. Weak, so an entry dies with the
    /// last lease.
    pub(super) leased: HashMap<ShardKey, Weak<Mutex<Shard>>>,
    /// Wall-clock ms of the last interactive point admit. Not `next_tick`.
    pub(super) interactive_touch_unix_ms: HashMap<ShardKey, u64>,
    /// Owner class recorded by a point admit. Absent means unspecified.
    pub(super) owner_class: HashMap<ShardKey, AdmitClass>,
    /// Groups whose keys are the shared `atom\0` plane. The flag sticks.
    pub(super) atom_plane: HashSet<ShardKey>,
}

impl ShardWarmSet {
    /// Would charging `key` keep the index, the body, and the total inside budget?
    ///
    /// Index and total both use `index_budget` (`effective_warm_bytes`). Body
    /// uses `body_budget`, which is `0` while host pressure is high and equal
    /// to the index budget otherwise. A non-growing charge always fits, so a
    /// trim can be recorded while other groups still hold body bytes.
    /// `index_budget == 0` is no budget. Two independent ceilings would let
    /// resident reach about twice the pin.
    pub(super) fn fits(
        &self,
        key: &ShardKey,
        residency: Residency,
        index_budget: u64,
        body_budget: u64,
    ) -> bool {
        if index_budget == 0 {
            return true;
        }
        let prev_total = self.resident_by_key.get(key).copied().unwrap_or(0);
        let prev_body = self.trimmable_by_key.get(key).copied().unwrap_or(0);
        let prev_index = prev_total.saturating_sub(prev_body);
        let index = residency.total.saturating_sub(residency.trimmable);
        let body = residency.trimmable;
        if index.saturating_sub(prev_index) == 0
            && body.saturating_sub(prev_body) == 0
            && residency.total.saturating_sub(prev_total) == 0
        {
            return true;
        }
        let new_index = self
            .resident_bytes
            .saturating_sub(self.trimmable_bytes)
            .saturating_sub(prev_index)
            .saturating_add(index);
        let new_body = self
            .trimmable_bytes
            .saturating_sub(prev_body)
            .saturating_add(body);
        let new_total = self
            .resident_bytes
            .saturating_sub(prev_total)
            .saturating_add(residency.total);
        new_index <= index_budget && new_body <= body_budget && new_total <= index_budget
    }

    pub(super) fn growth(&self, key: &ShardKey, total: u64) -> u64 {
        let previous = self.resident_by_key.get(key).copied().unwrap_or(0);
        total.saturating_sub(previous)
    }

    /// Re-charge `key` only when `handle` is still its resident authority and
    /// the new charge fits `budget`.
    ///
    /// For paths that re-measure a group they did not grow (cache trims). A
    /// charge that no longer fits means a concurrent write grew the group; that
    /// writer re-charges it through the budget gate itself.
    pub(super) fn recharge_if_fits(
        &mut self,
        key: &ShardKey,
        handle: &ShardHandle,
        residency: Residency,
        index_budget: u64,
        body_budget: u64,
    ) {
        let current = self
            .handles
            .get(key)
            .is_some_and(|resident| Arc::ptr_eq(resident, handle));
        if current && self.fits(key, residency, index_budget, body_budget) {
            self.recharge(key, residency);
        }
    }

    /// The live unpublished scan handle for `key`, if a lease still holds one.
    pub(super) fn leased_handle(&mut self, key: &ShardKey) -> Option<ShardHandle> {
        let handle = self.leased.get(key)?.upgrade();
        if handle.is_none() {
            self.leased.remove(key);
        }
        handle
    }

    /// Does any handle own `key` right now, published or leased?
    pub(super) fn holds(&self, key: &ShardKey) -> bool {
        self.handles.contains_key(key)
            || self
                .leased
                .get(key)
                .is_some_and(|handle| handle.strong_count() > 0)
    }

    /// Record `key` as the most recently used group.
    pub(super) fn touch(&mut self, key: &ShardKey) {
        if let Some(previous) = self.tick_by_key.get(key) {
            self.order.remove(previous);
            if self.scan_keys.contains(key) {
                self.scan_order.remove(previous);
            }
        }
        let tick = self.next_tick;
        self.next_tick = self.next_tick.saturating_add(1);
        self.order.insert(tick, key.clone());
        if self.scan_keys.contains(key) {
            self.scan_order.insert(tick, key.clone());
        }
        self.tick_by_key.insert(key.clone(), tick);
    }

    /// Remove the least recently used key from the recency order and return it.
    ///
    /// The handle itself is left in place: the caller decides whether the group
    /// can be evicted, and re-admits it with [`Self::touch`] when it cannot.
    pub(super) fn pop_lru(&mut self) -> Option<ShardKey> {
        let (_, key) = self.order.pop_first()?;
        self.tick_by_key.remove(&key);
        Some(key)
    }

    /// Remove the least-recently-used scan-admitted key from the order.
    pub(super) fn pop_scan_lru(&mut self) -> Option<ShardKey> {
        let (tick, key) = self.scan_order.pop_first()?;
        self.order.remove(&tick);
        self.tick_by_key.remove(&key);
        Some(key)
    }

    pub(super) fn take_from_order(&mut self, key: &ShardKey) -> Option<ShardKey> {
        let tick = self.tick_by_key.remove(key)?;
        self.scan_order.remove(&tick);
        self.order.remove(&tick)
    }

    /// Mark a resident key as scan-admitted without changing its charge.
    pub(super) fn mark_scan(&mut self, key: &ShardKey) {
        if self.scan_keys.insert(key.clone()) {
            if let Some(tick) = self.tick_by_key.get(key).copied() {
                self.scan_order.insert(tick, key.clone());
            }
        }
    }

    /// A point operation promotes a scan entry into the protected segment.
    pub(super) fn promote_point(&mut self, key: &ShardKey) {
        if self.scan_keys.remove(key) {
            if let Some(tick) = self.tick_by_key.get(key) {
                self.scan_order.remove(tick);
            }
        }
    }

    /// Keys that still hold reproducible read cache, least recently used first.
    pub(super) fn trimmable_lru_order(&self) -> Vec<ShardKey> {
        self.order
            .values()
            .filter(|key| self.trimmable_by_key.contains_key(*key))
            .cloned()
            .collect()
    }

    /// Drop every trace of `key` from the warm set, returning the bytes it was
    /// charged.
    pub(super) fn remove(&mut self, key: &ShardKey) -> Option<u64> {
        self.handles.remove(key)?;
        if let Some(tick) = self.tick_by_key.remove(key) {
            self.order.remove(&tick);
            self.scan_order.remove(&tick);
        }
        if let Some(trimmable) = self.trimmable_by_key.remove(key) {
            self.trimmable_bytes = self.trimmable_bytes.saturating_sub(trimmable);
        }
        let charged = self.resident_by_key.remove(key).unwrap_or_default();
        self.resident_bytes = self.resident_bytes.saturating_sub(charged);
        self.scan_keys.remove(key);
        self.interactive_touch_unix_ms.remove(key);
        self.owner_class.remove(key);
        self.atom_plane.remove(key);
        Some(charged)
    }

    /// Record the wall-clock time of an interactive point touch.
    ///
    /// A background touch must not call this. The value is unix milliseconds,
    /// not `next_tick`.
    pub(super) fn note_interactive_touch(&mut self, key: &ShardKey, unix_ms: u64) {
        self.interactive_touch_unix_ms.insert(key.clone(), unix_ms);
    }

    /// Apply an admit's class and atom-plane flag.
    ///
    /// Interactive sticks. Background cannot demote it or clear its stamp.
    /// The atom-plane flag sticks. Scan admission sets the atom flag only:
    /// it does not stamp and it does not record an owner class. Unspecified
    /// leaves the class unchanged so a raw laststore group stays evictable.
    pub(super) fn note_admission(
        &mut self,
        key: &ShardKey,
        admission: WarmAdmission,
        touch: WarmTouch,
    ) {
        if touch.atom {
            self.atom_plane.insert(key.clone());
        }
        if admission != WarmAdmission::Point {
            return;
        }
        match touch.class {
            AdmitClass::Interactive => {
                self.owner_class
                    .insert(key.clone(), AdmitClass::Interactive);
                self.note_interactive_touch(key, wall_unix_ms());
            }
            AdmitClass::Background => {
                if self.atom_plane.contains(key)
                    || self.owner_class.get(key).copied() == Some(AdmitClass::Interactive)
                {
                    return;
                }
                self.owner_class.insert(key.clone(), AdmitClass::Background);
            }
            AdmitClass::Unspecified => {}
        }
    }

    /// Atom-plane groups never leave. A fresh interactive molecule-tip index
    /// leaves only while pressure is high and the shed flag is on.
    pub(super) fn eviction_protected(
        &self,
        key: &ShardKey,
        pressure_high: bool,
        shed_interactive: bool,
        now_ms: u64,
    ) -> bool {
        if self.atom_plane.contains(key) {
            return true;
        }
        if self.owner_class.get(key).copied() != Some(AdmitClass::Interactive) {
            return false;
        }
        if !pressure_high || !shed_interactive {
            return true;
        }
        !matches!(
            self.interactive_touch_unix_ms.get(key).copied(),
            Some(stamp) if now_ms.saturating_sub(stamp) > INTERACTIVE_PROTECT_MS
        )
    }

    /// Re-charge `key` with a freshly measured residency estimate.
    pub(super) fn recharge(&mut self, key: &ShardKey, residency: Residency) {
        if !self.handles.contains_key(key) {
            return;
        }
        let previous = self
            .resident_by_key
            .insert(key.clone(), residency.total)
            .unwrap_or_default();
        self.resident_bytes = self
            .resident_bytes
            .saturating_sub(previous)
            .saturating_add(residency.total);
        self.peak_resident_bytes = self.peak_resident_bytes.max(self.resident_bytes);

        let previous_trimmable = if residency.trimmable == 0 {
            self.trimmable_by_key.remove(key)
        } else {
            self.trimmable_by_key
                .insert(key.clone(), residency.trimmable)
        }
        .unwrap_or_default();
        self.trimmable_bytes = self
            .trimmable_bytes
            .saturating_sub(previous_trimmable)
            .saturating_add(residency.trimmable);
    }
}
