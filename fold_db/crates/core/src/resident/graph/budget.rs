//! Byte-budget charging and clean-entry eviction for the resident graph.

use super::*;

impl ResidentGraph {
    /// Charge `key` at `bytes`, then evict clean LRU entries if over budget.
    pub(super) fn charge_and_enforce(&self, key: DirtyKey, bytes: u64) {
        self.ledger
            .lock()
            .expect("resident ledger lock")
            .charge(key, bytes);
        self.enforce_budget();
    }

    /// Mark `key` recently used (resolve hits).
    pub(super) fn note_touch(&self, key: &DirtyKey) {
        self.ledger
            .lock()
            .expect("resident ledger lock")
            .touch_existing(key);
    }

    /// Drop `key`'s charge (entry left the graph).
    pub(super) fn discharge(&self, key: &DirtyKey) {
        self.ledger
            .lock()
            .expect("resident ledger lock")
            .discharge(key);
    }

    /// Evict clean entries LRU-first until under budget.
    ///
    /// Dirty entries are skipped, never evicted (the persist worker is the
    /// only thing that turns dirty into clean). When everything over budget
    /// is dirty, the pass records `evict_refused_dirty` and returns — the
    /// graph temporarily exceeds its budget rather than dropping unpersisted
    /// writes.
    pub(super) fn enforce_budget(&self) {
        if self.budget_bytes == 0 {
            return;
        }
        // Snapshot candidates under the ledger lock, then evict without it —
        // eviction takes map + dirty locks and lock order must stay flat.
        let candidates: Vec<DirtyKey> = {
            let dirty = self.dirty.read().expect("resident dirty lock");
            let ledger = self.ledger.lock().expect("resident ledger lock");
            if ledger.total_bytes() <= self.budget_bytes {
                return;
            }
            ledger.eviction_candidates(ledger.total_bytes() - self.budget_bytes, |key| {
                // The catalog is the root of every local data lookup. Keep it
                // resident even when clean molecule/atom entries must leave.
                !matches!(
                    key,
                    DirtyKey::Schema(_)
                        | DirtyKey::MoleculeKeyTombstone { .. }
                        | DirtyKey::Protein(_)
                ) && !dirty.contains(key)
            })
        };
        let mut refused_dirty = self.dirty_count() > 0;
        for key in candidates {
            if self.resident_bytes() <= self.budget_bytes {
                return;
            }
            if self.is_dirty(&key) {
                refused_dirty = true;
                continue;
            }
            let removed = self.evict_clean_key(&key);
            if removed {
                self.metrics.record_evicted(key.kind());
            }
        }
        if refused_dirty && self.resident_bytes() > self.budget_bytes {
            self.metrics.record_evict_refused_dirty();
        }
    }

    /// Check the dirty pin while holding the value map lock. An apply pins
    /// before it installs, so a selected clean victim cannot turn into an
    /// acknowledged dirty value between the check and removal.
    pub(super) fn evict_clean_key(&self, key: &DirtyKey) -> bool {
        match key {
            DirtyKey::QueryPage(id) => self.evict_page_read(*id),
            DirtyKey::Atom(uuid) => self.evict_clean_map_entry(&self.atoms, uuid, key),
            DirtyKey::MoleculeTip {
                molecule_uuid,
                hash,
                range,
            } => self
                .try_evict_tip(molecule_uuid, hash, range)
                .unwrap_or(false),
            DirtyKey::MoleculeKeyIndex {
                molecule_uuid,
                hash,
                range,
            } => {
                let mut index = self.key_index.write().expect("key index lock");
                let dirty = self.dirty.read().expect("resident dirty lock");
                if dirty.contains(key) {
                    return false;
                }
                self.invalidate_partition(molecule_uuid, hash);
                let member = ResidentMoleculeKey::new(hash, range);
                let removed = index
                    .get_mut(molecule_uuid)
                    .is_some_and(|keys| keys.remove(&member));
                if index.get(molecule_uuid).is_some_and(BTreeSet::is_empty) {
                    index.remove(molecule_uuid);
                }
                self.discharge(key);
                self.invalidate_partition(molecule_uuid, hash);
                drop(dirty);
                drop(index);
                if removed {
                    self.demote_complete_key_set(molecule_uuid);
                }
                removed
            }
            // Catalog entries remain resident. Delete overlays remain pinned
            // until their exact durable completion removes them.
            _ => false,
        }
    }

    pub(super) fn evict_clean_map_entry<T>(
        &self,
        values: &RwLock<HashMap<String, T>>,
        id: &str,
        key: &DirtyKey,
    ) -> bool {
        let mut values = values.write().expect("resident value lock");
        let dirty = self.dirty.read().expect("resident dirty lock");
        if dirty.contains(key) {
            return false;
        }
        if let DirtyKey::MoleculeTip {
            molecule_uuid,
            hash,
            ..
        } = key
        {
            self.invalidate_partition(molecule_uuid, hash);
        }
        let removed = values.remove(id).is_some();
        if let DirtyKey::MoleculeTip {
            molecule_uuid,
            hash,
            ..
        } = key
        {
            self.invalidate_partition(molecule_uuid, hash);
        }
        self.discharge(key);
        removed
    }
}
