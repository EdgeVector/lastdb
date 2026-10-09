//! Tombstone admission and delete/undo for the logical resident set.

use super::*;

impl LogicalResidentSet {
    /// Admit a dirty delete. The tombstone counts toward the key budget.
    /// It stays while the call holds it or the flush has not covered its
    /// token. After that, purge may remove it when it is least recently used.
    pub fn admit_tombstone(
        &mut self,
        molecule: MoleculeId,
        hash: String,
        range: String,
        token: DurabilityToken,
    ) {
        self.hide_range_from_order(molecule, &hash, &range);
        let key = TipKey {
            molecule,
            hash,
            range,
        };
        let resident = Self::tombstone_resident_key(&key);
        if self.tombstones.contains_key(&key) {
            let (before_hold, after_hold, before_dirty) = {
                let entry = self.tombstones.get_mut(&key).expect("tombstone");
                let before_hold = entry.hold;
                let before_dirty = entry.token.is_some();
                entry.take_hold();
                entry.token = Some(token);
                (before_hold, entry.hold, before_dirty)
            };
            self.note_hold_transition(before_hold, after_hold);
            self.note_dirty_transition(before_dirty, true);
            self.touch(resident);
            self.publish_occupancy();
            return;
        }
        self.tombstones.insert(key, Held::admit_dirty((), token));
        self.note_used_insert(1, true);
        self.touch(resident);
        self.publish_occupancy();
    }

    /// Delete one key. Inserts a tombstone even when no tip was resident.
    /// Removes a resident tip without demoting a `Complete` hash view.
    pub fn delete_resident(
        &mut self,
        molecule: MoleculeId,
        hash: String,
        range: String,
        token: DurabilityToken,
    ) -> Option<Tip> {
        let key = TipKey {
            molecule,
            hash: hash.clone(),
            range: range.clone(),
        };
        let previous = self.take_tip_for_delete(&key);
        self.admit_tombstone(molecule, hash, range, token);
        self.publish_occupancy();
        previous
    }

    /// Failed-transaction undo. Removes the tombstone and restores the held
    /// tip, its durability token, and the atom body `take_tip_for_delete`
    /// removed. This does not admit a clean tip in place of a dirty one.
    pub fn undo_delete(
        &mut self,
        molecule: MoleculeId,
        hash: &str,
        range: &str,
        previous: Option<Tip>,
    ) {
        let key = tip_key(molecule, hash, range);
        self.drop_tombstone_record(&key);
        let snapshot = self.undo_snapshots.remove(&key);
        if let Some(tip) = previous {
            match snapshot {
                Some(snapshot) => self.restore_deleted_tip(key, tip, snapshot),
                None => self.insert_new_tip(key, tip),
            }
        }
    }

    /// A newer put of the key supersedes its delete overlay.
    pub fn supersede_tombstone(&mut self, molecule: MoleculeId, hash: &str, range: &str) {
        self.clear_tombstone(&tip_key(molecule, hash, range));
    }

    pub fn has_tombstone(&self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        self.tombstones
            .contains_key(&tip_key(molecule, hash, range))
    }

    pub fn tombstone_hold_count(&self, molecule: MoleculeId, hash: &str, range: &str) -> u32 {
        self.tombstones
            .get(&tip_key(molecule, hash, range))
            .map_or(0, |entry| entry.hold)
    }

    pub fn tombstone_token(
        &self,
        molecule: MoleculeId,
        hash: &str,
        range: &str,
    ) -> Option<DurabilityToken> {
        self.tombstones
            .get(&tip_key(molecule, hash, range))
            .and_then(|entry| entry.token)
    }
}
