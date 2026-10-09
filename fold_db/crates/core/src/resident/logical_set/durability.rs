//! Dirty marking and durable-token coverage.

use super::*;

impl LogicalResidentSet {
    /// Applied, and the durable append is not finished. Stores the token
    /// placeholder. Not a use.
    pub fn mark_dirty(
        &mut self,
        molecule: MoleculeId,
        hash: &str,
        range: &str,
        token: DurabilityToken,
    ) -> bool {
        let was_dirty = {
            let Some(entry) = self.tips.get_mut(&tip_key(molecule, hash, range)) else {
                return false;
            };
            let was_dirty = entry.token.is_some();
            entry.token = Some(token);
            was_dirty
        };
        self.note_dirty_transition(was_dirty, true);
        self.publish_occupancy();
        true
    }

    /// The exact durability token is covered (`WriteAck.covered_by_file_len`).
    /// An entry becomes clean only when its owning call has returned. It
    /// then stays warm. `flush`'s `Ok` does not call this.
    pub fn mark_covered(&mut self, token: DurabilityToken) {
        self.covered_exact.insert(token);
        self.cover_ready_entries();
    }

    /// Full-flush seal. Tokens at or below `seal` are covered. A newer token
    /// stays dirty. An entry becomes clean only when the owning call has
    /// returned, and then it stays warm.
    pub fn apply_durable_through(&mut self, seal: DurabilityToken) {
        self.covered_through = Some(match self.covered_through {
            Some(prev) if prev >= seal => prev,
            _ => seal,
        });
        self.cover_ready_entries();
    }

    pub(super) fn token_is_covered(&self, token: Option<DurabilityToken>) -> bool {
        let Some(token) = token else {
            return false;
        };
        self.covered_exact.contains(&token)
            || self.covered_through.is_some_and(|seal| token <= seal)
    }

    pub(super) fn cover_ready_entries(&mut self) {
        let tip_keys: Vec<TipKey> = self.tips.keys().cloned().collect();
        for key in tip_keys {
            let ready = self
                .tips
                .get(&key)
                .is_some_and(|entry| entry.hold == 0 && self.token_is_covered(entry.token));
            if ready {
                let was_dirty = if let Some(entry) = self.tips.get_mut(&key) {
                    let was_dirty = entry.token.is_some();
                    entry.token = None;
                    was_dirty
                } else {
                    false
                };
                self.note_dirty_transition(was_dirty, false);
            }
        }
        let tomb_keys: Vec<TipKey> = self.tombstones.keys().cloned().collect();
        for key in tomb_keys {
            let ready = self
                .tombstones
                .get(&key)
                .is_some_and(|entry| entry.hold == 0 && self.token_is_covered(entry.token));
            if ready {
                let was_dirty = if let Some(entry) = self.tombstones.get_mut(&key) {
                    let was_dirty = entry.token.is_some();
                    entry.token = None;
                    was_dirty
                } else {
                    false
                };
                self.note_dirty_transition(was_dirty, false);
            }
        }
        self.purge_over_cap();
    }
}
