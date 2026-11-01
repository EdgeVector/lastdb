//! Exact invalidation edges contain no tip or atom value copies.

use super::*;

impl CoreWarmSet {
    /// Call under the same publication boundary as the changed disk/write view.
    /// Every alias of the affected core slot loses its old current value, and
    /// a changed record key loses its decoded record.
    pub(crate) fn invalidate_sources(&mut self, changes: &[(String, String)]) {
        self.memory.invalidate(changes);
        self.invalidate_records(changes);
    }

    /// Conservative guard for an untyped write whose logical slot is unknown.
    /// The caller publishes that write and this invalidation under one boundary.
    pub(crate) fn invalidate_molecule(&mut self, scope: Option<&str>, molecule: &str) {
        let slots: Vec<_> = self
            .slots
            .iter()
            .filter(|(_, slot)| {
                slot.key.molecule.scope.as_deref() == scope
                    && slot.key.molecule.molecule.as_ref() == molecule
            })
            .map(|(id, _)| id)
            .collect();
        for slot in slots {
            self.invalidate_slot_key(slot);
        }
    }

    /// The id may already have left with an alias's last owner; that is a no-op.
    pub(super) fn invalidate_slot_key(&mut self, id: tables::SlotId) {
        let Some(slot) = self.slots.get(id) else {
            return;
        };
        let owners = slot.owners.clone();
        self.forget_slot_value(id);
        for path in owners {
            let forget = if let Some(state) = self.paths.state_mut(path) {
                state.known = false;
                state.absent = false;
                state.tip_authorized = false;
                state.holds == 0
            } else {
                false
            };
            if forget {
                self.drop_id(path);
            }
        }
    }
}
