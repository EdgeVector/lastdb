//! Exact invalidation edges contain no tip or atom value copies.

use super::charge::link_charge;
use super::*;

const MAX_SLOT_SOURCES: usize = 64;

impl CoreWarmSet {
    /// Recover only existing logical bindings for changed physical sources.
    pub(crate) fn paths_for_sources(&self, changes: &[(String, String)]) -> Vec<CorePath> {
        changes
            .iter()
            .filter_map(|(collection, key)| self.sources.find(collection, key))
            .filter_map(|source| self.sources.get(source))
            .flat_map(|source| source.slots.iter())
            .filter_map(|slot| self.slots.get(slot))
            .flat_map(|slot| slot.owners.iter())
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|path| self.paths.core_path(&self.slots, *path))
            .collect()
    }

    /// Replace source edges after the normal resolver confirms a retained value.
    pub(crate) fn clear_sources(&mut self, path: &CorePath) {
        if let Some(id) = self.slot_of(path) {
            self.forget_slot_sources(id);
        }
    }

    /// Check the complete replacement before removing any certified source edge.
    /// Refusal leaves existing source links and retained values unchanged.
    pub(crate) fn replace_sources(
        &mut self,
        path: &CorePath,
        sources: &[(String, String)],
    ) -> bool {
        if self.path_id(path).is_none() {
            return false;
        }
        let mut distinct = HashSet::new();
        for source in sources {
            distinct.insert(source);
            if distinct.len() > MAX_SLOT_SOURCES {
                return false;
            }
        }
        self.clear_sources(path);
        for (collection, key) in distinct {
            let linked = self.link_source(path, collection, key);
            debug_assert!(linked, "validated replacement source bound");
        }
        true
    }

    /// Bounded source coordinates contain no additional tip or atom values.
    pub(crate) fn source_coordinates(&self, path: &CorePath) -> Vec<(String, String)> {
        self.slot_of(path)
            .and_then(|id| self.slots.get(id))
            .into_iter()
            .flat_map(|slot| slot.sources.iter())
            .map(|source| self.sources.coordinates(*source))
            .collect()
    }

    /// Source edges belong to the shared slot and leave with its last path.
    /// Removing one schema alias cannot remove another alias's protection.
    pub(crate) fn link_source(&mut self, path: &CorePath, collection: &str, key: &str) -> bool {
        let Some(id) = self.path_state(path).map(|state| state.slot) else {
            return false;
        };
        let slot = self.slots.get(id).expect("warm core slot");
        if let Some(source) = self.sources.find(collection, key) {
            if slot.sources.contains(&source) {
                return true;
            }
        }
        if slot.sources.len() >= MAX_SLOT_SOURCES {
            return false;
        }
        let (source, created) = self.sources.intern(collection, key, id);
        if created {
            self.charge_source(self.sources.charged_len(source));
        } else {
            self.sources.link(source, id);
        }
        self.slots
            .get_mut(id)
            .expect("warm core slot")
            .sources
            .push(source);
        self.charge_link(link_charge());
        true
    }

    /// Call under the same publication boundary as the changed disk/write view.
    /// Every alias of the affected core slot loses its old current value.
    pub(crate) fn invalidate_sources(&mut self, changes: &[(String, String)]) {
        self.memory.invalidate(changes);
        let slots: HashSet<_> = changes
            .iter()
            .filter_map(|(collection, key)| self.sources.find(collection, key))
            .filter_map(|source| self.sources.get(source))
            .flat_map(|source| source.slots.iter())
            .collect();
        for slot in slots {
            self.invalidate_slot_key(slot);
        }
    }

    /// A direct typed writer can invalidate without a physical source spelling.
    pub(crate) fn invalidate_slot(&mut self, path: &CorePath) {
        if let Some(id) = self.slot_of(path) {
            self.invalidate_slot_key(id);
        }
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
        self.forget_slot_sources(id);
        self.forget_slot_value(id);
        for path in owners {
            let forget = if let Some(state) = self.paths.state_mut(path) {
                state.known = false;
                state.absent = false;
                state.tip_authorized = false;
                state.holds == 0 && state.dirty.is_none()
            } else {
                false
            };
            if forget {
                self.drop_id(path);
            }
        }
    }

    pub(super) fn forget_slot_sources(&mut self, id: tables::SlotId) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        let sources = std::mem::take(&mut slot.sources);
        self.release_links(sources.len() * link_charge());
        for source in sources {
            if let Some(packed_len) = self.sources.unlink(source, id) {
                self.release_source(packed_len);
            }
        }
    }
}
