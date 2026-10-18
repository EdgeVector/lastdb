//! Exact invalidation edges contain no tip or atom value copies.

use super::*;

const MAX_SLOT_SOURCES: usize = 64;

impl CoreWarmSet {
    /// Recover only existing logical bindings for changed physical sources.
    pub(crate) fn paths_for_sources(&self, changes: &[(String, String)]) -> Vec<CorePath> {
        changes
            .iter()
            .flat_map(|(collection, key)| {
                self.source_slots
                    .get(&SourceKey {
                        collection: collection.clone(),
                        key: key.clone(),
                    })
                    .into_iter()
                    .flatten()
                    .flat_map(|slot| self.slot_owners.get(slot).into_iter().flatten())
            })
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Replace source edges after the normal resolver confirms a retained value.
    pub(crate) fn clear_sources(&mut self, path: &CorePath) {
        self.forget_slot_sources(&path.slot());
    }

    /// Check the complete replacement before removing any certified source edge.
    /// Refusal leaves existing source links and retained values unchanged.
    pub(crate) fn replace_sources(
        &mut self,
        path: &CorePath,
        sources: &[(String, String)],
    ) -> bool {
        if !self.paths.contains_key(path) {
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
        self.slot_sources
            .get(&path.slot())
            .into_iter()
            .flatten()
            .map(|source| (source.collection.clone(), source.key.clone()))
            .collect()
    }

    /// Source edges belong to the shared slot and leave with its last path.
    /// Removing one schema alias cannot remove another alias's protection.
    pub(crate) fn link_source(&mut self, path: &CorePath, collection: &str, key: &str) -> bool {
        if !self.paths.contains_key(path) {
            return false;
        }
        let slot = path.slot();
        let source = SourceKey {
            collection: collection.to_string(),
            key: key.to_string(),
        };
        let sources = self.slot_sources.entry(slot.clone()).or_default();
        if sources.contains(&source) {
            return true;
        }
        if sources.len() >= MAX_SLOT_SOURCES {
            return false;
        }
        sources.insert(source.clone());
        self.source_slots.entry(source).or_default().insert(slot);
        true
    }

    /// Call under the same publication boundary as the changed disk/write view.
    /// Every alias of the affected core slot loses its old current value.
    pub(crate) fn invalidate_sources(&mut self, changes: &[(String, String)]) {
        self.memory.invalidate(changes);
        let slots: HashSet<_> = changes
            .iter()
            .flat_map(|(collection, key)| {
                self.source_slots
                    .get(&SourceKey {
                        collection: collection.clone(),
                        key: key.clone(),
                    })
                    .into_iter()
                    .flatten()
                    .cloned()
            })
            .collect();
        for slot in slots {
            self.invalidate_slot_key(&slot);
        }
    }

    /// A direct typed writer can invalidate without a physical source spelling.
    pub(crate) fn invalidate_slot(&mut self, path: &CorePath) {
        self.invalidate_slot_key(&path.slot());
    }

    /// Conservative guard for an untyped write whose logical slot is unknown.
    /// The caller publishes that write and this invalidation under one boundary.
    pub(crate) fn invalidate_molecule(&mut self, scope: Option<&str>, molecule: &str) {
        let slots: Vec<_> = self
            .slot_owners
            .keys()
            .filter(|slot| {
                slot.molecule.scope.as_deref() == scope && slot.molecule.molecule == molecule
            })
            .cloned()
            .collect();
        for slot in slots {
            self.invalidate_slot_key(&slot);
        }
    }

    pub(super) fn invalidate_slot_key(&mut self, slot: &SlotKey) {
        let owners: Vec<_> = self
            .slot_owners
            .get(slot)
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        self.forget_slot_sources(slot);
        self.forget_slot_value(slot);
        for path in owners {
            let forget = if let Some(state) = self.paths.get_mut(&path) {
                state.known = false;
                state.absent = false;
                state.tip_authorized = false;
                state.holds == 0 && state.dirty.is_none()
            } else {
                false
            };
            if forget {
                self.drop_path(&path);
            }
        }
    }

    pub(super) fn forget_slot_sources(&mut self, slot: &SlotKey) {
        let Some(sources) = self.slot_sources.remove(slot) else {
            return;
        };
        for source in sources {
            let empty = self.source_slots.get_mut(&source).is_some_and(|slots| {
                slots.remove(slot);
                slots.is_empty()
            });
            if empty {
                self.source_slots.remove(&source);
            }
        }
    }
}
