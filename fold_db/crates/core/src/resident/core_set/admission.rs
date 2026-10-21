//! Admission changes the actual core objects, never a parallel tip value map.

use super::*;

impl CoreWarmSet {
    /// Reserve one hold per distinct path, or leave the set unchanged on refusal.
    /// The returned paths must be released once after the caller finishes.
    pub(crate) fn reserve_paths(
        &mut self,
        requested: &[CorePath],
    ) -> Result<Vec<CorePath>, CoreCapacity> {
        let mut paths: Vec<_> = requested
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        paths.sort();
        if paths.iter().any(|path| {
            self.path_state(path)
                .is_some_and(|state| state.holds == u32::MAX)
        }) {
            return Err(CoreCapacity {
                detail: "core warm path hold count exhausted",
            });
        }
        self.make_room(&paths)?;
        for path in &paths {
            let id = match self.path_id(path) {
                Some(id) => id,
                None => self.insert_path(path),
            };
            self.paths.state_mut(id).expect("reserved core path").holds += 1;
        }
        Ok(paths)
    }

    /// The caller checks its captured publication boundary before admission.
    /// Existing path holds remain attached to the stable path after an overwrite.
    pub(crate) fn admit(
        &mut self,
        path: &CorePath,
        mut value: CoreWarmSlot,
        dirty_sequence: Option<u64>,
    ) -> Result<(), CoreCapacity> {
        if value.entry.atom_uuid != value.atom.uuid() {
            return Err(CoreCapacity {
                detail: "core tip and atom identities differ",
            });
        }
        let id = self.ensure_path(path)?;
        let molecule = self
            .slots
            .get(id)
            .expect("warm core slot")
            .key
            .molecule
            .clone();
        self.share_model_atom(&molecule, &mut value);
        self.track_atom(&value.atom);
        let slot = self.slots.get(id).expect("warm core slot");
        // A clean read keeps no owned copy: the tip lives in the molecule and
        // the atom in `slot.atom`, which `retained_values` hands to holders.
        // An identical prepared value keeps its owner and reservation.
        let owned = slot
            .owned
            .as_ref()
            .filter(|previous| {
                previous.value.as_ref().is_some_and(|previous| {
                    previous.entry == value.entry
                        && previous.metadata == value.metadata
                        && previous.atom.as_ref() == value.atom.as_ref()
                })
            })
            .cloned();
        let previous = slot.tip.as_ref().map(|tip| tip.atom_uuid.clone());
        if let Some(previous) = previous {
            self.unlink_atom(id, &previous);
        }
        self.link_atom(id, value.atom, value.bytes);
        self.set_slot_tip(id, value.entry, value.metadata);
        self.refresh_tip_charge(id);
        self.mark_slot(id, false, dirty_sequence);
        self.slots.get_mut(id).expect("warm core slot").owned = owned;
        self.touch(path);
        self.trim_after_admit(path);
        Ok(())
    }

    /// Retain a selected tip without a body. Identical full values keep their
    /// body and reservation owner; an unflushed value cannot be replaced here.
    /// The caller supplies the exact sources and authorizes the resulting tip.
    pub(crate) fn admit_tip(
        &mut self,
        path: &CorePath,
        tip: CoreWarmTip,
    ) -> Result<bool, CoreCapacity> {
        let existing = self.slot_of(path).and_then(|id| self.slots.get(id));
        let identical = existing.is_some_and(|slot| {
            slot.tip.as_ref() == Some(&tip.entry)
                && slot.metadata.as_deref() == tip.metadata.as_ref()
        });
        if !identical
            && existing.is_some_and(|slot| {
                slot.owners.iter().any(|owner| {
                    self.paths
                        .state(*owner)
                        .is_some_and(|state| state.dirty.is_some())
                })
            })
        {
            return Err(CoreCapacity {
                detail: "tip-only retention cannot replace an unflushed core value",
            });
        }
        let id = self.ensure_path(path)?;
        let retains_value = identical && self.slot_atom(id, &tip.entry.atom_uuid).is_some();
        if !retains_value {
            self.forget_slot_value(id);
        }
        self.set_slot_tip(id, tip.entry, tip.metadata);
        self.refresh_tip_charge(id);
        self.mark_slot(id, false, None);
        self.touch(path);
        self.trim_after_admit(path);
        Ok(retains_value)
    }

    /// Admit a confirmed absent key under the caller's captured publication lock.
    /// The caller must link every possible winning source before authorization.
    pub(crate) fn admit_absent(
        &mut self,
        path: &CorePath,
        dirty_sequence: Option<u64>,
    ) -> Result<(), CoreCapacity> {
        let id = self.ensure_path(path)?;
        self.forget_slot_sources(id);
        self.forget_slot_value(id);
        self.mark_slot(id, true, dirty_sequence);
        self.slots.get_mut(id).expect("warm core slot").owned =
            Some(model::CoreModelSlot::clean(None));
        self.touch(path);
        Ok(())
    }

    /// Select a known tip, a full value, or absence under the caller's lock.
    pub(crate) fn lookup(&mut self, path: &CorePath) -> CoreWarmLookup {
        let Some(id) = self
            .path_id(path)
            .filter(|id| self.paths.state(*id).is_some_and(|state| state.known))
        else {
            self.misses = self.misses.saturating_add(1);
            return CoreWarmLookup::unknown();
        };
        self.touch_id(id);
        if self.paths.state(id).is_some_and(|state| state.absent) {
            self.hits = self.hits.saturating_add(1);
            return CoreWarmLookup::absent();
        }
        if let Some(selected) = self.select(path) {
            self.hits = self.hits.saturating_add(1);
            selected
        } else {
            self.misses = self.misses.saturating_add(1);
            CoreWarmLookup::unknown()
        }
    }

    /// The ordinary tip resolver can refresh a retained slot without loading
    /// its atom again. Its existing flush protection and source edges remain.
    pub(crate) fn refresh_tip(
        &mut self,
        path: &CorePath,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) -> bool {
        let Some(id) = self
            .path_state(path)
            .filter(|state| state.known && !state.absent)
            .map(|state| state.slot)
        else {
            return false;
        };
        if self.slot_atom(id, &entry.atom_uuid).is_none() {
            return false;
        }
        let Some(slot) = self.slots.get_mut(id) else {
            return false;
        };
        if slot.tip.as_ref().map(|tip| &tip.atom_uuid) != Some(&entry.atom_uuid) {
            return false;
        }
        slot.tip = Some(entry);
        slot.metadata = metadata.map(Box::new);
        if let Some(previous) = &slot.owned {
            let mut value = previous.value.clone();
            if let Some(value) = &mut value {
                value.entry = slot.tip.clone().expect("refreshed tip");
                value.metadata = slot.metadata.as_deref().cloned();
            }
            slot.owned = Some(Arc::new(model::CoreModelSlot {
                value,
                sources: previous.sources.clone(),
                owner: previous.owner.clone(),
                metadata_charge: previous.metadata_charge.clone(),
                charged_bytes: previous.charged_bytes,
            }));
        }
        for owner in &slot.owners {
            if let Some(state) = self.paths.state_mut(*owner) {
                state.tip_authorized = false;
            }
        }
        self.refresh_tip_charge(id);
        self.trim_after_admit(path);
        true
    }

    fn select(&self, path: &CorePath) -> Option<CoreWarmLookup> {
        let id = self.slot_of(path)?;
        let slot = self.slots.get(id)?;
        let entry = slot.tip.clone()?;
        let metadata = slot.metadata.as_deref().cloned();
        Some(match self.slot_atom(id, &entry.atom_uuid) {
            Some(atom) => CoreWarmLookup::value(CoreWarmSelection {
                entry,
                metadata,
                atom: Arc::clone(&atom.value),
                bytes: atom.bytes,
            }),
            None => CoreWarmLookup::Tip(CoreWarmTip { entry, metadata }),
        })
    }

    fn set_slot_tip(
        &mut self,
        id: tables::SlotId,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) {
        let slot = self.slots.get_mut(id).expect("warm core slot");
        slot.tip = Some(entry);
        slot.metadata = metadata.map(Box::new);
    }

    fn slot_atom(&self, id: tables::SlotId, uuid: &str) -> Option<&SharedAtom> {
        self.slots
            .get(id)?
            .atom
            .as_ref()
            .filter(|atom| atom.value.uuid() == uuid)
            .map(Arc::as_ref)
    }

    /// Return the slot of a path that is warm after this call.
    fn ensure_path(&mut self, path: &CorePath) -> Result<tables::SlotId, CoreCapacity> {
        if let Some(state) = self.path_state(path) {
            return Ok(state.slot);
        }
        self.make_room(std::slice::from_ref(path))?;
        let id = self.insert_path(path);
        Ok(self.paths.state(id).expect("warm core path").slot)
    }

    fn insert_path(&mut self, path: &CorePath) -> path_table::PathId {
        let slot = if let Some(slot) = self.slots.find_path(path) {
            slot
        } else {
            let slot = self.slots.insert(path.slot_key());
            self.charge_slot(slot);
            slot
        };
        let charge = self.charge_path();
        let id = self.paths.insert(
            path,
            PathState {
                slot,
                charge,
                ..PathState::default()
            },
        );
        self.slots
            .get_mut(slot)
            .expect("warm core slot")
            .owners
            .push(id);
        self.touch_id(id);
        id
    }

    fn mark_slot(&mut self, id: tables::SlotId, absent: bool, sequence: Option<u64>) {
        let Some(slot) = self.slots.get(id) else {
            return;
        };
        for path in &slot.owners {
            let state = self.paths.state_mut(*path).expect("core slot owner");
            state.known = true;
            state.absent = absent;
            state.tip_authorized = false;
            if let Some(sequence) = sequence {
                state.dirty = Some(state.dirty.map_or(sequence, |old| old.max(sequence)));
            }
        }
    }

    fn link_atom(&mut self, id: tables::SlotId, value: Arc<Atom>, bytes: usize) {
        let Some(slot) = self.slots.get_mut(id) else {
            return;
        };
        let key = AtomKey {
            scope: slot.key.molecule.scope.clone(),
            uuid: value.uuid().to_string(),
        };
        let variants = self.atoms.entry(key).or_default();
        variants.retain(|existing| existing.strong_count() > 0);
        let shared = variants
            .iter()
            .filter_map(Weak::upgrade)
            .find(|existing| existing.value.as_ref() == value.as_ref());
        let atom = shared.unwrap_or_else(|| {
            let atom = Arc::new(SharedAtom { value, bytes });
            variants.push(Arc::downgrade(&atom));
            atom
        });
        slot.atom = Some(atom);
    }
}
