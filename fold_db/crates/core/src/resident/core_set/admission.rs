//! Admission changes the actual core objects, never a parallel tip value map.

use super::*;

impl CoreWarmSet {
    /// Reserve one hold per distinct path, or leave the set unchanged on refusal.
    /// The returned paths must be released once after the caller finishes.
    pub(crate) fn reserve_paths(
        &mut self,
        requested: &[CorePath],
    ) -> Result<Vec<CorePath>, CoreCapacity> {
        if requested.len() > super::super::resident_key_cap() {
            return Err(CoreCapacity {
                detail: "core warm reservation exceeds the logical path cap",
            });
        }
        let mut paths: Vec<_> = requested
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        paths.sort();
        if paths.iter().any(|path| {
            self.paths
                .get(path)
                .is_some_and(|state| state.holds == u32::MAX)
        }) {
            return Err(CoreCapacity {
                detail: "core warm path hold count exhausted",
            });
        }
        self.make_room(&paths)?;
        for path in &paths {
            if !self.paths.contains_key(path) {
                self.insert_path(path);
            }
            self.paths.get_mut(path).expect("reserved core path").holds += 1;
        }
        Ok(paths)
    }

    /// The caller checks its captured publication boundary before admission.
    /// Existing path holds remain attached to the stable path after an overwrite.
    pub(crate) fn admit(
        &mut self,
        path: &CorePath,
        value: CoreWarmSlot,
        dirty_sequence: Option<u64>,
    ) -> Result<(), CoreCapacity> {
        if value.entry.atom_uuid != value.atom.uuid() {
            return Err(CoreCapacity {
                detail: "core tip and atom identities differ",
            });
        }
        self.ensure_path(path)?;
        let owned = self
            .owned_slots
            .get(&path.slot())
            .filter(|previous| {
                previous.value.as_ref().is_some_and(|previous| {
                    previous.entry == value.entry
                        && previous.metadata == value.metadata
                        && previous.atom.as_ref() == value.atom.as_ref()
                })
            })
            .cloned()
            .unwrap_or_else(|| model::CoreModelSlot::clean(Some(value.clone())));
        let slot = path.slot();
        let previous = self
            .molecules
            .get(&slot.molecule)
            .and_then(|molecule| molecule.get_atom_uuid(&slot.hash, &slot.range))
            .cloned();
        if let Some(previous) = previous {
            self.unlink_atom(&slot, &previous);
        }
        self.link_atom(&slot, value.atom, value.bytes);
        self.molecules
            .entry(slot.molecule.clone())
            .or_insert_with(|| MoleculeHashRange::with_partial_uuid(path.molecule.clone()))
            .insert_loaded_slot_verbatim(
                slot.hash.clone(),
                slot.range.clone(),
                value.entry,
                value.metadata,
            );
        self.mark_slot(&slot, false, dirty_sequence);
        self.owned_slots.insert(slot, owned);
        self.touch(path);
        Ok(())
    }

    /// Admit a confirmed absent key under the caller's captured publication lock.
    /// The caller must link every possible winning source before authorization.
    pub(crate) fn admit_absent(
        &mut self,
        path: &CorePath,
        dirty_sequence: Option<u64>,
    ) -> Result<(), CoreCapacity> {
        self.ensure_path(path)?;
        let slot = path.slot();
        self.forget_slot_sources(&slot);
        self.forget_slot_value(&slot);
        self.mark_slot(&slot, true, dirty_sequence);
        self.owned_slots
            .insert(slot, model::CoreModelSlot::clean(None));
        self.touch(path);
        Ok(())
    }

    /// Select the current full atom or confirmed absence under the caller's lock.
    pub(crate) fn lookup(&mut self, path: &CorePath) -> CoreWarmLookup {
        let known = self.paths.get(path).is_some_and(|state| state.known);
        if !known {
            self.misses = self.misses.saturating_add(1);
            return CoreWarmLookup::unknown();
        }
        self.touch(path);
        if self.paths.get(path).is_some_and(|state| state.absent) {
            self.hits = self.hits.saturating_add(1);
            return CoreWarmLookup::absent();
        }
        if let Some(selected) = self.select(path) {
            self.hits = self.hits.saturating_add(1);
            CoreWarmLookup::tip(selected)
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
        if !self
            .paths
            .get(path)
            .is_some_and(|state| state.known && !state.absent)
        {
            return false;
        }
        let slot = path.slot();
        if self.slot_atom(&slot, &entry.atom_uuid).is_none() {
            return false;
        }
        let Some(molecule) = self.molecules.get_mut(&slot.molecule) else {
            return false;
        };
        if molecule.get_atom_uuid(&slot.hash, &slot.range) != Some(&entry.atom_uuid) {
            return false;
        }
        molecule.insert_loaded_slot_verbatim(
            slot.hash.clone(),
            slot.range.clone(),
            entry,
            metadata,
        );
        if let Some(previous) = self.owned_slots.get(&slot) {
            let mut value = previous.value.clone();
            if let Some(value) = &mut value {
                value.entry = molecule
                    .get_atom_entry(&slot.hash, &slot.range)
                    .expect("refreshed tip")
                    .clone();
                value.metadata = molecule.get_key_metadata(&slot.hash, &slot.range).cloned();
            }
            self.owned_slots.insert(
                slot.clone(),
                Arc::new(model::CoreModelSlot {
                    value,
                    sources: previous.sources.clone(),
                    owner: previous.owner.clone(),
                    metadata_charge: previous.metadata_charge.clone(),
                    charged_bytes: previous.charged_bytes,
                }),
            );
        }
        for owner in self.slot_owners.get(&slot).into_iter().flatten() {
            if let Some(state) = self.paths.get_mut(owner) {
                state.tip_authorized = false;
            }
        }
        true
    }

    fn select(&self, path: &CorePath) -> Option<CoreWarmSelection> {
        let slot = path.slot();
        let molecule = self.molecules.get(&slot.molecule)?;
        let entry = molecule.get_atom_entry(&slot.hash, &slot.range)?.clone();
        let atom = self.slot_atom(&slot, &entry.atom_uuid)?;
        Some(CoreWarmSelection {
            entry,
            metadata: molecule.get_key_metadata(&slot.hash, &slot.range).cloned(),
            atom: Arc::clone(&atom.value),
            bytes: atom.bytes,
        })
    }

    fn slot_atom(&self, slot: &SlotKey, uuid: &str) -> Option<&SharedAtom> {
        self.slot_atoms
            .get(slot)
            .filter(|atom| atom.value.uuid() == uuid)
            .map(Arc::as_ref)
    }

    fn ensure_path(&mut self, path: &CorePath) -> Result<(), CoreCapacity> {
        if !self.paths.contains_key(path) {
            self.make_room(std::slice::from_ref(path))?;
            self.insert_path(path);
        }
        Ok(())
    }

    fn insert_path(&mut self, path: &CorePath) {
        self.slot_owners
            .entry(path.slot())
            .or_default()
            .insert(path.clone());
        self.paths.insert(path.clone(), PathState::default());
        self.touch(path);
    }

    fn mark_slot(&mut self, slot: &SlotKey, absent: bool, sequence: Option<u64>) {
        for path in self.slot_owners.get(slot).into_iter().flatten() {
            let state = self.paths.get_mut(path).expect("core slot owner");
            state.known = true;
            state.absent = absent;
            state.tip_authorized = false;
            if let Some(sequence) = sequence {
                state.dirty = Some(state.dirty.map_or(sequence, |old| old.max(sequence)));
            }
        }
    }

    fn link_atom(&mut self, slot: &SlotKey, value: Arc<Atom>, bytes: usize) {
        let key = AtomKey {
            scope: slot.molecule.scope.clone(),
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
        self.slot_atoms.insert(slot.clone(), atom);
    }
}
