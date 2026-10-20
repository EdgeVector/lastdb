//! Preflight all immutable metadata before the common publication switch.

use super::coverage::covered;
use super::*;
use std::io::{self, Write};

impl CoreWarmSet {
    pub(crate) fn prepare_dirty(
        &mut self,
        changes: Vec<CoreModelChange>,
        definitions: Vec<PreparedCoreMemory>,
        owner: Option<&Arc<DeferReservation>>,
        changed_sources: &[(String, String)],
    ) -> Result<CoreModelUpdate, CoreCapacity> {
        self.observe_model_owner(owner);
        if owner.is_none() && changes.iter().any(|change| change.slot.is_some()) {
            return Err(capacity());
        }
        let result = if covered(&self.model.root, &changes, &definitions, changed_sources) {
            self.prepare_model(changes, definitions, owner)
        } else {
            Err(CoreCapacity {
                detail: "dirty core effects do not cover every changed source",
            })
        };
        if result.is_err() {
            self.model.refused = self.model.refused.saturating_add(1);
        }
        result
    }

    /// A committed root needs only its incoming current-model effects.
    pub(crate) fn prepare_clean(
        &mut self,
        changes: Vec<CoreModelChange>,
        definitions: Vec<PreparedCoreMemory>,
        owner: Option<&Arc<DeferReservation>>,
    ) -> Result<CoreModelUpdate, CoreCapacity> {
        self.observe_model_owner(owner);
        if owner.is_none() && changes.iter().any(|change| change.slot.is_some()) {
            return Err(capacity());
        }
        validate(self, &changes)?;
        let sources = changes.iter().try_fold(0usize, |count, change| {
            count.checked_add(change.sources.len()).ok_or_else(capacity)
        })?;
        self.reserve_install(changes.len(), sources, definitions.len())?;
        let mut slots = HashMap::new();
        let mut paths = Vec::new();
        for change in changes {
            let mut value = change.slot;
            if let Some(value) = &mut value {
                let molecule =
                    MoleculeKey::new(change.path.scope.as_deref(), &change.path.molecule);
                self.share_model_atom(&molecule, value);
            }
            let slot = self.prepare_slot(value, change.sources, owner.cloned())?;
            slots.insert(change.path.slot_key(), slot);
            paths.push(change.path);
        }
        let changes = paths
            .into_iter()
            .map(|path| {
                let value = Arc::clone(&slots[&path.slot_key()]);
                (path, value)
            })
            .collect();
        Ok(CoreModelUpdate {
            root: ModelRoot::default(),
            changes,
            definitions,
        })
    }

    fn prepare_model(
        &mut self,
        changes: Vec<CoreModelChange>,
        definitions: Vec<PreparedCoreMemory>,
        owner: Option<&Arc<DeferReservation>>,
    ) -> Result<CoreModelUpdate, CoreCapacity> {
        validate(self, &changes)?;
        let bytes = root_quote(&self.model.root, &changes, &definitions);
        let charge = self
            .model
            .budget
            .reserve_temporary(bytes)
            .map_err(|_| capacity())?;
        let mut root = ModelRoot {
            molecules: self.model.root.molecules.clone(),
            definitions: self.model.root.definitions.clone(),
            sequence: 0,
            charged_bytes: bytes,
            _charge: Some(charge),
        };
        let mut grouped: HashMap<MoleculeKey, Vec<CoreModelChange>> = HashMap::new();
        for change in changes {
            grouped
                .entry(MoleculeKey::new(
                    change.path.scope.as_deref(),
                    &change.path.molecule,
                ))
                .or_default()
                .push(change);
        }
        let mut prepared = Vec::new();
        for (key, changes) in grouped {
            let previous = root.molecules.get(&key).map(Arc::as_ref);
            let (molecule, paths) = self.prepare_molecule(&key, previous, changes, owner)?;
            prepared.extend(paths);
            root.molecules.insert(key, Arc::new(molecule));
        }
        let mut definitions_by_key = HashMap::new();
        for definition in definitions {
            root.definitions
                .insert(definition.key.clone(), definition.clone());
            definitions_by_key.insert(definition.key.clone(), definition);
        }
        if root.definitions.len() > 10_000 {
            return Err(capacity());
        }
        let sources = prepared.iter().try_fold(0usize, |count, (_, value)| {
            count.checked_add(value.sources.len()).ok_or_else(capacity)
        })?;
        self.reserve_install(prepared.len(), sources, definitions_by_key.len())?;
        Ok(CoreModelUpdate {
            root,
            changes: prepared,
            definitions: definitions_by_key.into_values().collect(),
        })
    }

    fn prepare_molecule(
        &self,
        key: &MoleculeKey,
        previous: Option<&ModelMolecule>,
        changes: Vec<CoreModelChange>,
        owner: Option<&Arc<DeferReservation>>,
    ) -> Result<PreparedMolecule, CoreCapacity> {
        let bytes = molecule_quote(previous, &changes)?;
        let charge = self
            .model
            .budget
            .reserve_temporary(bytes)
            .map_err(|_| capacity())?;
        let mut slots = previous.map_or_else(BTreeMap::new, |old| old.slots.clone());
        let mut paths = Vec::with_capacity(changes.len());
        for change in changes {
            let mut value = change.slot;
            if let Some(value) = &mut value {
                self.share_model_atom(key, value);
            }
            let at = (change.path.hash.clone(), change.path.range.clone());
            let slot = self.prepare_slot(value, change.sources, owner.cloned())?;
            slots.insert(at, slot);
            paths.push(change.path);
        }
        let mut model = MoleculeHashRange::with_partial_uuid(key.molecule.to_string());
        for ((hash, range), value) in &slots {
            if let Some(value) = &value.value {
                model.insert_loaded_slot_verbatim(
                    hash.clone(),
                    range.clone(),
                    value.entry.clone(),
                    value.metadata.clone(),
                );
            }
        }
        let paths = paths
            .into_iter()
            .map(|path| {
                let value = Arc::clone(&slots[&(path.hash.clone(), path.range.clone())]);
                (path, value)
            })
            .collect();
        Ok((
            ModelMolecule {
                model,
                slots,
                charged_bytes: bytes,
                _charge: charge,
            },
            paths,
        ))
    }

    pub(in crate::resident::core_set) fn share_model_atom(
        &self,
        key: &MoleculeKey,
        value: &mut CoreWarmSlot,
    ) {
        let atom = AtomKey {
            scope: key.scope.clone(),
            uuid: value.atom.uuid().to_owned(),
        };
        if let Some(existing) = self
            .atoms
            .get(&atom)
            .into_iter()
            .flatten()
            .filter_map(Weak::upgrade)
            .find(|existing| existing.value.as_ref() == value.atom.as_ref())
        {
            value.atom = Arc::clone(&existing.value);
        }
    }

    fn prepare_slot(
        &self,
        value: Option<CoreWarmSlot>,
        sources: Vec<(String, String)>,
        owner: Option<Arc<DeferReservation>>,
    ) -> Result<Arc<CoreModelSlot>, CoreCapacity> {
        let payload = value
            .as_ref()
            .map(|value| encoded_len(&(&value.entry, &value.metadata)))
            .transpose()?
            .unwrap_or(0);
        let bytes = sources.iter().fold(
            2048usize.saturating_add(payload.saturating_mul(4)),
            |n, (collection, key)| n.saturating_add(512 + 4 * (collection.len() + key.len())),
        );
        let charge = self
            .model
            .budget
            .reserve_temporary(bytes)
            .map_err(|_| capacity())?;
        let owner = if value.is_some() { owner } else { None };
        Ok(Arc::new(CoreModelSlot {
            value,
            sources,
            owner,
            metadata_charge: Some(Arc::new(charge)),
            charged_bytes: bytes,
        }))
    }

    fn reserve_install(
        &mut self,
        slots: usize,
        sources: usize,
        definitions: usize,
    ) -> Result<(), CoreCapacity> {
        self.molecules.try_reserve(slots).map_err(|_| capacity())?;
        self.slots.try_reserve(slots)?;
        self.sources.try_reserve(sources)?;
        self.memory.reserve_install(definitions)
    }
}

fn validate(core: &CoreWarmSet, changes: &[CoreModelChange]) -> Result<(), CoreCapacity> {
    for change in changes {
        if core
            .path_state(&change.path)
            .is_none_or(|state| state.holds == 0)
            || change.sources.is_empty()
            || change.sources.len() > 64
            || change
                .slot
                .as_ref()
                .is_some_and(|slot| slot.entry.atom_uuid != slot.atom.uuid())
        {
            return Err(capacity());
        }
    }
    Ok(())
}

fn root_quote(
    root: &ModelRoot,
    changes: &[CoreModelChange],
    definitions: &[PreparedCoreMemory],
) -> usize {
    let molecules = root
        .molecules
        .keys()
        .map(|key| key.molecule.len() + key.scope.as_ref().map_or(0, |scope| scope.len()));
    let additions = changes.iter().map(|change| {
        change.path.molecule.len() + change.path.scope.as_ref().map_or(0, String::len)
    });
    let names = root
        .definitions
        .keys()
        .map(|key| key.1.len())
        .chain(definitions.iter().map(|value| value.key.1.len()));
    molecules
        .chain(additions)
        .chain(names)
        .fold(4096usize, |bytes, length| {
            bytes.saturating_add(1024 + length.saturating_mul(4))
        })
}

fn molecule_quote(
    previous: Option<&ModelMolecule>,
    changes: &[CoreModelChange],
) -> Result<usize, CoreCapacity> {
    let previous = previous.into_iter().flat_map(|value| {
        value
            .slots
            .iter()
            .map(|((hash, range), value)| (hash.as_str(), range.as_str(), value.value.as_ref()))
    });
    let incoming = changes.iter().map(|change| {
        (
            change.path.hash.as_str(),
            change.path.range.as_str(),
            change.slot.as_ref(),
        )
    });
    previous
        .chain(incoming)
        .try_fold(4096usize, |bytes, (hash, range, value)| {
            let payload = match value {
                Some(value) => encoded_len(&(&value.entry, &value.metadata))?,
                None => 0,
            };
            // Source vectors belong to the shared slot owner. This map only
            // owns its key copies, Arc references, and logical tip projection.
            bytes
                .checked_add(2048 + 8 * (hash.len() + range.len() + payload))
                .ok_or_else(capacity)
        })
}

fn encoded_len(value: &impl serde::Serialize) -> Result<usize, CoreCapacity> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("metadata size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| capacity())?;
    Ok(counter.0)
}
