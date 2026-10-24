//! Preflight all immutable metadata before the common publication switch.

use super::*;
use std::io::{self, Write};

impl CoreWarmSet {
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
            changes,
            definitions,
        })
    }

    pub(in crate::resident::core_set) fn share_model_atom(
        &self,
        key: &MoleculeKey,
        value: &mut CoreWarmSlot,
    ) {
        let atom = AtomKey::new(key.scope.clone(), value.atom.uuid());
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
