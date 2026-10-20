//! Immutable typed dirty state shares its values with the current core model.

use super::*;
use crate::memory_budget::DeferReservation;
use crate::resident::core_keys::{CoreKeyBudget, CoreKeyReservation};

mod coverage;
mod prepare;
mod snapshot;

const METADATA_BYTES: usize = 16 * 1024 * 1024;

/// The final effect for one logical key. None means a confirmed absence.
pub(crate) struct CoreModelChange {
    pub(crate) path: CorePath,
    pub(crate) slot: Option<CoreWarmSlot>,
    pub(crate) sources: Vec<(String, String)>,
}

pub(super) struct CoreModelSlot {
    pub(super) value: Option<CoreWarmSlot>,
    pub(super) sources: Vec<(String, String)>,
    pub(super) owner: Option<Arc<DeferReservation>>,
    pub(super) metadata_charge: Option<Arc<CoreKeyReservation>>,
    pub(super) charged_bytes: usize,
}

impl CoreModelSlot {
    pub(super) fn clean(value: Option<CoreWarmSlot>) -> Arc<Self> {
        Arc::new(Self {
            value,
            sources: Vec::new(),
            owner: None,
            metadata_charge: None,
            charged_bytes: 0,
        })
    }

    fn lookup(&self) -> CoreWarmLookup {
        match &self.value {
            Some(value) => CoreWarmLookup::value(CoreWarmSelection {
                entry: value.entry.clone(),
                metadata: value.metadata.clone(),
                atom: Arc::clone(&value.atom),
                bytes: value.bytes,
            }),
            None => CoreWarmLookup::absent(),
        }
    }
}

struct ModelMolecule {
    model: MoleculeHashRange,
    slots: BTreeMap<(String, String), Arc<CoreModelSlot>>,
    _charge: CoreKeyReservation,
    charged_bytes: usize,
}

type PreparedMolecule = (ModelMolecule, Vec<(CorePath, Arc<CoreModelSlot>)>);

#[derive(Default)]
struct ModelRoot {
    molecules: HashMap<MoleculeKey, Arc<ModelMolecule>>,
    definitions: HashMap<memory::MemoryKey, PreparedCoreMemory>,
    sequence: u64,
    _charge: Option<CoreKeyReservation>,
    charged_bytes: usize,
}

pub(super) struct CoreModel {
    root: Arc<ModelRoot>,
    budget: Arc<CoreKeyBudget>,
    refused: u64,
    body_owners: HashMap<usize, Weak<DeferReservation>>,
    body_peak_bytes: u64,
}

impl Default for CoreModel {
    fn default() -> Self {
        Self {
            root: Arc::new(ModelRoot::default()),
            budget: Arc::new(CoreKeyBudget::new(METADATA_BYTES)),
            refused: 0,
            body_owners: HashMap::new(),
            body_peak_bytes: 0,
        }
    }
}

/// One retained typed read version, without raw queue or encoded-body owners.
#[derive(Clone)]
pub(crate) struct CoreModelSnapshot {
    root: Arc<ModelRoot>,
}

/// Fully prepared metadata and typed values. Installation cannot refuse quota.
pub(crate) struct CoreModelUpdate {
    root: ModelRoot,
    changes: Vec<(CorePath, Arc<CoreModelSlot>)>,
    definitions: Vec<PreparedCoreMemory>,
}

#[derive(Clone, Copy, Default, serde::Serialize)]
pub(crate) struct CoreModelStats {
    pub(crate) dirty_slots: usize,
    pub(crate) dirty_molecules: usize,
    pub(crate) dirty_definitions: usize,
    pub(crate) metadata_limit_bytes: usize,
    pub(crate) metadata_current_bytes: usize,
    pub(crate) metadata_used_bytes: usize,
    /// Includes held old versions and privately prepared replacement versions.
    pub(crate) metadata_retained_bytes: usize,
    pub(crate) metadata_peak_bytes: usize,
    pub(crate) refused: u64,
    pub(crate) body_reserved_current_bytes: u64,
    pub(crate) body_reserved_used_bytes: u64,
    /// Held earlier values and privately prepared owners outside current slots.
    pub(crate) body_reserved_retained_bytes: u64,
    pub(crate) body_reserved_peak_bytes: u64,
}

impl CoreWarmSet {
    /// A path hold protects membership; these owners protect overwritten bytes.
    /// A clean value has no owned copy, so its holder keeps the atom itself.
    pub(crate) fn retained_values(
        &self,
        paths: &[CorePath],
    ) -> Vec<Arc<dyn std::any::Any + Send + Sync>> {
        paths
            .iter()
            .filter_map(|path| {
                let slot = self.slots.get(self.slot_of(path)?)?;
                match (&slot.owned, &slot.atom) {
                    (Some(owned), _) => {
                        Some(Arc::clone(owned) as Arc<dyn std::any::Any + Send + Sync>)
                    }
                    (None, Some(atom)) => {
                        Some(Arc::clone(&atom.value) as Arc<dyn std::any::Any + Send + Sync>)
                    }
                    (None, None) => None,
                }
            })
            .collect()
    }

    pub(crate) fn dirty_snapshot(&self) -> CoreModelSnapshot {
        CoreModelSnapshot {
            root: Arc::clone(&self.model.root),
        }
    }

    /// All paths must already hold the ordinary core path reservation.
    pub(crate) fn install_dirty(&mut self, mut update: CoreModelUpdate, sequence: u64) {
        self.install_model_changes(&update, Some(sequence));
        update.root.sequence = sequence;
        self.model.root = Arc::new(update.root);
    }

    /// Disk-first publication installs the same values without a new dirty root.
    pub(crate) fn install_clean(&mut self, update: &CoreModelUpdate) {
        self.install_model_changes(update, None);
    }

    fn install_model_changes(&mut self, update: &CoreModelUpdate, sequence: Option<u64>) {
        for (path, value) in &update.changes {
            self.install_owned(path, Arc::clone(value), sequence);
            self.clear_sources(path);
            for (collection, key) in &value.sources {
                let retained = self.link_source(path, collection, key);
                debug_assert!(retained, "prepared core source count");
            }
            self.authorize_path(path);
        }
        for definition in &update.definitions {
            self.memory.install_prepared(definition.clone());
        }
    }

    fn install_owned(&mut self, path: &CorePath, value: Arc<CoreModelSlot>, sequence: Option<u64>) {
        // The immutable slot/root retains this allocation even if admission
        // shares an equal current atom. Both allocations must stay measured.
        if let Some(slot) = &value.value {
            self.track_atom(&slot.atom);
        }
        let result = match &value.value {
            Some(slot) => self.admit(path, slot.clone(), sequence),
            None => self.admit_absent(path, sequence),
        };
        debug_assert!(result.is_ok(), "prepared and held core path");
        if let Some(slot) = self.slot_of(path).and_then(|id| self.slots.get_mut(id)) {
            slot.owned = Some(value);
        }
    }

    pub(crate) fn model_stats(&self) -> CoreModelStats {
        let root = &self.model.root;
        let budget = self.model.budget.stats();
        let current = root.charged_bytes
            + root
                .molecules
                .values()
                .map(|value| value.charged_bytes)
                .sum::<usize>()
            + self
                .slots
                .iter()
                .filter_map(|(_, slot)| slot.owned.as_ref())
                .map(|slot| slot.charged_bytes)
                .sum::<usize>();
        let current_owners: HashMap<_, _> = self
            .slots
            .iter()
            .filter_map(|(_, slot)| slot.owned.as_ref())
            .filter_map(|slot| {
                slot.owner
                    .as_ref()
                    .map(|owner| (Arc::as_ptr(owner) as usize, owner.bytes()))
            })
            .collect();
        let body_current = current_owners.values().sum();
        let body_used: u64 = self
            .model
            .body_owners
            .values()
            .filter_map(Weak::upgrade)
            .map(|owner| owner.bytes())
            .sum();
        CoreModelStats {
            dirty_slots: root.molecules.values().map(|value| value.slots.len()).sum(),
            dirty_molecules: root.molecules.len(),
            dirty_definitions: root.definitions.len(),
            metadata_limit_bytes: budget.limit_bytes,
            metadata_current_bytes: current,
            metadata_used_bytes: budget.used_bytes,
            metadata_retained_bytes: budget.used_bytes.saturating_sub(current),
            metadata_peak_bytes: budget.peak_bytes,
            refused: self.model.refused,
            body_reserved_current_bytes: body_current,
            body_reserved_used_bytes: body_used,
            body_reserved_retained_bytes: body_used.saturating_sub(body_current),
            body_reserved_peak_bytes: self.model.body_peak_bytes,
        }
    }

    pub(super) fn complete_model_flush(&mut self, sequence: u64) {
        if self.model.root.sequence <= sequence {
            self.model.root = Arc::new(ModelRoot::default());
        }
    }

    fn observe_model_owner(&mut self, owner: Option<&Arc<DeferReservation>>) {
        self.model
            .body_owners
            .retain(|_, owner| owner.strong_count() > 0);
        if let Some(owner) = owner {
            self.model
                .body_owners
                .insert(Arc::as_ptr(owner) as usize, Arc::downgrade(owner));
        }
        let used = self
            .model
            .body_owners
            .values()
            .filter_map(Weak::upgrade)
            .map(|owner| owner.bytes())
            .sum();
        self.model.body_peak_bytes = self.model.body_peak_bytes.max(used);
    }
}

pub(super) fn capacity() -> CoreCapacity {
    CoreCapacity {
        detail: "immutable core model metadata allowance refused the publication",
    }
}
