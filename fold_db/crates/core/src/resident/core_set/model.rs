//! Typed write effects are prepared before disk-first core publication.

use super::*;
use crate::memory_budget::DeferReservation;
use crate::resident::core_keys::{CoreKeyBudget, CoreKeyReservation};

mod prepare;

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
}

pub(super) struct CoreModel {
    budget: Arc<CoreKeyBudget>,
    refused: u64,
    body_owners: HashMap<usize, Weak<DeferReservation>>,
    body_peak_bytes: u64,
}

impl Default for CoreModel {
    fn default() -> Self {
        Self {
            budget: Arc::new(CoreKeyBudget::new(METADATA_BYTES)),
            refused: 0,
            body_owners: HashMap::new(),
            body_peak_bytes: 0,
        }
    }
}

/// Fully prepared metadata and typed values. Installation cannot refuse quota.
pub(crate) struct CoreModelUpdate {
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

    /// All paths must already hold the ordinary core path reservation.
    /// Publish prepared effects only after their complete disk root is committed.
    pub(crate) fn install_clean(&mut self, update: &CoreModelUpdate) {
        self.install_model_changes(update);
    }

    fn install_model_changes(&mut self, update: &CoreModelUpdate) {
        for (path, value) in &update.changes {
            self.install_clean_slot(path, value);
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

    fn install_clean_slot(&mut self, path: &CorePath, value: &CoreModelSlot) {
        // Preparation still owns this allocation until publication returns,
        // including when admission shares an equal current atom.
        if let Some(slot) = &value.value {
            self.track_atom(&slot.atom);
        }
        let result = match &value.value {
            Some(slot) => self.admit(path, slot.clone()),
            None => self.admit_absent(path),
        };
        debug_assert!(result.is_ok(), "prepared and held core path");
        if let Some(slot) = self.slot_of(path).and_then(|id| self.slots.get_mut(id)) {
            // The complete disk root is already committed. Ordinary warm-set
            // charges and read-held atom owners now cover the clean value;
            // do not retain the temporary write metadata/quota for its life.
            slot.owned = None;
        }
    }

    pub(crate) fn model_stats(&self) -> CoreModelStats {
        let budget = self.model.budget.stats();
        let current = self
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
            // Retained status fields: native writes publish only after disk commit.
            dirty_slots: 0,
            dirty_molecules: 0,
            dirty_definitions: 0,
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
