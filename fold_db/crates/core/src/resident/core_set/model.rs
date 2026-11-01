//! Typed write effects are prepared before disk-first core publication.

use super::*;
use crate::memory_budget::DeferReservation;
use crate::resident::core_keys::CoreKeyBudget;

mod prepare;

const METADATA_BYTES: usize = 16 * 1024 * 1024;

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

/// Fully prepared memory definitions. Installation cannot refuse quota.
pub(crate) struct CoreModelUpdate {
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
    /// Publish prepared memory definitions only after their complete disk root
    /// is committed.
    pub(crate) fn install_clean(&mut self, update: &CoreModelUpdate) {
        self.install_model_changes(update);
    }

    fn install_model_changes(&mut self, update: &CoreModelUpdate) {
        for definition in &update.definitions {
            self.memory.install_prepared(definition.clone());
        }
    }

    /// A format 7 home keeps no current field slots, so the current byte
    /// fields are 0 and every reserved byte counts as retained.
    pub(crate) fn model_stats(&self) -> CoreModelStats {
        let budget = self.model.budget.stats();
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
            metadata_current_bytes: 0,
            metadata_used_bytes: budget.used_bytes,
            metadata_retained_bytes: budget.used_bytes,
            metadata_peak_bytes: budget.peak_bytes,
            refused: self.model.refused,
            body_reserved_current_bytes: 0,
            body_reserved_used_bytes: body_used,
            body_reserved_retained_bytes: body_used,
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
