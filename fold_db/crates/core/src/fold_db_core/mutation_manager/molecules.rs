//! Atom preparation, molecule apply/persist, and per-key store paths.

mod apply;
mod apply_gate;
mod persist;
mod prepare;
mod protein_batch;
mod restore;

pub(in crate::fold_db_core::mutation_manager) use apply_gate::{
    ApplyGateOutcome, PreparedSchemaDelta,
};
pub(in crate::fold_db_core::mutation_manager) use persist::{
    DeferredLaneWriter, DeferredPersistJob, LanePersistJob, PurgeEnvelope, ResidentPurgeSlot,
    StorageSlotPurgeEnvelope,
};
