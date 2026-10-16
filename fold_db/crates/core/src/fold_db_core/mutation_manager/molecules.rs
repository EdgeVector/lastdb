//! Atom preparation, molecule apply/persist, and per-key store paths.

mod apply;
mod apply_gate;
mod batch_rebase;
mod field_atom;
mod persist;
mod prepare;
mod prepared;
mod protein_batch;
mod restore;
mod scoped_gate;
mod storage_links;
mod warm_release;

pub(in crate::fold_db_core::mutation_manager) use apply_gate::{
    ApplyGateOutcome, PreparedSchemaDelta,
};
pub(in crate::fold_db_core::mutation_manager) use persist::{
    DeferredLaneWriter, DeferredPersistJob, LanePersistJob, MoleculeGateGuard, PurgeCompletion,
    PurgeEnvelope, ResidentPurgeSlot, StorageSlotPurgeEnvelope,
};
pub(in crate::fold_db_core::mutation_manager) use prepared::{
    PreparedSiblingUpdates, PreparedSlotRevision,
};
