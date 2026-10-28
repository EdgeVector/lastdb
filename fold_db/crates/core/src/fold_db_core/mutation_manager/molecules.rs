//! Atom preparation, molecule apply/persist, and per-key store paths.

mod apply;
pub(in crate::fold_db_core::mutation_manager) mod batch_rebase;
mod delta;
mod field_rules;
mod persist;
mod prepare;
mod prepared;
mod protein_batch;
mod protein_key_fields;
mod protein_moves;
mod restore;

pub(in crate::fold_db_core::mutation_manager) use delta::PreparedSchemaDelta;
pub(in crate::fold_db_core::mutation_manager) use persist::{
    DeferredLaneWriter, DeferredPersistJob, LanePersistJob, PurgeCompletion, PurgeEnvelope,
};
pub(in crate::fold_db_core::mutation_manager) use prepare::PreparedAtoms;
pub(in crate::fold_db_core::mutation_manager) use prepared::{PreparedSiblingUpdates, SiblingMove};
