//! Atom preparation, molecule apply/persist, and per-key store paths.
//!
//! The molecule key decisions live in [`super::plan::molecules`].

mod apply;
pub(in crate::fold_db_core::mutation_manager) mod batch_rebase;
mod delta;
mod persist;
mod prepare;
mod restore;

pub(in crate::fold_db_core::mutation_manager) use super::plan::molecules::PreparedFieldAtom;
pub(in crate::fold_db_core::mutation_manager) use delta::PreparedSchemaDelta;
pub(in crate::fold_db_core::mutation_manager) use persist::{
    DeferredLaneWriter, DeferredPersistJob, LanePersistJob, PurgeCompletion, PurgeEnvelope,
};
pub(in crate::fold_db_core::mutation_manager) use prepare::PreparedAtoms;
