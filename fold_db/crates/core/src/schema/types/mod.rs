//! Schema model types live in `lastdb_model::schema::types`. This module keeps
//! the old `crate::schema::types` path and adds the behavior the model must
//! not hold: the task-local key comparison context and the page order that
//! reads it, and the uuid v4 constructor of a mutation.

pub use lastdb_model::schema::types::*;

pub mod key_comparison;
mod mutation_ids;

pub use key_comparison::KeyValuePageOrder;
pub use mutation_ids::MutationIds;
