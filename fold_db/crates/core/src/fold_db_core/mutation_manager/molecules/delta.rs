//! Prepared native logical effects for one schema.

use std::collections::{HashMap, HashSet};

use super::super::plan::molecules::PreparedFieldAtom;
use crate::atom::Atom;
use crate::db_operations::ChangedKey;
use crate::schema::types::{KeyValue, Mutation, Schema};

/// One schema's private effects in the complete native request.
pub(in crate::fold_db_core::mutation_manager) struct PreparedSchemaDelta {
    /// Registered before atom creation so an atom-GC cut cannot miss this job.
    pub pending_persist_task: Option<crate::fold_db_core::pending_task_tracker::PendingTask>,
    pub schema_name: String,
    pub schema: Schema,
    pub schema_mutations: Vec<Mutation>,
    pub mutation_key_values: Vec<KeyValue>,
    pub atom_results: Vec<PreparedFieldAtom>,
    pub deferred_atoms: Option<Vec<Atom>>,
    pub search_batch: Option<crate::db_operations::search_index::IndexChangeBatch>,
    pub share_prefixes: Vec<String>,
    pub idempotency_entries: Vec<(String, String)>,
    pub changed_keys: HashMap<String, HashSet<ChangedKey>>,
    pub tracks_retention_age: bool,
    pub tracks_retention_hash_partitions: bool,
}
