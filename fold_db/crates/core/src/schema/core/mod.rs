use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use crate::schema::field_mapper::FieldMapperService;
use crate::schema::types::Schema;
use crate::schema::SchemaState;

mod cache;
mod declare;
mod load;
mod locks;
mod state;

/// Core schema management system that combines schema interpretation, validation, and management.
///
/// SchemaCore is responsible for:
/// - Loading and validating schemas from JSON
/// - Managing schema storage and persistence
/// - Handling schema field mappings
/// - Providing schema access and validation services
///
/// This unified component simplifies the schema system by combining the functionality
/// previously split across SchemaManager and SchemaInterpreter.
pub struct SchemaCore {
    /// Storage for loaded schemas.
    ///
    /// An `RwLock`, not a `Mutex`, on purpose: the query read path resolves the
    /// schema for *every* read (`get_schema_following_supersession_for_read` →
    /// `clone_for_read_shared`), so a `Mutex` here serialized all concurrent
    /// reads through one exclusive critical section — a lock convoy. Even though
    /// each read only clones the molecule-stripped schema (cheap, O(field
    /// count)), `Mutex` forced N readers to take turns, capping concurrent
    /// read throughput at ~1 core's worth regardless of how many cores are
    /// free. With an `RwLock` the hot path takes a shared `.read()` lock and the
    /// (cheap, `&self`) `clone_for_read_shared` runs in parallel across readers;
    /// only the rare schema mutations (load/insert/remove) take `.write()`.
    schemas: Arc<RwLock<HashMap<String, Schema>>>,
    /// Storage for all schemas known to the system and their load state
    schema_states: Arc<Mutex<HashMap<String, SchemaState>>>,
    /// Maps blocked/superseded schema names to their replacement schema names
    superseded_by: Arc<Mutex<HashMap<String, String>>>,
    /// Schema names whose claim on their `descriptive_name` is retired.
    ///
    /// A member is still `Available` and still resolves by canonical name and
    /// by identity hash. It is only dropped from the candidate set when a
    /// caller names a schema by `descriptive_name`, so a rekey predecessor
    /// stops competing for the readable name without breaking the by-hash pins
    /// that address it. Durable in the node-local `schema_states` namespace;
    /// see `SchemaStore::set_schema_name_claim_retired`.
    retired_name_claims: Arc<Mutex<HashSet<String>>>,
    /// Unified database operations with storage abstraction
    db_ops: std::sync::Arc<crate::db_operations::DbOperations>,
    /// Domain service that applies FieldMapper entries during schema expansion.
    field_mapper: FieldMapperService,
    /// Set once the initial store → memory catalog load finishes.
    ///
    /// `GET /api/schemas` must not answer `{count:0, ok:true}` while this is
    /// false — clients (and the safe-upgrade smoke bar) treat empty-ok as a
    /// finished empty brain, which false-REDs a hydrating post-rekey home.
    catalog_ready: AtomicBool,
    /// Schema name → the exact multi-key binding set this process has already
    /// established durably, for [`Self::apply_field_hash_coherence_on_load`].
    ///
    /// The pass's name says "on load", but its only caller is
    /// `load_schema_internal`, and the mutation write path calls that after
    /// **every write** to refresh the in-memory cache. So a step designed to run
    /// once per schema registration was running once per mutation, and its
    /// binder had no already-bound fast path: in the steady state each shared
    /// field still cost several durable protein reads plus an unconditional
    /// `fldprot:` breadcrumb rewrite. Measured on the primary 2026-08-17, that
    /// made `schema_reload` 2162ms/mutation on `BoardCards_hashrange_v1` (24
    /// fields, has multi-key siblings) against 3.8ms/mutation on `Card` (no
    /// sibling, so its binding set is empty and it returned early) — 25% of all
    /// time that schema's mutations spent in the node.
    ///
    /// Keyed on the binding tuples themselves, NOT on the schema's identity
    /// hash: the tuples are exactly what the durable work depends on, so a
    /// changed molecule UUID or a newly-matched peer invalidates the memo
    /// without needing a separate rule about what else should.
    ///
    /// **Why a per-schema memo is still complete when a NEW sibling appears
    /// later.** Binding is symmetric — `bind_cross_key_field_protein` joins both
    /// molecules to one protein regardless of which side initiated. A schema
    /// registered after this one runs its own pass, finds this one in the peer
    /// scan, and binds the pair. So a memo hit here can never be the only path
    /// to a binding that ought to exist.
    ///
    /// Process-local and deliberately not persisted: a restart re-establishes it
    /// on the first load of each schema, which is the one-time cost the pass was
    /// always meant to be.
    coherence_bound: Arc<Mutex<HashMap<String, Vec<CoherenceBinding>>>>,
}

/// One durable multi-key binding, as [`SchemaCore::apply_field_hash_coherence_on_load`]
/// computes it. Compared as a whole to decide whether the pass has anything
/// left to do — see [`SchemaCore::coherence_bound`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoherenceBinding {
    pub(crate) field: String,
    pub(crate) field_hash: String,
    pub(crate) peer_molecule: String,
    pub(crate) peer_layout: crate::schema::field_hash_coherence::KeyLayoutFingerprint,
}

/// SchemaCore is THE schema owner; the resident graph's schema half stays a
/// test/fidelity surface. This impl is the sanctioned bridge: a resident
/// schema miss loads from the catalog instead of growing a second cache.
impl crate::resident::SchemaLoader for SchemaCore {
    fn load_schema(&self, name: &str) -> Result<Option<Schema>, String> {
        Ok(self
            .schemas
            .read()
            .map_err(|e| format!("schema catalog lock poisoned: {e}"))?
            .get(name)
            .cloned())
    }
}
