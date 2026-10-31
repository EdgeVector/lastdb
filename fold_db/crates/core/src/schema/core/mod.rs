use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use crate::schema::types::Schema;
use crate::schema::SchemaState;

mod cache;
mod load;
mod locks;
mod read_snapshot;
mod staged_cache;
mod state;

/// Core schema management system that combines schema interpretation, validation, and management.
///
/// SchemaCore is responsible for:
/// - Loading and validating schemas
/// - Managing schema storage and persistence
/// - Handling schema field mappings
/// - Providing schema access and validation services
///
/// This unified component simplifies the schema system by combining the functionality
/// previously split across SchemaManager and SchemaInterpreter.
pub struct SchemaCore {
    /// Management definitions shared with field mapping and schema declaration.
    /// Native data calls resolve definitions through their captured core view.
    /// Private reloads stage changes before publication of this management map.
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
    /// Set once the initial store → memory catalog load finishes.
    ///
    /// `GET /api/schemas` must not answer `{count:0, ok:true}` while this is
    /// false — clients (and the safe-upgrade smoke bar) treat empty-ok as a
    /// finished empty brain, which false-REDs a hydrating post-rekey home.
    catalog_ready: AtomicBool,
}
