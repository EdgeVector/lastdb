//! # FoldDB Core Library
//!
//! This library implements the core database functionality of the Fold distributed data platform.
//! It provides schema-based data storage and query with distributed networking capabilities.
//!
//! ## Core Components
//!
//! * `atom` - Atomic data storage units that form the foundation of the database
//! * `db_operations` - Database operation handlers
//! * `error` - Error types and handling
//! * `fold_db_core` - Core database functionality
//! * `schema` - Schema definition, validation, and execution
//! * `security` - Cryptographic key management and signing
//! * `storage` - Storage backend abstraction (Last Store)
//!
//! ## Architecture
//!
//! Fold uses a distributed architecture where each node can store and process data
//! according to defined schemas. Nodes can communicate with each other to share and
//! replicate data, with permissions controlling access to different schemas and operations.
//!
//! The system is built around the concept of schemas that define the structure of data
//! and the operations that can be performed on it.

pub mod access;
/// Atom and molecule model types from `lastdb_model`, plus core clock stamps.
pub mod atom;
// Pure model modules. They moved to `lastdb_model`; the old paths stay.
pub use lastdb_model::{canonical, constants, hex, json_order, kind_partition};

/// Core logical memory, record retention, and background persist work.
pub mod resident;
pub use resident::{PersistLaneMetrics, PersistLanePressure};
pub mod clock;
pub mod crypto;
pub mod db_operations;
pub mod durable_flush;
mod env_number;
pub mod error;
pub mod fold_db_core;
/// One accounted process memory budget — the warm set limit projected to RSS,
/// with the deferred-write window derived from what is left under the node's
/// RSS guard.
pub mod memory_budget;
pub mod mini_cutover;
#[cfg(unix)]
pub mod process_cpu;
pub mod read_receipts;
/// On-disk record formats of the typed stores (model layer).
pub(crate) mod records;
pub mod request_phases;
pub mod schema;
pub mod security;
pub mod sharing;
pub mod storage;
pub mod sync;
pub mod sync_conflict;
pub mod user_context;
mod write_preparation;

// Re-export main types for convenience
pub use error::{FoldDbError, FoldDbResult};
pub use fold_db_core::{
    FoldDB, HashRangeWatch, HashRangeWatchBounds, HashRangeWatchError, HashRangeWatchEvent,
};

// Re-export schema types
pub use schema::types::operations::MutationType;
pub use schema::SchemaState;

// Re-export storage types
pub use storage::DatabaseConfig;
pub use sync_conflict::SyncConflict;

pub mod alternate_keys;
