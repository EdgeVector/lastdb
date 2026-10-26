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
//! * `storage` - Storage backend abstraction (Last Store, with optional cloud sync)
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
pub mod atom;
/// Protein — UUID'd molecule set with bi-directional membership.
pub mod protein;
pub use protein::{Protein, ProteinMember, PROTEIN_SCHEMA_MARKER};
/// Core logical memory, record retention, and background persist work.
pub mod resident;
pub use resident::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneMetrics,
    PersistLaneOccupancy, PersistLanePressure, PersistLaneSet, PersistReservation,
};
/// Age-based durability health, evaluated with no sync engine running.
/// Sealed-chunk backup progress math (no network).
pub mod backup_progress;
pub mod canonical;
pub mod clock;
pub mod constants;
pub mod crypto;
pub mod db_operations;
pub mod durable_flush;
pub mod error;
pub mod error_context;
pub mod fold_db_core;
pub mod hex;
pub mod json_order;
pub mod kind_partition;
/// One accounted process memory budget — warm set + key cache + resident graph
/// projected to RSS, with the deferred-write window derived from what is left
/// under the node's RSS guard.
pub mod memory_budget;
pub mod mini_cutover;
#[cfg(unix)]
pub mod process_cpu;
pub mod read_receipts;
pub mod request_phases;
pub mod schema;
pub mod security;
pub mod sharing;
pub mod storage;
pub mod sync;
pub mod sync_conflict;
pub mod user_context;
/// Schema-owner class carried into the warm set across the blocking boundary.
pub mod warm_admit;

// Re-export main types for convenience
pub use error::{FoldDbError, FoldDbResult};
pub use error_context::{ContextError, ResultExt};
pub use fold_db_core::{
    FoldDB, HashRangeWatch, HashRangeWatchBounds, HashRangeWatchError, HashRangeWatchEvent,
};

// Re-export schema types
pub use schema::types::operations::MutationType;
pub use schema::SchemaState;

// Re-export storage types
pub use storage::DatabaseConfig;
pub use sync_conflict::SyncConflict;
