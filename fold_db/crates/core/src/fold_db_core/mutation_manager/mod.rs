//! Mutation Manager — handles all mutation operations.
//!
//! Layout:
//! - [`write`] — public write entry points and the record batch pipeline
//! - [`key_values`] — primary keys and declared field values of a mutation
//! - [`cas_rules`] — whole-record write preconditions
//! - [`receipt`] — what one logical resident commit answers to its caller
//! - [`index`] — best-effort native-index side effects

mod cas_rules;
mod index;
mod key_values;
pub mod portable_checkpoint;
mod portable_definitions;
mod portable_journal;
pub mod portable_replay;
pub mod portable_schema;
pub mod receipt;
mod write;

use std::path::PathBuf;
use std::sync::Arc;

pub use receipt::{
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
};

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;

/// Manages mutation operations for the FoldDB system
pub struct MutationManager {
    /// Database operations for persistence
    db_ops: Arc<DbOperations>,
    /// Schema manager for schema operations
    schema_manager: Arc<SchemaCore>,
    /// Signing keypair for the author clock
    signer: Arc<crate::security::Ed25519KeyPair>,
    /// Explicit Search app inbox for this database instance. Daemon boots pass
    /// this so two `Host::boot` calls in one process do not race through
    /// process-global `LASTDB_HOME`.
    search_outbox_inbox: Option<PathBuf>,
    /// Resident high-water author clock for this device. Boot point-loads the
    /// durable state once; requests advance and sign in memory and persist the
    /// high-water state atomically in the native write batch.
    author_clock_state: std::sync::Mutex<crate::schema::types::MutationAuthorClockState>,
}

impl MutationManager {
    /// Creates a new MutationManager instance
    pub fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        search_outbox_inbox: Option<PathBuf>,
        author_clock_state: crate::schema::types::MutationAuthorClockState,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            signer,
            search_outbox_inbox,
            author_clock_state: std::sync::Mutex::new(author_clock_state),
        }
    }
}
