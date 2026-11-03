//! Boot-time helpers for [`FoldDB::initialize_from_init`].

use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::storage::StorageError;

use super::FoldDB;

impl FoldDB {
    /// Point-load the device clock once at boot. A mutation ACK must not
    /// read this metadata row or flush its namespace on every request.
    pub(super) async fn load_author_clock_state(
        db_ops: &Arc<DbOperations>,
        signer: &Arc<crate::security::Ed25519KeyPair>,
    ) -> Result<crate::schema::types::MutationAuthorClockState, StorageError> {
        let author_clock_state_key = crate::schema::types::author_clock::mutation_author_clock_key(
            &signer.public_key_base64(),
        );
        Ok(db_ops
            .metadata()
            .get_typed::<crate::schema::types::MutationAuthorClockState>(&author_clock_state_key)
            .await
            .map_err(|error| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {error}"
                ))
            })?
            .unwrap_or_default())
    }

    /// Log the process memory budget. Actual native pool charges have their
    /// own status counters.
    pub(super) fn log_memory_budget() {
        crate::memory_budget::process_memory_budget().log_at_boot();
    }
}
