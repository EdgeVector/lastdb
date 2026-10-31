//! Delete barrier and marker cleanup wiring.

use std::collections::HashMap;

use serde_json::Value;

use super::super::delete_barriers::DeleteBarriers;
use super::super::delete_marker_cleanup::MarkerCleanup;
use super::super::ports::DeleteBarrierReads;
use super::super::types::PerKeyRecord;
use super::super::AtomStore;
use crate::atom::delete_barrier::DeleteBarrier;
use crate::schema::SchemaError;

impl DeleteBarrierReads for AtomStore {
    async fn winning_delete_barriers(
        &self,
        molecule_keys: &[String],
    ) -> Result<HashMap<String, DeleteBarrier>, SchemaError> {
        Self::winning_delete_barriers(self, molecule_keys).await
    }
}

impl AtomStore {
    /// Read exact Delete target tips in one batch.
    pub(crate) async fn delete_target_tips(
        &self,
        molecule_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        DeleteBarriers::new(self)
            .delete_target_tips(molecule_keys)
            .await
    }

    /// The exact base/generation winner, without its Delete barrier filter.
    pub(crate) async fn delete_target_tips_with_generations(
        &self,
        molecule_keys: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        DeleteBarriers::new(self)
            .delete_target_tips_with_generations(molecule_keys, storage_prefix)
            .await
    }

    /// Exact Delete winners in one batch, while the caller holds the tip locks.
    pub(crate) async fn winning_delete_barriers(
        &self,
        molecule_keys: &[String],
    ) -> Result<HashMap<String, DeleteBarrier>, SchemaError> {
        DeleteBarriers::new(self)
            .winning_delete_barriers(molecule_keys)
            .await
    }

    /// The barrier winners and eligible target tips for Delete convergence.
    pub(crate) async fn delete_converge_targets(
        &self,
        planned: &[DeleteBarrier],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(DeleteBarrier, Option<PerKeyRecord>)>, SchemaError> {
        DeleteBarriers::new(self)
            .delete_converge_targets(planned, storage_prefix)
            .await
    }

    /// One page of delete-marker cleanup for one molecule. See
    /// `MarkerCleanup::cleanup_delete_markers_page`.
    pub async fn cleanup_delete_markers_page(
        &self,
        molecule: &str,
        scope: Option<&str>,
        after_key: Option<&str>,
        limit: usize,
        execute: bool,
    ) -> Result<Value, SchemaError> {
        MarkerCleanup::new(self)
            .cleanup_delete_markers_page(molecule, scope, after_key, limit, execute)
            .await
    }
}
