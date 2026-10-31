//! Durable attribution roots for accepted mutations.

use crate::db_operations::AttributionPendingScope;
use crate::schema::SchemaError;

use super::attribution::{attribution_events, live_write_attribution_path};
use super::MutationManager;

impl MutationManager {
    /// Persist the durable attribution root for a batch, then append its
    /// source events and clear the pending-scope markers.
    ///
    /// This runs once per mutation batch, from its one call site: the live
    /// request write (`write_mutations_batch_with_receipt_admitted`). No-op
    /// when source events are disabled or the batch carries no attribution
    /// scopes.
    pub(super) async fn record_attribution_scopes(
        &self,
        attribution_scopes: &[AttributionPendingScope],
    ) -> Result<(), SchemaError> {
        if attribution_scopes.is_empty() {
            return Ok(());
        }
        // One durable path row per written object, before the event append's
        // trailing flush — so a caller that observes the response also has a
        // durable attribution root for what it just wrote, not only a source
        // event that a later batch walk has yet to classify.
        for scope in attribution_scopes {
            self.db_ops
                .attribution()
                .put_attribution_path(&live_write_attribution_path(scope))
                .await?;
        }
        let mutation_ids = attribution_scopes
            .iter()
            .map(|scope| scope.mutation_id.clone())
            .collect::<Vec<_>>();
        self.db_ops
            .attribution()
            .append_events_and_clear_pending_scopes(
                attribution_events(attribution_scopes),
                &mutation_ids,
            )
            .await?;
        Ok(())
    }
}
