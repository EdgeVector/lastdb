//! Moved verbatim out of the parent module; see the parent for context.

use super::*;
use crate::schema::types::MutationType;

impl MutationManager {
    /// Write mutations with local caller context.
    ///
    /// Local callers approved by the host consent layer get full local DB write
    /// access. Trust tiers, capability quotas, payment gates, and namespace walls
    /// no longer gate this path (consent-only local control).
    ///
    /// When `access_context.storage_prefix` is set (org multi-DB handle from
    /// `X-LastDB-Db`), record keys land under `{prefix}:…` so cohabiting DBs
    /// on one Mini node stay isolated.
    pub async fn write_mutations_with_access_receipt(
        &self,
        mutations: Vec<Mutation>,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let mut resolved_prefix = None;
        let mut schema_names: Vec<&str> =
            mutations.iter().map(|m| m.schema_name.as_str()).collect();
        schema_names.sort_unstable();
        schema_names.dedup();
        for schema_name in schema_names {
            let prefix = self
                .db_ops
                .db_catalog()
                .resolve_storage_prefix(
                    access_context.db_locator.as_deref(),
                    schema_name,
                    access_context.storage_prefix.as_deref(),
                )
                .await?;
            if let Some(existing) = &resolved_prefix {
                if existing != &prefix {
                    return Err(SchemaError::InvalidData(
                        "one mutation batch resolved to multiple catalog instances; split the batch by schema instance"
                            .to_string(),
                    ));
                }
            } else {
                resolved_prefix = Some(prefix);
            }
        }
        let resolved_prefix = resolved_prefix.flatten();
        self.write_mutations_batch_with_receipt(mutations, resolved_prefix.as_deref())
            .await
    }

    pub(super) async fn write_mutations_batch_with_receipt_admitted(
        &self,
        mut mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.validate_mutation_batch(&mutations)?;
        let logical_admission = self
            .admit_logical_capture(&mutations, storage_prefix)
            .await?;
        // This marker is the conservative bridge between the product write and
        // the attribution event. If a crash lands between them, a later walk
        // sees the marker and blocks reclaim for this exact source scope.
        let attribution_scopes = if attribution_source_events_enabled() {
            attribution_scopes(&mutations, storage_prefix)?
        } else {
            Vec::new()
        };
        self.db_ops
            .attribution()
            .begin_pending_scopes(&attribution_scopes)
            .await?;
        // Which mutations arrive WITHOUT a clock is the only signal that says
        // which ones this node is authoring, and stamping erases it. Capture it
        // first so the receipt reports the counter this batch allocated rather
        // than an imported peer's, which would name a revision no local read
        // can be checked against.
        let authored_locally: Vec<bool> = mutations
            .iter()
            .map(|mutation| mutation.imported_written_at.is_none())
            .collect();
        let wait_for_author_clock = mutations
            .iter()
            .any(|mutation| mutation.synchronous == Some(true));
        let author_clock_persist = self.prepare_mutation_author_clocks(&mut mutations)?;
        // Submit before resident apply. Each data envelope waits on a clone of
        // this barrier, so durable LWW data cannot pass its durable clock.
        let inline_clock = self.db_ops.namespaced_store().disk_pack_runtime().is_some()
            && mutations.iter().all(|mutation| {
                matches!(
                    mutation.mutation_type,
                    MutationType::Create | MutationType::Update
                )
            });
        let author_clock_barrier = author_clock_persist.and_then(|(reservation, state)| {
            if inline_clock {
                drop(reservation);
                None
            } else {
                Some(reservation.submit_with_barrier(state))
            }
        });
        // One logical counter per caller batch, so its first local mutation
        // carries the common revision. Per-item author times preserve order.
        let revision = authored_locally
            .iter()
            .zip(mutations.iter())
            .find(|(authored, _)| **authored)
            .map(|(_, mutation)| mutation.logical_counter);
        // A synchronous receipt includes the author clock. Persist it before
        // data, so a process loss cannot reopen below a durable LWW value.
        // A later data failure leaves a harmless counter gap.
        if wait_for_author_clock {
            if let Some(barrier) = &author_clock_barrier {
                barrier.wait().await?;
            }
        }
        let mut receipt = crate::fold_db_core::admission::with_mutation_admission(
            self.capture_router(),
            super::super::logical_capture::with_admission(
                logical_admission,
                self.write_mutations_batch_inner_with_clock(
                    mutations,
                    storage_prefix,
                    author_clock_barrier,
                ),
            ),
        )
        .await?;
        // Event append is durable before this method returns. Clear only after
        // the event succeeds; a clear failure leaves a conservative marker.
        // With source events disabled the scope list is empty and this call
        // performs no write and no flush.
        self.record_attribution_scopes(&attribution_scopes).await?;
        receipt.revision = revision;
        Ok(receipt)
    }
}
