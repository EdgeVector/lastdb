//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

impl MutationManager {
    /// Single-route sibling that can secure a durable delete's cloud intent.
    pub async fn write_mutations_with_access_receipt_cloud(
        &self,
        mutations: Vec<Mutation>,
        access_context: &crate::access::AccessContext,
        cloud_policy: CloudCapturePolicy,
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
        self.write_mutations_batch_with_receipt_cloud(
            mutations,
            resolved_prefix.as_deref(),
            cloud_policy,
        )
        .await
    }

    // lint:fn-size-ok verbatim move from write.rs; splitting this function is separate work
    pub(super) async fn write_mutations_batch_with_receipt_cloud_admitted(
        &self,
        mut mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        cloud_policy: CloudCapturePolicy,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
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
        #[cfg(not(feature = "cloud-sync"))]
        let cloud_mutation_uuid = mutations
            .iter()
            .find_map(|mutation| (!mutation.uuid.is_empty()).then(|| mutation.uuid.clone()))
            .unwrap_or_default();
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
        let author_clock_barrier =
            author_clock_persist.map(|(reservation, state)| reservation.submit_with_barrier(state));
        // One clock value per caller batch, so the first locally authored
        // mutation carries the whole batch's committed revision.
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
        let result = {
            #[cfg(feature = "cloud-sync")]
            {
                // `sync_capture` contains only envelope encode and bounded queue
                // admission. Marker and log storage run after acknowledgment.
                let encode_started = std::time::Instant::now();
                let mut envelopes =
                    crate::sync::mutation_intent::encode_mutations(&mutations, storage_prefix);
                // An atom id in the envelope is what authorizes
                // `strip_sot_field_values` to drop the inline body, so an id for a
                // field this pipeline will never store is a value the log throws
                // away and the seal can never read back. Keep only the fields
                // `prepare_atoms_and_key_values` will actually create an atom for.
                crate::sync::mutation_intent::retain_persisted_field_atom_uuids(
                    &mut envelopes,
                    &mutations,
                    |schema_name| {
                        self.schema_manager
                            .get_schema_metadata(schema_name)
                            .ok()
                            .flatten()
                            .map(|schema| schema.runtime_fields.keys().cloned().collect())
                    },
                );
                crate::request_phases::add_phase(
                    crate::request_phases::RequestPhase::SyncCapture,
                    encode_started.elapsed(),
                );
                crate::sync::capture::capture_logical_commit_with_policy_and_author_clock(
                    self.capture_router(),
                    envelopes,
                    cloud_policy,
                    author_clock_barrier.clone(),
                    self.write_with_aggregate_invalidations(
                        mutations,
                        storage_prefix,
                        WriteOrigin::Request,
                        author_clock_barrier,
                    ),
                )
                .await
            }
            #[cfg(not(feature = "cloud-sync"))]
            {
                let cloud = (!matches!(cloud_policy, CloudCapturePolicy::Async)).then(|| {
                    CloudMutationReceipt::unavailable(
                        cloud_mutation_uuid,
                        "cloud sync capture is unavailable in this build",
                    )
                });
                self.write_with_aggregate_invalidations(
                    mutations,
                    storage_prefix,
                    WriteOrigin::Request,
                    author_clock_barrier,
                )
                .await
                .map(|receipt| (receipt, cloud))
            }
        };
        let (mut receipt, cloud) = result?;
        // Event append is durable before this method returns. Clear only after
        // the event succeeds; a clear failure leaves a conservative marker.
        // With source events disabled the scope list is empty and this call
        // performs no write and no flush.
        self.record_attribution_scopes(&attribution_scopes).await?;
        receipt.revision = revision;
        receipt.cloud = cloud;
        Ok(receipt)
    }
}
