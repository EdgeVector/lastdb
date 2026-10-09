//! Explicit shared-surface publish/attach for the facade.

use super::*;

impl<S, E, L> LocalFirstSchemaResolver<S, E, L>
where
    S: ResolverPackObjectStore + Send + Sync + 'static,
    E: Embedder + Send + Sync + 'static,
    L: LiveSchemaGateway,
{
    /// Explicit shared-surface publish/attach.
    // lint:fn-size-ok moved verbatim from its original module
    pub async fn publish_attach(
        &self,
        req: SharedSurfacePublishRequest,
    ) -> FoldDbResult<SharedSurfacePublishResult> {
        validate_shared_surface_request(&req.request)
            .map_err(|e: SharedSurfaceValidationError| FoldDbError::Config(e.to_string()))?;

        if req.descriptive_name.trim().is_empty() {
            return Err(FoldDbError::Config(
                "descriptive_name must be non-empty".into(),
            ));
        }
        if req.fields.is_empty() || req.fields.iter().any(|f| f.trim().is_empty()) {
            return Err(FoldDbError::Config(
                "fields must be non-empty without blank names".into(),
            ));
        }

        let local_schema_id = req.request.local_schema_id.clone();
        let proposal = SchemaResolveProposal {
            descriptive_name: req.descriptive_name.clone(),
            fields: req.fields.clone(),
            field_descriptions: req.field_descriptions.clone(),
            purpose_statement: req.purpose_statement.clone(),
            identity_hash: req.local_identity_hash.clone(),
            owner_app_id: Some(req.request.surface.owner_app_id.clone()),
        };

        let items = self
            .resolve(vec![FacadeProposal {
                proposal_id: local_schema_id.clone(),
                proposal: proposal.clone(),
            }])
            .await?;
        let item = items
            .into_iter()
            .next()
            .ok_or_else(|| FoldDbError::Config("facade returned empty resolve batch".into()))?;

        // Enforce local reuse → attach without register.
        if matches!(self.mode, LocalFirstMode::EnforceExistingOnly)
            && matches!(item.path, FacadePath::Local)
            && item.result.outcome == SchemaResolveOutcome::Reuse
        {
            let hash = item.result.matched_shared_schema_hash.clone();
            let attachment = SharedSurfaceAttachmentRecord {
                local_schema_id: local_schema_id.clone(),
                shared_schema_hash: hash.clone(),
                surface: req.request.surface.clone(),
                attached_at: now_rfc3339(),
                source: "local_reuse".into(),
            };
            return Ok(SharedSurfacePublishResult {
                local_schema_id,
                shared_schema_hash: hash,
                outcome: SharedSurfacePublishOutcome::AttachedExisting,
                path: item.path,
                attachment,
            });
        }

        // Shadow / live / enforce-fallback: use live resolve outcome.
        match item.result.outcome {
            SchemaResolveOutcome::Reuse => {
                let hash = item.result.matched_shared_schema_hash.clone();
                let attachment = SharedSurfaceAttachmentRecord {
                    local_schema_id: local_schema_id.clone(),
                    shared_schema_hash: hash.clone(),
                    surface: req.request.surface.clone(),
                    attached_at: now_rfc3339(),
                    source: "live_resolve".into(),
                };
                Ok(SharedSurfacePublishResult {
                    local_schema_id,
                    shared_schema_hash: hash,
                    outcome: SharedSurfacePublishOutcome::AttachedExisting,
                    path: item.path,
                    attachment,
                })
            }
            SchemaResolveOutcome::CandidateEquivalent | SchemaResolveOutcome::Refresh => {
                let hash = item
                    .result
                    .matched_shared_schema_hash
                    .clone()
                    .or_else(|| item.result.candidate_shared_schema_hashes.first().cloned());
                let attachment = SharedSurfaceAttachmentRecord {
                    local_schema_id: local_schema_id.clone(),
                    shared_schema_hash: hash.clone(),
                    surface: req.request.surface.clone(),
                    attached_at: now_rfc3339(),
                    source: "live_resolve".into(),
                };
                Ok(SharedSurfacePublishResult {
                    local_schema_id,
                    shared_schema_hash: hash,
                    outcome: SharedSurfacePublishOutcome::NeedsHumanOrLiveCreate,
                    path: item.path,
                    attachment,
                })
            }
            SchemaResolveOutcome::Novel => {
                // Live register with shared_surface metadata.
                let mut schema = Schema::new(
                    local_schema_id.clone(),
                    SchemaType::Single,
                    None,
                    Some(req.fields.clone()),
                    None,
                    None,
                );
                schema.descriptive_name = Some(req.descriptive_name.clone());
                schema.purpose_statement = req.purpose_statement.clone();
                schema.field_descriptions = req.field_descriptions.clone();
                schema.owner_app_id = Some(req.request.surface.owner_app_id.clone());
                if let Some(h) = &req.local_identity_hash {
                    schema.identity_hash = Some(h.clone());
                }

                match self
                    .live
                    .add_shared_schema(
                        &schema,
                        req.request.surface.clone(),
                        "shared_surface_publish",
                        item.fallback_reason,
                    )
                    .await
                {
                    Ok(response) => {
                        let hash = response
                            .schema
                            .get_identity_hash()
                            .cloned()
                            .or_else(|| Some(response.schema.name.clone()));
                        let attachment = SharedSurfaceAttachmentRecord {
                            local_schema_id: local_schema_id.clone(),
                            shared_schema_hash: hash.clone(),
                            surface: req.request.surface.clone(),
                            attached_at: now_rfc3339(),
                            source: "live_register".into(),
                        };
                        Ok(SharedSurfacePublishResult {
                            local_schema_id,
                            shared_schema_hash: hash,
                            outcome: SharedSurfacePublishOutcome::RegisteredLive,
                            path: item.path,
                            attachment,
                        })
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "shared surface live register failed");
                        let attachment = SharedSurfaceAttachmentRecord {
                            local_schema_id: local_schema_id.clone(),
                            shared_schema_hash: None,
                            surface: req.request.surface.clone(),
                            attached_at: now_rfc3339(),
                            source: "live_register".into(),
                        };
                        Ok(SharedSurfacePublishResult {
                            local_schema_id,
                            shared_schema_hash: None,
                            outcome: SharedSurfacePublishOutcome::Rejected,
                            path: item.path,
                            attachment,
                        })
                    }
                }
            }
        }
    }
}
