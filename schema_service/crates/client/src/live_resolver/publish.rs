//! Explicit shared-surface publish/attach for the facade.

use super::*;

impl LiveSchemaResolver {
    /// Explicit shared-surface publish/attach.
    pub async fn publish_attach(
        &self,
        req: SharedSurfacePublishRequest,
    ) -> FoldDbResult<SharedSurfacePublishResult> {
        validate_publish_request(&req)?;

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
                proposal,
            }])
            .await?;
        let item = items
            .into_iter()
            .next()
            .ok_or_else(|| FoldDbError::Config("facade returned empty resolve batch".into()))?;

        match item.result.outcome {
            SchemaResolveOutcome::Reuse => {
                let hash = item.result.matched_shared_schema_hash.clone();
                Ok(publish_result(
                    &req,
                    hash,
                    "live_resolve",
                    SharedSurfacePublishOutcome::AttachedExisting,
                    item.path,
                ))
            }
            SchemaResolveOutcome::CandidateEquivalent | SchemaResolveOutcome::Refresh => {
                let hash = item
                    .result
                    .matched_shared_schema_hash
                    .clone()
                    .or_else(|| item.result.candidate_shared_schema_hashes.first().cloned());
                Ok(publish_result(
                    &req,
                    hash,
                    "live_resolve",
                    SharedSurfacePublishOutcome::NeedsHumanOrLiveCreate,
                    item.path,
                ))
            }
            SchemaResolveOutcome::Novel => {
                self.publish_novel_live(&req, item.path, item.fallback_reason)
                    .await
            }
        }
    }

    /// Live register with shared_surface metadata for a novel schema.
    async fn publish_novel_live(
        &self,
        req: &SharedSurfacePublishRequest,
        path: FacadePath,
        fallback_reason: Option<&'static str>,
    ) -> FoldDbResult<SharedSurfacePublishResult> {
        let mut schema = Schema::new(
            req.request.local_schema_id.clone(),
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
            .add_schema_with_shared_surface(
                &schema,
                HashMap::new(),
                req.request.surface.clone(),
                "shared_surface_publish",
                fallback_reason,
            )
            .await
        {
            Ok(response) => {
                let hash = response
                    .schema
                    .get_identity_hash()
                    .cloned()
                    .or_else(|| Some(response.schema.name.clone()));
                Ok(publish_result(
                    req,
                    hash,
                    "live_register",
                    SharedSurfacePublishOutcome::RegisteredLive,
                    path,
                ))
            }
            Err(e) => {
                tracing::warn!(error = %e, "shared surface live register failed");
                Ok(publish_result(
                    req,
                    None,
                    "live_register",
                    SharedSurfacePublishOutcome::Rejected,
                    path,
                ))
            }
        }
    }
}

fn validate_publish_request(req: &SharedSurfacePublishRequest) -> FoldDbResult<()> {
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
    Ok(())
}

fn publish_result(
    req: &SharedSurfacePublishRequest,
    hash: Option<String>,
    source: &str,
    outcome: SharedSurfacePublishOutcome,
    path: FacadePath,
) -> SharedSurfacePublishResult {
    let local_schema_id = req.request.local_schema_id.clone();
    let attachment = SharedSurfaceAttachmentRecord {
        local_schema_id: local_schema_id.clone(),
        shared_schema_hash: hash.clone(),
        surface: req.request.surface.clone(),
        attached_at: now_rfc3339(),
        source: source.into(),
    };
    SharedSurfacePublishResult {
        local_schema_id,
        shared_schema_hash: hash,
        outcome,
        path,
        attachment,
    }
}
