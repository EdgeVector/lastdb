//! Live schema resolution with stable proposal IDs and explicit shared-surface publishing.
use crate::SchemaServiceClient;
use schema_service_core::schema_resolver_abi::ResolverDecision;
use schema_service_core::types::{
    SchemaResolveOutcome, SchemaResolveProposal, SchemaResolveResponse, SchemaResolveResult,
};
use schema_service_core::{
    validate_shared_surface_request, SharedSurfaceMetadata, SharedSurfacePublishAttachRequest,
    SharedSurfaceValidationError,
};
use schema_types::{FoldDbError, FoldDbResult, Schema, SchemaType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
mod types;
pub use types::*;
mod helpers;
pub use helpers::match_live_result;
use helpers::{novel_result, now_rfc3339};
mod publish;

/// Resolves through the live service and preserves caller proposal identity.
pub struct LiveSchemaResolver {
    live: SchemaServiceClient,
}
impl LiveSchemaResolver {
    pub fn new(live: SchemaServiceClient) -> Self {
        Self { live }
    }
    /// Resolve a batch through the live service, preserving proposal identity.
    pub async fn resolve(
        &self,
        proposals: Vec<FacadeProposal>,
    ) -> FoldDbResult<Vec<FacadeResolveItem>> {
        if proposals.is_empty() {
            return Ok(Vec::new());
        }
        for p in &proposals {
            if p.proposal_id.trim().is_empty() {
                return Err(FoldDbError::Config(
                    "facade proposal_id must be non-empty".into(),
                ));
            }
        }

        self.resolve_live_only(proposals).await
    }
    async fn resolve_live_only(
        &self,
        proposals: Vec<FacadeProposal>,
    ) -> FoldDbResult<Vec<FacadeResolveItem>> {
        let wire = proposals.iter().map(|p| p.proposal.clone()).collect();
        let live_map = self.live.resolve_schemas(None, wire).await?;
        let mut claimed = HashSet::new();
        let mut out = Vec::with_capacity(proposals.len());
        for p in proposals {
            let live = match_live_result(
                &live_map,
                &p.proposal_id,
                &p.proposal.descriptive_name,
                &mut claimed,
            )
            .cloned()
            .unwrap_or_else(novel_result);
            out.push(FacadeResolveItem {
                proposal_id: p.proposal_id,
                result: live,
                path: FacadePath::Live,
                local_decision: None,
                fallback_reason: Some("live_only"),
                pack_format_version: None,
                config_format_version: None,
            });
        }
        Ok(out)
    }
}
