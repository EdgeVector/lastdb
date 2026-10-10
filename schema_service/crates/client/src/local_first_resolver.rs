//! Local-first schema resolve facade for shared-surface publish/attach.
//!
//! Modes:
//! - [`LocalFirstMode::Shadow`] — always call live; compare local native
//!   decisions for telemetry only; return live results unchanged.
//! - [`LocalFirstMode::EnforceExistingOnly`] — return local UseExisting /
//!   UseComponents when the pack policy allows UseLocal; otherwise live.
//! - [`LocalFirstMode::LiveOnly`] — kill switch; never evaluate the pack.
//!
//! Callers always key results by stable `proposal_id`. Live service responses
//! currently key by `descriptive_name`; this facade remaps them.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use schema_service_core::native_schema_resolver::{evaluate_native, NativeResolverInput};
use schema_service_core::resolver_pack::EmbeddingVectorRecord;
use schema_service_core::resolver_pack_consumer::{
    LoadedResolverPack, ResolverPackObjectStore, ResolverPackResolutionRoute,
};
use schema_service_core::schema_resolver_abi::{
    ProposalEmbeddings, ProposalFieldMetadata, ProposalMetadata, RegistryCanonicalFieldHandle,
    RegistryFieldHandle, RegistryMetadata, RegistrySchemaHandle, ResolverDecision, ResolverOutput,
};
use schema_service_core::types::{
    AddSchemaResponse, SchemaResolveOutcome, SchemaResolveProposal, SchemaResolveResponse,
    SchemaResolveResult,
};
use schema_service_core::{
    validate_shared_surface_request, Embedder, NativeResolverState, ResolverRuntime,
    SharedSurfaceMetadata, SharedSurfacePublishAttachRequest, SharedSurfaceValidationError,
};
use schema_types::{FoldDbError, FoldDbResult, Schema, SchemaType};
use serde::{Deserialize, Serialize};

use crate::SchemaServiceClient;

mod modes_types;
pub use modes_types::*;
mod live_gateway;
pub use live_gateway::*;
mod local_eval;
pub use local_eval::*;
mod disagreement;
pub use disagreement::*;
mod enforce;
mod publish;

// ---------------------------------------------------------------------------
// Facade
// ---------------------------------------------------------------------------

/// Local-first schema resolver facade.
pub struct LocalFirstSchemaResolver<S, E, L = SchemaServiceClient>
where
    S: ResolverPackObjectStore + Send + Sync + 'static,
    E: Embedder + Send + Sync + 'static,
    L: LiveSchemaGateway,
{
    live: L,
    runtime: Option<Arc<ResolverRuntime<S>>>,
    /// Optional fixed pack (tests / pre-loaded state without full runtime).
    active_override: Option<Arc<NativeResolverState>>,
    embedder: Arc<E>,
    mode: LocalFirstMode,
    /// Telemetry counter: live resolve batches (no proposal content).
    live_resolve_batches: AtomicU64,
    /// Telemetry counter: live resolve proposal count.
    live_resolve_proposals: AtomicU64,
}

impl<S, E, L> LocalFirstSchemaResolver<S, E, L>
where
    S: ResolverPackObjectStore + Send + Sync + 'static,
    E: Embedder + Send + Sync + 'static,
    L: LiveSchemaGateway,
{
    pub fn new(live: L, embedder: Arc<E>, mode: LocalFirstMode) -> Self {
        Self {
            live,
            runtime: None,
            active_override: None,
            embedder,
            mode,
            live_resolve_batches: AtomicU64::new(0),
            live_resolve_proposals: AtomicU64::new(0),
        }
    }

    pub fn mode(&self) -> LocalFirstMode {
        self.mode
    }

    async fn load_active(&self) -> Option<Arc<NativeResolverState>> {
        if let Some(s) = &self.active_override {
            return Some(Arc::clone(s));
        }
        if let Some(rt) = &self.runtime {
            return rt.active().await;
        }
        None
    }

    /// Resolve a batch of proposals under the configured mode.
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

        let mode = self.mode;
        let active = if mode == LocalFirstMode::LiveOnly {
            None
        } else {
            self.load_active().await
        };

        match mode {
            LocalFirstMode::LiveOnly => self.resolve_live_only(proposals).await,
            LocalFirstMode::Shadow => self.resolve_shadow(proposals, active.as_deref()).await,
            LocalFirstMode::EnforceExistingOnly => {
                self.resolve_enforce(proposals, active.as_deref()).await
            }
        }
    }

    async fn resolve_live_only(
        &self,
        proposals: Vec<FacadeProposal>,
    ) -> FoldDbResult<Vec<FacadeResolveItem>> {
        let live_map = self.call_live(&proposals).await?;
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

    async fn resolve_shadow(
        &self,
        proposals: Vec<FacadeProposal>,
        active: Option<&NativeResolverState>,
    ) -> FoldDbResult<Vec<FacadeResolveItem>> {
        // ALWAYS call live for the full batch first (or after local — card
        // says always call live; order does not affect caller result).
        let mut local_evals: Vec<Option<LocalEvaluateOutcome>> =
            Vec::with_capacity(proposals.len());
        let pack_versions = active.map(|s| {
            (
                s.pack.manifest.format_version,
                s.pack.resolver_config.format_version,
            )
        });

        for p in &proposals {
            let eval = active.and_then(|state| {
                match evaluate_local_proposal(
                    &state.pack,
                    self.embedder.as_ref(),
                    &p.proposal_id,
                    &p.proposal,
                ) {
                    Ok(o) => Some(o),
                    Err(e) => {
                        tracing::debug!(
                            proposal_id = %p.proposal_id,
                            error = %e,
                            "local evaluate failed in shadow; treating as miss"
                        );
                        None
                    }
                }
            });
            local_evals.push(eval);
        }

        let live_map = self.call_live(&proposals).await?;
        let mut claimed = HashSet::new();
        let mut out = Vec::with_capacity(proposals.len());

        for (p, local) in proposals.into_iter().zip(local_evals) {
            let live = match_live_result(
                &live_map,
                &p.proposal_id,
                &p.proposal.descriptive_name,
                &mut claimed,
            )
            .cloned()
            .unwrap_or_else(novel_result);

            let local_summary = local.as_ref().map(local_shadow_summary);
            let disagreement = classify_disagreement(local_summary.as_ref(), &live);
            let local_decision = local.as_ref().map(|o| o.output.decision);

            out.push(FacadeResolveItem {
                proposal_id: p.proposal_id,
                result: live.clone(),
                path: FacadePath::Shadow {
                    local: local_summary,
                    live: Box::new(live),
                    disagreement,
                },
                local_decision,
                fallback_reason: None,
                pack_format_version: pack_versions.map(|v| v.0),
                config_format_version: pack_versions.map(|v| v.1),
            });
        }
        Ok(out)
    }
}

// Phantom store for LiveOnly / override-only construction without a runtime.
/// No-op object store used when the facade has no pack runtime.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopPackStore;

#[async_trait]
impl ResolverPackObjectStore for NoopPackStore {
    async fn get_object(
        &self,
        _key: &str,
    ) -> Result<Option<Vec<u8>>, schema_service_core::ResolverPackFetchError> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
