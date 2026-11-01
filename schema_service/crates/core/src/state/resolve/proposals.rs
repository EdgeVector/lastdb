use serde_json::Value;

use super::*;

/// The schema-resolve result both HTTP routers answer.
#[derive(Debug)]
pub enum SchemaResolveReply {
    /// The proposals resolved. HTTP 200.
    Ready(SchemaResolveResponse),
    /// The resolve refused the request. HTTP 400.
    ///
    /// `body` is `{ "error": "Schema resolve failed: {detail}" }`.
    Refused { body: Value },
}

impl SchemaResolveReply {
    /// The resolve payload, or the HTTP error.
    ///
    /// Ready stays typed so each router serializes it. Refused is 400.
    pub fn into_http(self) -> Result<SchemaResolveResponse, (u16, Value)> {
        match self {
            Self::Ready(response) => Ok(response),
            Self::Refused { body } => Err((400, body)),
        }
    }
}

impl SchemaServiceState {
    /// Resolve proposed schemas against the current shared registry without
    /// mutating registry state.
    ///
    /// This is the core for `POST /v1/schemas/resolve`. It deliberately avoids
    /// the `add_schema` pipeline, canonical-field registration, app registry
    /// writes, state-version bumps, and telemetry counters. It only reads the
    /// active schema registry and returns the same style of reuse advice that
    /// older batch-check callers consume.
    pub fn resolve_schema_proposals(
        &self,
        request: &SchemaResolveRequest,
    ) -> FoldDbResult<SchemaResolveResponse> {
        if request.proposals.len() > MAX_SCHEMA_RESOLVE_PROPOSALS {
            return Err(FoldDbError::Config(format!(
                "too many schema resolve proposals: {} > {MAX_SCHEMA_RESOLVE_PROPOSALS}",
                request.proposals.len()
            )));
        }

        let registry_version = self.current_state_version();
        let cache_stale = request
            .client_registry_version
            .is_some_and(|client_version| client_version < registry_version);
        let mut results = HashMap::new();

        for proposal in &request.proposals {
            let result = self.resolve_one_proposal(proposal, cache_stale)?;

            results.insert(proposal.descriptive_name.clone(), result);
        }

        Ok(SchemaResolveResponse {
            registry_version,
            cache_stale,
            results,
        })
    }

    /// The same call as [`Self::resolve_schema_proposals`], classified for HTTP.
    ///
    /// A refusal stays HTTP 400. The body carries the prefix. Each router
    /// still writes its response. [`Self::resolve_schema_proposals`] stays public.
    #[must_use]
    pub fn schema_resolve_reply(&self, request: &SchemaResolveRequest) -> SchemaResolveReply {
        match self.resolve_schema_proposals(request) {
            Ok(response) => SchemaResolveReply::Ready(response),
            Err(error) => SchemaResolveReply::Refused {
                body: crate::error_only_body(&format!("Schema resolve failed: {error}")),
            },
        }
    }

    /// Resolve one proposal: refresh when the client cache is stale, else the
    /// exact catalog row, native component cover, or embedding match.
    fn resolve_one_proposal(
        &self,
        proposal: &SchemaResolveProposal,
        cache_stale: bool,
    ) -> FoldDbResult<SchemaResolveResult> {
        let result = if cache_stale {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::Refresh,
                matched_shared_schema_hash: None,
                r#match: None,
                candidates: Vec::new(),
                candidate_shared_schema_hashes: Vec::new(),
                confidence: None,
            }
        } else if let Some(exact) = self.resolve_schema_proposal_exact_match(proposal)? {
            // O(1) catalog hit: the proposal's identity_hash, or its
            // (owner_app_id, descriptive_name), is already an exact row.
            // This runs before the embedding-beam path because that
            // path embeds the whole registry per request; a fresh node
            // declaring schemas the catalog already holds must not pay
            // for it (2026-09-13: every fresh install timed out here and
            // then burned its add-schema quota on registers).
            let outcome = match exact.outcome {
                SchemaResolveOutcome::Reuse => "matched_existing",
                SchemaResolveOutcome::CandidateEquivalent => "component_cover",
                SchemaResolveOutcome::Novel => "no_match",
                SchemaResolveOutcome::Refresh => "refresh",
            };
            self.record_schema_match_outcome(outcome, "exact_catalog_row", None);
            exact
        } else if let Some(native) = self.try_native_component_cover_resolve(proposal)? {
            // embedding-beam / native_component_cover@1 (preferred live path)
            self.record_schema_match_outcome(
                match native.outcome {
                    SchemaResolveOutcome::Reuse => "matched_existing",
                    SchemaResolveOutcome::CandidateEquivalent => "component_cover",
                    SchemaResolveOutcome::Novel => "no_match",
                    SchemaResolveOutcome::Refresh => "refresh",
                },
                "native_component_cover",
                None,
            );
            native
        } else if let Some(reuse_match) = self.resolve_schema_proposal_match(proposal)? {
            let shared_hash = reuse_match
                .schema
                .schema
                .identity_hash
                .clone()
                .unwrap_or_else(|| reuse_match.schema.schema.name.clone());
            let confidence = resolve_match_confidence(&reuse_match, proposal.fields.len());
            if reuse_match.is_superset {
                SchemaResolveResult {
                    outcome: SchemaResolveOutcome::Reuse,
                    matched_shared_schema_hash: Some(shared_hash),
                    r#match: Some(reuse_match),
                    candidates: Vec::new(),
                    candidate_shared_schema_hashes: Vec::new(),
                    confidence: Some(confidence),
                }
            } else {
                SchemaResolveResult {
                    outcome: SchemaResolveOutcome::CandidateEquivalent,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates: vec![reuse_match],
                    candidate_shared_schema_hashes: vec![shared_hash],
                    confidence: Some(confidence),
                }
            }
        } else {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::Novel,
                matched_shared_schema_hash: None,
                r#match: None,
                candidates: Vec::new(),
                candidate_shared_schema_hashes: Vec::new(),
                confidence: None,
            }
        };
        Ok(result)
    }

    /// Exact catalog lookups only — no embedding work. Returns a reuse-style
    /// result when the proposal's `identity_hash` is an active registry row,
    /// or when `(owner_app_id, descriptive_name)` resolves through the exact
    /// `descriptive_name_index`. `Ok(None)` means "not an exact row"; the
    /// caller then decides whether to spend on semantic matching.
    pub(crate) fn resolve_schema_proposal_exact_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaResolveResult>> {
        let reuse_match = match self.exact_identity_reuse_match(proposal)? {
            Some(m) => m,
            None => match self.exact_owner_name_reuse_match(proposal)? {
                Some(m) => m,
                None => return Ok(None),
            },
        };
        let shared_hash = reuse_match
            .schema
            .schema
            .identity_hash
            .clone()
            .unwrap_or_else(|| reuse_match.schema.schema.name.clone());
        let confidence = resolve_match_confidence(&reuse_match, proposal.fields.len());
        Ok(Some(if reuse_match.is_superset {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::Reuse,
                matched_shared_schema_hash: Some(shared_hash),
                r#match: Some(reuse_match),
                candidates: Vec::new(),
                candidate_shared_schema_hashes: Vec::new(),
                confidence: Some(confidence),
            }
        } else {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::CandidateEquivalent,
                matched_shared_schema_hash: None,
                r#match: None,
                candidates: vec![reuse_match],
                candidate_shared_schema_hashes: vec![shared_hash],
                confidence: Some(confidence),
            }
        }))
    }
}
