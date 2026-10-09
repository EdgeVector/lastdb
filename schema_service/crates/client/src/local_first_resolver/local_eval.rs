//! Local evaluation helpers (pure / pack-bound).

use super::*;

// ---------------------------------------------------------------------------
// Local evaluation helpers (pure / pack-bound)
// ---------------------------------------------------------------------------

/// Build [`RegistryMetadata`] from a loaded pack snapshot.
pub fn pack_to_registry_metadata(pack: &LoadedResolverPack) -> RegistryMetadata {
    let mut schemas = Vec::with_capacity(pack.schema_snapshot.schemas.len());
    let mut fields = Vec::new();
    for schema in &pack.schema_snapshot.schemas {
        schemas.push(RegistrySchemaHandle {
            schema_id: schema.schema_id.clone(),
            descriptive_name: schema.descriptive_name.clone(),
            lifecycle: lifecycle_str(schema.lifecycle).to_string(),
        });
        for field in &schema.fields {
            fields.push(RegistryFieldHandle {
                field_id: field.field_id.clone(),
                schema_id: schema.schema_id.clone(),
                field_name: field.field_name.clone(),
                field_type: field.field_type.clone(),
                canonical_field_ids: field.canonical_field_ids.clone(),
            });
        }
    }
    let canonical_fields = pack
        .schema_snapshot
        .canonical_fields
        .iter()
        .map(|c| RegistryCanonicalFieldHandle {
            canonical_field_id: c.canonical_field_id.clone(),
            field_type: c.field_type.clone(),
        })
        .collect();
    RegistryMetadata {
        schemas,
        fields,
        canonical_fields,
    }
}

pub(super) fn lifecycle_str(
    lifecycle: schema_service_core::resolver_pack::SchemaLifecycle,
) -> &'static str {
    use schema_service_core::resolver_pack::SchemaLifecycle;
    match lifecycle {
        SchemaLifecycle::Seed => "seed",
        SchemaLifecycle::Active => "active",
        SchemaLifecycle::Deprecated => "deprecated",
    }
}

/// Field-context text aligned with schema service embedding construction.
pub fn field_context_text(
    descriptive_name: &str,
    field_name: &str,
    field_description: Option<&str>,
) -> String {
    match field_description.map(str::trim).filter(|s| !s.is_empty()) {
        Some(desc) => format!("the {field_name} of the {descriptive_name}: {desc}"),
        None => format!("the {field_name} of the {descriptive_name}"),
    }
}

/// Stable proposal field id (not the free-form field name alone).
pub fn proposal_field_id(index: usize, field_name: &str) -> String {
    format!("pf{index}:{field_name}")
}

/// Outcome of a local native evaluation attempt.
#[derive(Debug, Clone)]
pub struct LocalEvaluateOutcome {
    pub output: ResolverOutput,
    pub route: ResolverPackResolutionRoute,
    pub resolve_result: Option<SchemaResolveResult>,
    pub fallback_reason: Option<&'static str>,
}

/// Evaluate one proposal against a loaded pack (proposal embeddings only).
pub fn evaluate_local_proposal(
    pack: &LoadedResolverPack,
    embedder: &dyn Embedder,
    proposal_id: &str,
    proposal: &SchemaResolveProposal,
) -> Result<LocalEvaluateOutcome, LocalEvaluateError> {
    let proposal_metadata = proposal_to_metadata(proposal_id, proposal);
    let proposal_embeddings = embed_proposal(embedder, proposal, &proposal_metadata)?;
    let registry_metadata = pack_to_registry_metadata(pack);
    let registry_embeddings = pack.registry_embeddings_for_abi();
    let input = NativeResolverInput {
        resolver_contract_version: pack.resolver_config.resolver_contract_version,
        proposal_metadata,
        proposal_embeddings,
        registry_metadata,
        registry_embeddings,
        config: pack.resolver_config.clone(),
    };
    let output = evaluate_native(&input).map_err(LocalEvaluateError::Native)?;
    let route = pack.route_decision(output.decision);
    let (resolve_result, fallback_reason) = match (&route, output.decision) {
        (
            ResolverPackResolutionRoute::UseLocal,
            ResolverDecision::UseExisting | ResolverDecision::UseComponents,
        ) => match map_resolver_output_to_resolve_result(&output) {
            Some(r) => (Some(r), None),
            None => (None, Some("local_map_incomplete")),
        },
        (ResolverPackResolutionRoute::LiveServiceFallback { reason }, _) => (None, Some(*reason)),
        (ResolverPackResolutionRoute::UseLocal, _) => {
            // ExpandExisting etc. are not safe existing-only reuse.
            (None, Some("local_decision_not_existing_only"))
        }
    };
    Ok(LocalEvaluateOutcome {
        output,
        route,
        resolve_result,
        fallback_reason,
    })
}

#[derive(Debug)]
pub enum LocalEvaluateError {
    Embed(String),
    Native(schema_service_core::NativeResolverError),
    EmptyProposalId,
    EmptyFields,
}

impl std::fmt::Display for LocalEvaluateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Embed(e) => write!(f, "embed: {e}"),
            Self::Native(e) => write!(f, "native: {e}"),
            Self::EmptyProposalId => write!(f, "empty proposal_id"),
            Self::EmptyFields => write!(f, "proposal has no fields"),
        }
    }
}

impl std::error::Error for LocalEvaluateError {}

pub(super) fn proposal_to_metadata(
    proposal_id: &str,
    proposal: &SchemaResolveProposal,
) -> ProposalMetadata {
    let fields = proposal
        .fields
        .iter()
        .enumerate()
        .map(|(i, name)| ProposalFieldMetadata {
            proposal_field_id: proposal_field_id(i, name),
            field_name: name.clone(),
            field_type: "String".to_string(),
            required: true,
        })
        .collect();
    ProposalMetadata {
        proposal_id: proposal_id.to_string(),
        descriptive_name: proposal.descriptive_name.clone(),
        fields,
    }
}

pub(super) fn embed_proposal(
    embedder: &dyn Embedder,
    proposal: &SchemaResolveProposal,
    metadata: &ProposalMetadata,
) -> Result<ProposalEmbeddings, LocalEvaluateError> {
    let name = proposal.descriptive_name.trim();
    if name.is_empty() {
        return Err(LocalEvaluateError::Embed(
            "empty descriptive_name".to_string(),
        ));
    }
    let descriptive_name = embedder
        .embed_text(name)
        .map_err(|e| LocalEvaluateError::Embed(e.to_string()))?;

    let mut field_contexts = Vec::with_capacity(metadata.fields.len());
    for field in &metadata.fields {
        if field.field_name.trim().is_empty() {
            continue;
        }
        let desc = proposal
            .field_descriptions
            .get(&field.field_name)
            .map(String::as_str);
        let text = field_context_text(&proposal.descriptive_name, &field.field_name, desc);
        let vector = embedder
            .embed_text(&text)
            .map_err(|e| LocalEvaluateError::Embed(e.to_string()))?;
        field_contexts.push(EmbeddingVectorRecord {
            target_id: field.proposal_field_id.clone(),
            vector,
        });
    }
    if field_contexts.is_empty() {
        return Err(LocalEvaluateError::EmptyFields);
    }
    Ok(ProposalEmbeddings {
        descriptive_name,
        field_contexts,
    })
}

/// Map a native resolver output to a service-shaped [`SchemaResolveResult`].
///
/// Only `UseExisting` and `UseComponents` produce a local result; other
/// decisions return `None` (caller falls back to live).
pub fn map_resolver_output_to_resolve_result(
    output: &ResolverOutput,
) -> Option<SchemaResolveResult> {
    match output.decision {
        ResolverDecision::UseExisting => {
            let schema_id = output.use_existing.as_ref()?.schema_id.clone();
            Some(SchemaResolveResult {
                outcome: SchemaResolveOutcome::Reuse,
                matched_shared_schema_hash: Some(schema_id),
                r#match: None,
                candidates: Vec::new(),
                candidate_shared_schema_hashes: Vec::new(),
                confidence: Some(output.confidence),
            })
        }
        ResolverDecision::UseComponents => {
            if output.use_components.is_empty() {
                return None;
            }
            let mut hashes: Vec<String> = output
                .use_components
                .iter()
                .map(|c| c.schema_id.clone())
                .collect();
            hashes.sort();
            hashes.dedup();
            Some(SchemaResolveResult {
                outcome: SchemaResolveOutcome::CandidateEquivalent,
                matched_shared_schema_hash: None,
                r#match: None,
                candidates: Vec::new(),
                candidate_shared_schema_hashes: hashes,
                confidence: Some(output.confidence),
            })
        }
        ResolverDecision::Ambiguous
        | ResolverDecision::NeedsLiveSchemaService
        | ResolverDecision::Reject
        | ResolverDecision::ExpandExistingIfAllowed => None,
    }
}
