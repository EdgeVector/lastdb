//! Live Schema Service resolve via `native_component_cover@1` (embedding-beam).
//!
//! Production `POST /v1/schemas/resolve` prefers this path when enabled
//! (`SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE`, default **on**). On embedder
//! failure, work-limit, or non-reuse decisions the caller falls back to the
//! legacy descriptive-name match.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use schema_core_resolver::{
    evaluate_native, resolve as resolve_native, ComponentResolution, EmbeddingVectorRecord,
    FieldCoverageEvidence, NativeResolverInput, ProposalEmbeddings, ProposalFieldMetadata,
    ProposalMetadata, RegistryCanonicalFieldHandle, RegistryEmbeddings, RegistryFieldHandle,
    RegistryMetadata, RegistrySchemaHandle, ResolveVerdict, ResolverConfig, ResolverDecision,
    SCHEMA_RESOLVER_ABI_VERSION,
};
use schema_types::{FoldDbResult, Schema};

use super::builtin_schemas::is_app_starter_template;
use super::lock_helpers::read_lock;
use super::state::SchemaServiceState;
use super::types::{
    SchemaRegistrationComponent, SchemaRegistrationComposition, SchemaResolveOutcome,
    SchemaResolveProposal, SchemaResolveResult, SchemaReuseMatch,
};

/// Env gate: default ON. Set to `0`/`false`/`off` to force legacy resolve only.
pub(crate) fn native_component_cover_resolve_enabled() -> bool {
    match std::env::var("SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE") {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            !(t == "0" || t == "false" || t == "off" || t == "no")
        }
        Err(_) => true,
    }
}

/// Wall-clock budget for embedding the registry into the native resolve
/// input (`SCHEMA_NATIVE_RESOLVE_BUDGET_MS`, default 8000). The node client
/// gives a live resolve 10 s and API Gateway gives the Lambda 30 s; a
/// registry embedding that outruns this returns `Err` so the caller falls
/// back to the legacy exact/name match instead of timing out. Embeddings
/// computed before the budget fired stay in the memo, so the next request
/// starts further along and the memo fills across a few warm requests.
pub(crate) fn native_resolve_budget() -> Duration {
    const DEFAULT_MS: u64 = 8_000;
    let ms = env_flag::var_or("SCHEMA_NATIVE_RESOLVE_BUDGET_MS", DEFAULT_MS);
    Duration::from_millis(ms)
}

const REGISTRATION_COMPONENT_COVER_MIN_FIELDS: f32 = 0.90;
const STARTER_TEMPLATE_COVER_MIN_FIELDS: f32 = 0.60;

/// Field-context text aligned with pack publisher / local Mini resolver.
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

pub fn proposal_field_id(index: usize, field_name: &str) -> String {
    format!("pf{index}:{field_name}")
}

impl SchemaServiceState {
    /// Evaluate registration-time component cover for an incoming add-schema
    /// proposal. Returns `None` when native cover is disabled/unavailable or
    /// when the proposal does not meet the configured coverage threshold.
    ///
    /// Unlike `POST /v1/schemas/resolve`, this helper surfaces partial-cover
    /// residue so the mutation path can register only the uncovered fields and
    /// avoid minting a fat whole-schema identity.
    pub fn try_native_component_cover_registration(
        &self,
        schema: &Schema,
    ) -> FoldDbResult<Option<SchemaRegistrationComposition>> {
        if !native_component_cover_resolve_enabled() {
            return Ok(None);
        }

        let Some(fields) = schema.fields.as_ref().filter(|fields| !fields.is_empty()) else {
            return Ok(None);
        };
        if let Some(template_cover) = self.try_starter_template_cover_registration(schema)? {
            return Ok(Some(template_cover));
        }
        let proposal = SchemaResolveProposal {
            descriptive_name: schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema.name.clone()),
            fields: fields.clone(),
            field_descriptions: schema.field_descriptions.clone(),
            purpose_statement: schema.purpose_statement.clone(),
            identity_hash: schema.identity_hash.clone(),
            owner_app_id: schema.owner_app_id.clone(),
        };

        let mut built = match self.build_native_resolver_input(&proposal) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover_registration: input build failed"
                );
                return Ok(None);
            }
        };
        built.input.config.scoring.coverage.min_fields = built
            .input
            .config
            .scoring
            .coverage
            .min_fields
            .min(REGISTRATION_COMPONENT_COVER_MIN_FIELDS);
        built.input.config.scoring.coverage.min_required_fields = built
            .input
            .config
            .scoring
            .coverage
            .min_required_fields
            .min(REGISTRATION_COMPONENT_COVER_MIN_FIELDS);
        let output = match evaluate_native(&built.input) {
            Ok(output) => output,
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover_registration: evaluate failed"
                );
                return Ok(None);
            }
        };

        let field_by_id = proposal
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| (proposal_field_id(i, field), field.clone()))
            .collect::<HashMap<_, _>>();

        match output.decision {
            ResolverDecision::UseExisting => {
                let Some(use_existing) = output.use_existing else {
                    return Ok(None);
                };
                Ok(Some(SchemaRegistrationComposition {
                    matched_shared_schema_hash: Some(use_existing.schema_id),
                    covered_components: Vec::new(),
                    residue_fields: Vec::new(),
                    field_coverage: output.evidence.field_coverage,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::UseComponents | ResolverDecision::NeedsLiveSchemaService => {
                if output.use_components.is_empty() {
                    return Ok(None);
                }
                let residue_fields = output
                    .evidence
                    .residue_fields
                    .iter()
                    .filter_map(|r| field_by_id.get(&r.proposal_field_id).cloned())
                    .collect::<Vec<_>>();
                let covered_components =
                    registration_components_from_resolutions(&output.use_components, &field_by_id);
                Ok(Some(SchemaRegistrationComposition {
                    matched_shared_schema_hash: None,
                    covered_components,
                    residue_fields,
                    field_coverage: output.evidence.field_coverage,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::ExpandExistingIfAllowed
            | ResolverDecision::Ambiguous
            | ResolverDecision::Reject => Ok(None),
        }
    }

    fn try_starter_template_cover_registration(
        &self,
        schema: &Schema,
    ) -> FoldDbResult<Option<SchemaRegistrationComposition>> {
        let Some(proposal_fields) = schema.fields.as_ref().filter(|fields| !fields.is_empty())
        else {
            return Ok(None);
        };

        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let mut best: Option<TemplateCoverCandidate> = None;

        for (identity, template) in schemas.iter() {
            if template.superseded_by.is_some() || !is_app_starter_template(template) {
                continue;
            }
            let template_fields = template.fields.clone().unwrap_or_default();
            if template_fields.is_empty() {
                continue;
            }

            let mut covered = Vec::new();
            for field in proposal_fields {
                if template_fields
                    .iter()
                    .any(|template_field| template_field == field)
                {
                    covered.push(field.clone());
                }
            }

            let coverage = covered.len() as f32 / proposal_fields.len() as f32;
            if coverage < STARTER_TEMPLATE_COVER_MIN_FIELDS {
                continue;
            }

            let schema_id = template
                .identity_hash
                .clone()
                .unwrap_or_else(|| identity.clone());
            let candidate = TemplateCoverCandidate {
                schema_id,
                covered,
                coverage,
                template_field_count: template_fields.len(),
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate.better_than(current))
            {
                best = Some(candidate);
            }
        }

        let Some(best) = best else {
            return Ok(None);
        };

        let covered_set: HashSet<String> = best.covered.iter().cloned().collect();
        let residue_fields = proposal_fields
            .iter()
            .filter(|field| !covered_set.contains(*field))
            .cloned()
            .collect::<Vec<_>>();
        let covered_count = covered_set.len();
        let covered_components = best
            .covered
            .into_iter()
            .map(|field| SchemaRegistrationComponent {
                field,
                shared_schema_hash: best.schema_id.clone(),
                confidence: best.coverage,
            })
            .collect::<Vec<_>>();

        tracing::info!(
            target: "schema_service::schema",
            template_schema = %best.schema_id,
            field_coverage = best.coverage,
            residue_fields = residue_fields.len(),
            "starter_template_match: covered proposal through templates owner namespace"
        );

        Ok(Some(SchemaRegistrationComposition {
            matched_shared_schema_hash: None,
            covered_components,
            residue_fields,
            field_coverage: FieldCoverageEvidence {
                covered_fields: covered_count as u32,
                total_fields: proposal_fields.len() as u32,
                required_fields_covered: covered_count as u32,
                required_fields_total: proposal_fields.len() as u32,
            },
            confidence: Some(best.coverage),
        }))
    }

    /// Build the residual schema that should be minted for a partial component
    /// cover. Returns `None` for fully-covered proposals.
    pub fn residual_schema_for_registration(
        &self,
        schema: &Schema,
        composition: &SchemaRegistrationComposition,
    ) -> Option<Schema> {
        if composition.residue_fields.is_empty() {
            return None;
        }
        let residue: HashSet<&str> = composition
            .residue_fields
            .iter()
            .map(String::as_str)
            .collect();
        let mut residual = schema.clone();
        let fields = schema
            .fields
            .as_ref()?
            .iter()
            .filter(|field| residue.contains(field.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if fields.is_empty() {
            return None;
        }
        residual.fields = Some(fields);
        residual
            .field_descriptions
            .retain(|field, _| residue.contains(field.as_str()));
        residual
            .field_types
            .retain(|field, _| residue.contains(field.as_str()));
        residual
            .ref_fields
            .retain(|field, _| residue.contains(field.as_str()));
        residual.identity_hash = None;
        let base_name = schema
            .descriptive_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(schema.name.as_str());
        residual.descriptive_name = Some(format!("{base_name} Residual"));
        residual.purpose_statement = schema.purpose_statement.as_ref().map(|purpose| {
            format!("{purpose} Residual fields not covered by existing shared components.")
        });
        residual.name = residual
            .descriptive_name
            .clone()
            .unwrap_or_else(|| "Residual Schema".to_string());
        Some(residual)
    }

    /// Try embedding-beam native resolve. Returns `Ok(Some(result))` when the
    /// native path produced a reuse-style outcome; `Ok(None)` when the caller
    /// should fall back to legacy matching.
    pub(crate) fn try_native_component_cover_resolve(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaResolveResult>> {
        if !native_component_cover_resolve_enabled() {
            return Ok(None);
        }

        let built = match self.build_native_resolver_input(proposal) {
            Ok(b) => b,
            Err(e) => {
                tracing::info!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover: input build failed; legacy fallback"
                );
                return Ok(None);
            }
        };

        let output = match resolve_native(&built.input) {
            Ok(ResolveVerdict::Resolved(output)) => output,
            Ok(ResolveVerdict::Unresolvable(unresolvable)) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    decision = ?unresolvable.decision,
                    "native_component_cover: unresolvable decision; legacy fallback"
                );
                return Ok(None);
            }
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover: evaluate failed; legacy fallback"
                );
                return Ok(None);
            }
        };

        match output.decision {
            ResolverDecision::UseExisting => {
                let Some(ref use_ex) = output.use_existing else {
                    return Ok(None);
                };
                let Some(reuse) =
                    self.schema_reuse_match_for_identity(proposal, &use_ex.schema_id)?
                else {
                    return Ok(None);
                };
                if !self.resolve_purpose_gate_passes(proposal, &reuse.schema.schema) {
                    return Ok(None);
                }
                let is_superset = reuse.is_superset;
                let shared_hash = reuse
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| reuse.schema.schema.name.clone());
                if is_superset {
                    Ok(Some(SchemaResolveResult {
                        outcome: SchemaResolveOutcome::Reuse,
                        matched_shared_schema_hash: Some(shared_hash),
                        r#match: Some(reuse),
                        candidates: Vec::new(),
                        candidate_shared_schema_hashes: Vec::new(),
                        confidence: Some(output.confidence),
                    }))
                } else {
                    Ok(Some(SchemaResolveResult {
                        outcome: SchemaResolveOutcome::CandidateEquivalent,
                        matched_shared_schema_hash: None,
                        r#match: None,
                        candidates: vec![reuse],
                        candidate_shared_schema_hashes: vec![shared_hash],
                        confidence: Some(output.confidence),
                    }))
                }
            }
            ResolverDecision::UseComponents => {
                if output.use_components.is_empty() {
                    return Ok(None);
                }
                let mut hashes: Vec<String> = output
                    .use_components
                    .iter()
                    .map(|c| c.schema_id.clone())
                    .collect();
                hashes.sort();
                hashes.dedup();
                // Prefer full reuse matches for components when available.
                let mut candidates = Vec::new();
                for h in &hashes {
                    if let Some(m) = self.schema_reuse_match_for_identity(proposal, h)? {
                        candidates.push(m);
                    }
                }
                // Every component must describe the same data as the
                // proposal (the register path's purpose gate). One that
                // does not turns the cover into a composition of unrelated
                // schemas that merely share generic fields; hand the
                // proposal to the gated legacy path instead.
                if candidates
                    .iter()
                    .any(|m| !self.resolve_purpose_gate_passes(proposal, &m.schema.schema))
                {
                    return Ok(None);
                }
                Ok(Some(SchemaResolveResult {
                    outcome: SchemaResolveOutcome::CandidateEquivalent,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates,
                    candidate_shared_schema_hashes: hashes,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::ExpandExistingIfAllowed => {
                let Some(ref expand) = output.expand_existing_if_allowed else {
                    return Ok(None);
                };
                let Some(reuse) =
                    self.schema_reuse_match_for_identity(proposal, &expand.schema_id)?
                else {
                    return Ok(None);
                };
                if !self.resolve_purpose_gate_passes(proposal, &reuse.schema.schema) {
                    return Ok(None);
                }
                let shared_hash = reuse
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| reuse.schema.schema.name.clone());
                Ok(Some(SchemaResolveResult {
                    outcome: SchemaResolveOutcome::CandidateEquivalent,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates: vec![reuse],
                    candidate_shared_schema_hashes: vec![shared_hash],
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::Ambiguous
            | ResolverDecision::NeedsLiveSchemaService
            | ResolverDecision::Reject => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    decision = ?output.decision,
                    "native_component_cover: non-reuse decision; legacy fallback"
                );
                Ok(None)
            }
        }
    }

    fn schema_reuse_match_for_identity(
        &self,
        proposal: &SchemaResolveProposal,
        identity: &str,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(identity) else {
            return Ok(None);
        };
        let (active_schema, active_name) =
            match self.resolve_active_schema(existing, identity, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), identity.to_string()),
            };
        let matched_desc = active_schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| active_name.clone());
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            true,
        )))
    }

    /// Embed `text` through the process-local memo. Registry texts repeat on
    /// every resolve, so only the first request on a warm process pays the
    /// model; later requests read the vector back.
    fn memo_embed_text(&self, text: &str, memo: &mut MemoStats) -> Result<Vec<f32>, String> {
        if let Ok(cache) = self.native_resolve_embeddings.read() {
            if let Some(vec) = cache.get(text) {
                memo.hits += 1;
                return Ok(vec.clone());
            }
        }
        let vec = self.embedder.embed_text(text).map_err(|e| e.to_string())?;
        memo.misses += 1;
        if let Ok(mut cache) = self.native_resolve_embeddings.write() {
            cache.insert(text.to_string(), vec.clone());
        }
        Ok(vec)
    }

    fn build_native_resolver_input(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> Result<BuiltNativeInput, String> {
        let config = ResolverConfig::embedding_beam_shadow_defaults();
        let schemas = read_lock(&self.schemas, "schemas_cache").map_err(|e| e.to_string())?;
        let budget = native_resolve_budget();
        let started = Instant::now();
        let mut memo = MemoStats::default();
        // Checked after every model call that missed the memo. A hit costs
        // a map read, so the check only matters on the slow path.
        let over_budget = |memo: &MemoStats| -> Result<(), String> {
            let elapsed = started.elapsed();
            if elapsed > budget {
                return Err(format!(
                    "registry embedding exceeded budget: {}ms > {}ms after {} model calls ({} memo hits)",
                    elapsed.as_millis(),
                    budget.as_millis(),
                    memo.misses,
                    memo.hits
                ));
            }
            Ok(())
        };

        let mut registry_schemas = Vec::new();
        let mut registry_fields = Vec::new();
        let mut name_embeddings = Vec::new();
        let mut field_embeddings = Vec::new();

        let max_schemas = config.limits.max_registry_schemas as usize;
        let mut count = 0usize;

        for (identity, schema) in schemas.iter() {
            if schema.superseded_by.is_some() {
                continue;
            }
            if count >= max_schemas {
                break;
            }
            let schema_id = schema
                .identity_hash
                .clone()
                .unwrap_or_else(|| identity.clone());
            let descriptive = schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema_id.clone());

            // Optional owner scoping: when proposal has owner_app_id, prefer
            // same-owner + unowned schemas for the registry shortlist. Still
            // include others so component cover can reuse global shapes.
            let _owner = schema.owner_app_id.as_deref();

            let name_vec = self
                .memo_embed_text(&descriptive, &mut memo)
                .map_err(|e| format!("embed schema name: {e}"))?;
            over_budget(&memo)?;
            name_embeddings.push(EmbeddingVectorRecord {
                target_id: schema_id.clone(),
                vector: name_vec,
            });

            let field_names = schema.fields.clone().unwrap_or_default();
            for field_name in &field_names {
                let field_id = format!("{schema_id}#{field_name}");
                let ftype = schema.get_field_type(field_name).to_string();
                registry_fields.push(RegistryFieldHandle {
                    field_id: field_id.clone(),
                    schema_id: schema_id.clone(),
                    field_name: field_name.clone(),
                    field_type: ftype,
                    canonical_field_ids: Vec::new(),
                });
                let desc = schema
                    .field_descriptions
                    .get(field_name)
                    .map(String::as_str);
                let text = field_context_text(&descriptive, field_name, desc);
                let fvec = self
                    .memo_embed_text(&text, &mut memo)
                    .map_err(|e| format!("embed field context: {e}"))?;
                over_budget(&memo)?;
                field_embeddings.push(EmbeddingVectorRecord {
                    target_id: field_id,
                    vector: fvec,
                });
            }

            registry_schemas.push(RegistrySchemaHandle {
                schema_id,
                descriptive_name: descriptive,
                lifecycle: "active".to_string(),
            });
            count += 1;
        }

        if registry_schemas.is_empty() {
            return Err("empty registry".into());
        }

        let proposal_id = proposal
            .identity_hash
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("prop:{}", proposal.descriptive_name));

        if proposal.fields.is_empty() {
            return Err("proposal has no fields".into());
        }

        let mut p_fields = Vec::with_capacity(proposal.fields.len());
        let mut p_field_emb = Vec::with_capacity(proposal.fields.len());
        for (i, name) in proposal.fields.iter().enumerate() {
            let pfid = proposal_field_id(i, name);
            let ftype = "String".to_string();
            p_fields.push(ProposalFieldMetadata {
                proposal_field_id: pfid.clone(),
                field_name: name.clone(),
                field_type: ftype,
                required: true,
            });
            let desc = proposal.field_descriptions.get(name).map(String::as_str);
            let text = field_context_text(&proposal.descriptive_name, name, desc);
            let fvec = self
                .embedder
                .embed_text(&text)
                .map_err(|e| format!("embed proposal field: {e}"))?;
            p_field_emb.push(EmbeddingVectorRecord {
                target_id: pfid,
                vector: fvec,
            });
        }

        let p_name = self
            .embedder
            .embed_text(proposal.descriptive_name.trim())
            .map_err(|e| format!("embed proposal name: {e}"))?;

        // Canonical fields from in-memory catalog (types only; embeddings optional).
        let mut canonical_meta = Vec::new();
        let mut canonical_emb = Vec::new();
        if let Ok(cf) = self.canonical_fields.read() {
            for (name, field) in cf.iter() {
                let cid = format!("field:{name}");
                canonical_meta.push(RegistryCanonicalFieldHandle {
                    canonical_field_id: cid.clone(),
                    field_type: field.field_type.to_string(),
                });
                if let Ok(vec) = self.memo_embed_text(name, &mut memo) {
                    canonical_emb.push(EmbeddingVectorRecord {
                        target_id: cid,
                        vector: vec,
                    });
                }
                over_budget(&memo)?;
            }
        }

        tracing::debug!(
            target: "schema_service::schema",
            descriptive_name = %proposal.descriptive_name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            model_calls = memo.misses,
            memo_hits = memo.hits,
            "native_component_cover: resolver input built"
        );

        let input = NativeResolverInput {
            resolver_contract_version: SCHEMA_RESOLVER_ABI_VERSION,
            proposal_metadata: ProposalMetadata {
                proposal_id,
                descriptive_name: proposal.descriptive_name.clone(),
                fields: p_fields,
            },
            proposal_embeddings: ProposalEmbeddings {
                descriptive_name: p_name,
                field_contexts: p_field_emb,
            },
            registry_metadata: RegistryMetadata {
                schemas: registry_schemas,
                fields: registry_fields,
                canonical_fields: canonical_meta,
            },
            registry_embeddings: RegistryEmbeddings {
                descriptive_names: name_embeddings,
                schema_field_contexts: field_embeddings,
                canonical_fields: canonical_emb,
            },
            config,
        };

        Ok(BuiltNativeInput { input })
    }
}

struct BuiltNativeInput {
    input: NativeResolverInput,
}

/// Memo hit/miss counters for one resolver-input build; `misses` is the
/// number of real model calls the request paid for.
#[derive(Default)]
struct MemoStats {
    hits: usize,
    misses: usize,
}

struct TemplateCoverCandidate {
    schema_id: String,
    covered: Vec<String>,
    coverage: f32,
    template_field_count: usize,
}

impl TemplateCoverCandidate {
    fn better_than(&self, other: &Self) -> bool {
        self.coverage
            .partial_cmp(&other.coverage)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.covered.len().cmp(&other.covered.len()))
            .then_with(|| other.template_field_count.cmp(&self.template_field_count))
            .then_with(|| other.schema_id.cmp(&self.schema_id))
            .is_gt()
    }
}

fn registration_components_from_resolutions(
    resolutions: &[ComponentResolution],
    field_by_id: &HashMap<String, String>,
) -> Vec<SchemaRegistrationComponent> {
    let mut components = resolutions
        .iter()
        .filter_map(|resolution| {
            field_by_id.get(&resolution.proposal_field_id).map(|field| {
                SchemaRegistrationComponent {
                    field: field.clone(),
                    shared_schema_hash: resolution.schema_id.clone(),
                    confidence: resolution.confidence,
                }
            })
        })
        .collect::<Vec<_>>();
    components.sort_by(|a, b| {
        a.field
            .cmp(&b.field)
            .then_with(|| a.shared_schema_hash.cmp(&b.shared_schema_hash))
    });
    components
}

/// Eval/debug: score left field texts against a right catalog of field texts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeItem {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeRequest {
    pub left: Vec<FieldMatchProbeItem>,
    pub right: Vec<FieldMatchProbeItem>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize {
    24
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeHit {
    pub id: String,
    pub score: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldMatchProbeResponse {
    pub matches: HashMap<String, Vec<FieldMatchProbeHit>>,
}

impl SchemaServiceState {
    /// Cosine similarity of embedded texts — used by component_cover.mjs embedding-beam.
    pub fn field_match_probe(
        &self,
        request: &FieldMatchProbeRequest,
    ) -> Result<FieldMatchProbeResponse, String> {
        let top_k = request.top_k.clamp(1, 128);
        let mut right_emb = Vec::with_capacity(request.right.len());
        for item in &request.right {
            let v = self
                .embedder
                .embed_text(&item.text)
                .map_err(|e| format!("embed right {}: {e}", item.id))?;
            right_emb.push((item.id.clone(), v));
        }

        let mut matches = HashMap::new();
        for left in &request.left {
            let lv = self
                .embedder
                .embed_text(&left.text)
                .map_err(|e| format!("embed left {}: {e}", left.id))?;
            let mut hits: Vec<FieldMatchProbeHit> = right_emb
                .iter()
                .map(|(id, rv)| FieldMatchProbeHit {
                    id: id.clone(),
                    score: crate::embedder::cosine_similarity(&lv, rv),
                })
                .collect();
            hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            hits.truncate(top_k);
            matches.insert(left.id.clone(), hits);
        }
        Ok(FieldMatchProbeResponse { matches })
    }
}
