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

pub(super) struct BuiltNativeInput {
    input: NativeResolverInput,
}

/// Memo hit/miss counters for one resolver-input build; `misses` is the
/// number of real model calls the request paid for.
#[derive(Default)]
pub(super) struct MemoStats {
    hits: usize,
    misses: usize,
}

pub(super) struct TemplateCoverCandidate {
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

pub(super) fn registration_components_from_resolutions(
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

mod input;
mod probe;
mod registration;
mod resolve;

pub use probe::{
    FieldMatchProbeHit, FieldMatchProbeItem, FieldMatchProbeRequest, FieldMatchProbeResponse,
};
