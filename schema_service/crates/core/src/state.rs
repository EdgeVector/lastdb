use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::embedder::{cosine_similarity, Embedder};
use crate::lock_helpers::{read_lock, write_lock};
use crate::memory_persistence::InMemorySchemaPersistence;
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use super::external_persistence::ExternalSchemaPersistence;
use super::near_miss::{NearMissDecision, NearMissRecord};
use super::schema_mutation_gate::{SchemaMutationGateConfig, SchemaMutationGateStore};
use super::snapshot::AppRecord;
use super::state_canonicalization::{
    gate_outcome_for_candidate, rank_candidates, CandidateSet, CanonicalizationGateOutcome,
    MatchCandidate, MatchSeam,
};
use super::state_matching::collect_field_names;
pub use super::state_matching::jaccard_index;
use super::types::SchemaResolveOutcome;
use super::types::{
    DeprecateSchemasRequest, DeprecateSchemasResponse, DeprecatedSchemaEntry,
    DescriptiveNameDedupeGroup, SchemaAddOutcome, SchemaEnvelope, SchemaLookupEntry,
    SchemaMatchTelemetrySnapshot, SchemaMutationGateIntent, SchemaMutationGateObservation,
    SchemaMutationGateRequirement, SchemaResolveProposal, SchemaResolveRequest,
    SchemaResolveResponse, SchemaResolveResult, SchemaReuseMatch, SimilarSchemaEntry,
    SimilarSchemasResponse,
};

mod add_schema;
mod add_schema_guards;
mod add_schema_persist;
mod add_schema_race;
mod add_schema_validate;
mod backfill;
mod candidate_seams;
mod canonical_field;
mod canonicalization_candidates;
mod collection_names;
mod idempotent_repost;
mod lifecycle;
mod loading;
mod lookup;
mod mutation_gate;
mod near_misses;
mod persistence;
mod resolve;
mod vocabulary_validation;

pub use backfill::BackfillPurposeReport;
pub use lifecycle::{DedupeNamesReply, DeprecateSchemasReply};
pub use lookup::SchemaByNameReply;
pub use near_misses::{DEFAULT_NEAR_MISSES_LIMIT, MAX_NEAR_MISSES_LIMIT};
pub use resolve::{
    accepted_similarity_threshold, SchemaResolveReply, SimilarSchemasReply,
    MAX_SCHEMA_RESOLVE_PROPOSALS,
};

/// Compose the key used in `descriptive_name_index` (and
/// `descriptive_name_embeddings`) so dedup is scoped by `owner_app_id`.
///
/// `(Some("fbrain"), "Project") → "fbrain/Project"` — distinct from a
/// seed `Project` (`(None, "Project") → "Project"`) or another app's
/// `Project` (`(Some("kanban"), "Project") → "kanban/Project"`).
///
/// Matches the canonical-name composition used by
/// [`schema_types::Schema::canonical_name`] /
/// [`schema_types::Schema::parse_canonical_name`] so callers
/// that already know the canonical name (e.g. fold_db_node fetching a
/// seed by `"Persona"` or a query referencing `"fbrain/Concept"`) hit
/// the right index entry directly.
///
/// Back-compat: `owner_app_id == None` (legacy + `SystemSeed` /
/// `StarterSeed`) keeps the bare-name key, so existing entries continue
/// to resolve unchanged. Empty-string `owner_app_id` is treated as
/// `None` for the same reason.
pub(crate) fn descriptive_name_key(owner_app_id: Option<&str>, descriptive_name: &str) -> String {
    match owner_app_id.filter(|s| !s.is_empty()) {
        Some(app_id) => format!("{app_id}/{descriptive_name}"),
        None => descriptive_name.to_string(),
    }
}

fn normalize_owner(owner_app_id: Option<&str>) -> Option<&str> {
    owner_app_id.filter(|s| !s.is_empty())
}

fn safe_telemetry_label(raw: &str) -> String {
    let label = raw.trim();
    if label.is_empty() {
        return "unknown".to_string();
    }
    if label.len() > 64
        || !label
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return "other".to_string();
    }
    label.to_string()
}

/// Truncate `s` to at most `max_bytes`, snapping the cut down to the nearest
/// UTF-8 char boundary. Local copy of `observability::truncate` so core does
/// not depend on the observability crate (diet cut #4).
fn truncate_on_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Storage backend for the schema service.
///
/// All durable paths go through [`ExternalSchemaPersistence`]:
/// - **Dev server / eval / seed tools:** [`InMemorySchemaPersistence`]
///   (lost on restart)
/// - **Lambda:** S3 (and friends) via schema-infra
///
/// In-memory caches still live on [`SchemaServiceState`]; this only
/// owns durability.
#[derive(Clone)]
pub enum SchemaStorage {
    /// Caller-supplied persistence backend (in-memory, S3, …).
    External(Arc<dyn ExternalSchemaPersistence>),
}

impl SchemaStorage {
    /// The durability backend (in-memory, S3, …).
    pub fn backend(&self) -> &Arc<dyn ExternalSchemaPersistence> {
        match self {
            Self::External(b) => b,
        }
    }
}

/// Shared state for the schema service
#[derive(Clone)]
pub struct SchemaServiceState {
    pub schemas: Arc<RwLock<HashMap<String, Schema>>>,
    /// Secondary index: descriptive_name -> schema_name (identity_hash)
    pub descriptive_name_index: Arc<RwLock<HashMap<String, String>>>,
    /// Cached embeddings for descriptive names: descriptive_name -> embedding vector
    pub descriptive_name_embeddings: Arc<RwLock<HashMap<String, Vec<f32>>>>,
    /// Cached embeddings for context-enriched field names: "desc_name:field_name" -> embedding
    pub field_embeddings: Arc<RwLock<HashMap<String, Vec<f32>>>>,
    /// Global canonical field registry: canonical_name -> CanonicalField (description + type).
    /// New schema proposals have their field names matched against this list
    /// so that semantically equivalent fields use consistent names across all schemas.
    pub canonical_fields: Arc<RwLock<HashMap<String, super::types::CanonicalField>>>,
    /// Cached embeddings for canonical field names
    pub canonical_field_embeddings: Arc<RwLock<HashMap<String, Vec<f32>>>>,
    /// Memo for the embedding-beam resolve input (`POST /v1/schemas/resolve`
    /// via `native_component_cover@1`), keyed by the exact embedded text.
    /// Building that input embeds every active schema name, every schema
    /// field context, and every canonical field name; without this memo a
    /// 327-schema catalog costs ~5,000 ONNX calls per request and the prod
    /// Lambda ran 250-300 s per resolve (2026-09-13), so API Gateway cut
    /// every call at 30 s with a 503. Process-local, so a warm Lambda pays
    /// the registry embedding once; see `state_native_resolve.rs`.
    pub native_resolve_embeddings: Arc<RwLock<HashMap<String, Vec<f32>>>>,
    /// Text embedding model for semantic descriptive name matching
    pub embedder: Arc<dyn Embedder>,
    /// Embeddings of the anti-field descriptions for this state's embedder.
    ///
    /// Empty until every description embeds. A failure is not stored, so the
    /// next check tries again. The memo is per state, with the embedder.
    pub(crate) anti_field_embedding_cache: Arc<RwLock<Option<Vec<Vec<f32>>>>>,
    pub storage: SchemaStorage,
    /// Identity hashes of schemas classified as system/infrastructure
    /// (the Phase 1 fingerprint built-ins: Fingerprint, Edge, Identity,
    /// Persona, IdentityReceipt, Mention, etc.). Populated by
    /// `builtin_schemas::seed()` at startup. Everything else — i.e.
    /// user-proposed schemas — is treated as `system = false`.
    ///
    /// Rebuilt on every boot because `seed()` is idempotent and
    /// re-runs on every Lambda cold start / actix binary start, so
    /// the set doesn't need to be persisted.
    pub system_schema_hashes: Arc<RwLock<HashSet<String>>>,
    /// True while an authoritative seeder (`builtin_schemas::seed`,
    /// `schema_org_seeds::seed`) is loading
    /// its committed schemas at startup. Gates OFF the fuzzy
    /// **reuse-before-NEW** path ([`crate::state_matching::SchemaServiceState::find_purpose_reuse_target`])
    /// so an authoritative schema can never be silently expanded into a
    /// semantically-near committed seed/built-in — which would both break the
    /// `builtin_schemas::seed` "built-ins never expand" startup invariant and
    /// corrupt the deterministic seed registry that federation depends on.
    /// Reuse-before-NEW is a USER-data dedup feature; seeding is authoritative.
    /// Sequential at startup, so a plain atomic is sufficient (no lock).
    /// `Arc`-wrapped so a cloned `SchemaServiceState` (the type is `Clone`)
    /// shares the same flag — the seeders flip it on one handle while
    /// `add_schema` reads it on another.
    pub seeding_in_progress: Arc<std::sync::atomic::AtomicBool>,
    /// Pre-computed embeddings for reference collection names (anchor set).
    /// Used to validate that incoming descriptive_names are proper collection names
    /// rather than AI-generated captions/descriptions.
    pub collection_name_anchors: Vec<Vec<f32>>,
    /// Phase C shadow-mode audit log: registrations where the single-signal
    /// and dual-signal canonicalization algorithms produced different
    /// outcomes. Populated at startup from persistent storage and appended
    /// on every disagreement when `SCHEMA_SHADOW_MODE=true`. Read by
    /// `GET /v1/canonicalization-near-misses`.
    pub near_misses: Arc<RwLock<Vec<NearMissRecord>>>,
    /// Monotonic state-version counter. Surfaced as `version` on
    /// `GET /v1/snapshot` so clients can detect mid-flight changes
    /// (app_identity v3.1, Lane B2a). Bumped via
    /// [`SchemaServiceState::bump_state_version`] at every state-changing
    /// write — currently from `add_schema`, `insert_pre_validated_schema`,
    /// `insert_pre_validated_canonical_field`, `add_view`, `import_snapshot`,
    /// and the transform-registry mutators. Starts at 0 on a fresh state;
    /// persistence is out of scope for B2a (every redeploy resets it —
    /// clients compare snapshot bytes, not version alone).
    pub state_version: Arc<AtomicU64>,
    /// Monotonic count of catalog WRITE attempts — every `add_schema` and
    /// every `declare_field`, counted at the entrypoint.
    ///
    /// This exists so "no schema registration during release or install" is
    /// an observation rather than a claim: read the counter before a release
    /// and after an install and the two values must be equal. Counting at
    /// the entrypoint makes the check conservative — a rejected write still
    /// counts, so the proof cannot pass by a write failing quietly.
    /// Surfaced as `schema_writes` on `GET /v1/health`. In-memory, so it
    /// resets on restart; the proof reads it twice inside one process.
    pub schema_writes: Arc<AtomicU64>,
    /// Canonical app registry: `app_id` → [`AppRecord`] (app_identity
    /// v3.1, Lane B2b). First-write-wins, immutable. Populated by
    /// `POST /v1/apps` and surfaced in `GET /v1/snapshot`'s `apps[]`.
    pub apps: Arc<RwLock<HashMap<String, AppRecord>>>,
    /// Published releases, keyed by `release_id` — the SHA-256 of the
    /// canonical release manifest (`/v2`, see [`crate::app_release`]).
    /// Content-addressed and immutable: a release is never edited, only
    /// annotated with a revocation. Every read is an exact-key point get.
    pub app_releases: Arc<RwLock<HashMap<String, crate::app_release::ReleaseRecord>>>,
    /// Release channels, keyed by [`crate::app_release::channel_key`]
    /// (`<app_id>\x1f<channel>`). Holds the app's *desired* release id and
    /// the generation counter that makes a stale re-point fail with a
    /// conflict instead of silently winning.
    pub app_channels: Arc<RwLock<HashMap<String, crate::app_release::ChannelRecord>>>,
    /// Declared fields: an app declares a field, gets a handle, and reuses it
    /// across schemas so their data stays in step
    /// (brain `design-lastdb-declared-fields`).
    ///
    /// Distinct from [`Self::canonical_fields`], which is the *global* registry
    /// answering "what does this field mean" and stays exactly as it is. This
    /// one answers "should a write here land there", which needs a scope one
    /// level narrower than meaning.
    pub declared_fields: Arc<RwLock<crate::declared_fields::DeclaredFieldRegistry>>,
    pub(crate) declared_field_writes: Arc<tokio::sync::Mutex<()>>,
    /// App-identity verification config (trusted exemem root pubkeys,
    /// deployment env, revoked dev pubkeys). Set once at startup via
    /// [`SchemaServiceState::configure_app_identity`]; defaults to empty
    /// (enforcement inactive — see `app_identity` module docs).
    pub app_identity: Arc<RwLock<crate::app_identity::AppIdentityConfig>>,
    /// Low-cardinality schema matching counters for operators. Labels are
    /// stable enums controlled by code, never schema names or user data.
    pub schema_match_telemetry: Arc<RwLock<SchemaMatchTelemetrySnapshot>>,
    /// Feature-flagged enforcement config for unowned shared schema
    /// mutations. Defaults to observe-only; startup config can enable it via
    /// `SCHEMA_MUTATION_GATE_ENFORCE=true`.
    pub schema_mutation_gate_config: Arc<RwLock<SchemaMutationGateConfig>>,
    /// Quota store for the schema mutation gate. Challenges are stateless HMAC
    /// values; the default quota store is in-memory, and Lambda can swap in a
    /// TTL-backed adapter without changing the HTTP contract.
    pub schema_mutation_gate_store: SchemaMutationGateStore,
    /// Startup snapshot. Shells build this once; core does not read process env.
    pub schema_config: Arc<crate::schema_config::SchemaConfig>,
    /// Process clock. The shell passes this in; core does not read the clock.
    pub(crate) clock: Arc<dyn crate::clock::Clock>,
    /// Dedicated-thread runtime. The shell passes this in.
    pub(crate) runtime: Arc<dyn crate::blocking_runtime::BlockingRuntime>,
    /// Random ids. The shell passes this in; core does not read OS entropy.
    pub(crate) ids: Arc<dyn crate::ids::IdGen>,
}

mod queries;
