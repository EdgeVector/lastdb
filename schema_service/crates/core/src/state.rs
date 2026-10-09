use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::embedder::{cosine_similarity, Embedder};
#[cfg(feature = "local-store")]
use crate::laststore_persistence::LastStoreSchemaPersistence;
use crate::lock_helpers::{read_lock, write_lock};
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use super::external_persistence::ExternalSchemaPersistence;
use super::near_miss::{NearMissDecision, NearMissRecord};
use super::schema_mutation_gate::{SchemaMutationGateConfig, SchemaMutationGateStore};
use super::snapshot::AppRecord;
use super::state_canonicalization::{
    gate_outcome_for_candidate, rank_candidates, CandidateConflict, CandidateSet,
    CanonicalizationGateOutcome, MatchCandidate, MatchSeam,
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
mod backfill;
mod canonical_field;
mod canonicalization_candidates;
mod collection_names;
mod idempotent_repost;
mod lifecycle;
mod loading;
mod mutation_gate;
mod near_misses;
mod persistence;
mod resolve;

pub use backfill::BackfillPurposeReport;
pub use near_misses::{DEFAULT_NEAR_MISSES_LIMIT, MAX_NEAR_MISSES_LIMIT};
pub use resolve::MAX_SCHEMA_RESOLVE_PROPOSALS;

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
/// - **Local / tests:** [`LastStoreSchemaPersistence`] (Last Store on disk)
/// - **Lambda:** S3 (and friends) via schema-infra
///
/// In-memory caches still live on [`SchemaServiceState`]; this only
/// owns durability.
#[derive(Clone)]
pub enum SchemaStorage {
    /// Caller-supplied persistence backend (Last Store, S3, …).
    External(Arc<dyn ExternalSchemaPersistence>),
}

impl SchemaStorage {
    /// The durability backend (Last Store, S3, …).
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
}

impl SchemaServiceState {
    /// Advance the monotonic state-version counter and return the new
    /// value. Call this from every state-changing write path so
    /// `GET /v1/snapshot` clients can detect mid-flight changes (app_identity
    /// v3.1, Lane B2a). Cheap (`fetch_add`); fine to call inside a critical
    /// section.
    ///
    /// The counter is over-counting-safe: the contract is monotonicity, not
    /// exactness. Bumping at the entrypoint of a mutating method even when
    /// the method ultimately no-ops (e.g. add_schema sees a duplicate) still
    /// satisfies the client contract — clients re-fetch the snapshot on
    /// version change and the bytes are unchanged so it's a cheap miss.
    pub fn bump_state_version(&self) -> u64 {
        self.state_version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Count one catalog write attempt. See [`Self::schema_writes`].
    pub fn record_schema_write(&self) -> u64 {
        self.schema_writes.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Read the catalog write counter without advancing it.
    #[must_use]
    pub fn current_schema_writes(&self) -> u64 {
        self.schema_writes.load(Ordering::SeqCst)
    }

    /// Read the current state-version counter without advancing it.
    pub fn current_state_version(&self) -> u64 {
        self.state_version.load(Ordering::SeqCst)
    }

    /// Look up the active schema bound to `(owner_app_id, descriptive_name)`
    /// in `descriptive_name_index`, verifying the resolved schema actually
    /// lives in the requested namespace before returning its identity hash.
    ///
    /// [`descriptive_name_key`] is not injective on
    /// `(Option<&str>, &str)`: `(None, "kanban/Tasks")` and
    /// `(Some("kanban"), "Tasks")` both encode to `"kanban/Tasks"`. A raw
    /// `index.get(key)` therefore can smuggle a schema from another
    /// namespace into a same-key hit — registering an app-owned
    /// `kanban/Tasks` against a legacy un-owned schema whose
    /// `descriptive_name` happens to be `"kanban/Tasks"` would otherwise
    /// route into the legacy slot (spurious expansion or a 409
    /// `DescriptiveNameConflict`). Re-resolving through the schemas map and
    /// discarding mismatched owners restores the namespace invariant the
    /// dedup index is meant to enforce.
    pub(crate) fn lookup_descriptive_name_in_namespace(
        &self,
        owner_app_id: Option<&str>,
        descriptive_name: &str,
    ) -> FoldDbResult<Option<String>> {
        let key = descriptive_name_key(owner_app_id, descriptive_name);
        let hash = {
            let index = read_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            index.get(&key).cloned()
        };
        let Some(hash) = hash else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas")?;
        if let Some(existing) = schemas.get(&hash) {
            fn normalize(s: Option<&str>) -> Option<&str> {
                s.filter(|x| !x.is_empty())
            }
            if normalize(existing.owner_app_id.as_deref()) == normalize(owner_app_id) {
                return Ok(Some(hash));
            }
        }
        Ok(None)
    }

    pub fn schema_match_telemetry_snapshot(&self) -> SchemaMatchTelemetrySnapshot {
        self.schema_match_telemetry
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    pub fn record_schema_match_outcome(
        &self,
        outcome: &'static str,
        source: &str,
        fallback_reason: Option<&str>,
    ) {
        let Ok(mut snapshot) = self.schema_match_telemetry.write() else {
            return;
        };
        *snapshot.outcomes.entry(outcome.to_string()).or_insert(0) += 1;
        *snapshot
            .sources
            .entry(safe_telemetry_label(source))
            .or_insert(0) += 1;
        if let Some(reason) = fallback_reason {
            *snapshot
                .fallback_reasons
                .entry(safe_telemetry_label(reason))
                .or_insert(0) += 1;
        }
    }

    /// Get all schema names (public accessor for Lambda integration)
    pub fn get_schema_names(&self) -> FoldDbResult<Vec<String>> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        Ok(schemas.keys().cloned().collect())
    }

    /// Get all schemas (public accessor for Lambda integration)
    pub fn get_all_schemas_cached(&self) -> FoldDbResult<Vec<Schema>> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        Ok(schemas.values().cloned().collect())
    }

    /// Get a schema by name (public accessor for Lambda integration).
    ///
    /// Accepts either a content-addressed identity hash (the stable
    /// key every schema is indexed under) or a descriptive name like
    /// `"Persona"`. Descriptive-name lookups resolve via the
    /// `descriptive_name_index`, which is populated from the stored
    /// schemas' `descriptive_name` field on every load/rebuild and
    /// does NOT depend on the embedding model — so this path works
    /// even in Lambda environments where fastembed can't fetch its
    /// ONNX weights.
    ///
    /// fold_db_node always fetches Phase 1 built-ins by descriptive
    /// name at startup, so this fallback is load-bearing for booting
    /// any node against a fresh schema service.
    pub fn get_schema_by_name(&self, name: &str) -> FoldDbResult<Option<Schema>> {
        let schemas = read_lock(&self.schemas, "schemas")?;

        // Fast path: direct lookup by content-addressed identity hash.
        if let Some(schema) = schemas.get(name).cloned() {
            return Ok(Some(schema));
        }

        // Fallback: resolve `name` as a descriptive_name, then look
        // up the resulting identity_hash. Drop the schemas lock
        // before re-acquiring to avoid lock upgrade paths.
        drop(schemas);
        let resolved_hash: Option<String> = {
            let index = read_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            index.get(name).cloned()
        };
        if let Some(hash) = resolved_hash {
            let schemas = read_lock(&self.schemas, "schemas")?;
            return Ok(schemas.get(&hash).cloned());
        }

        Ok(None)
    }

    /// Get schema count (public accessor for Lambda integration)
    pub fn get_schema_count(&self) -> usize {
        self.schemas.read().map_or(0, |s| s.len())
    }

    /// Install a pre-validated schema into the in-memory registry **only**.
    /// No persistence, no embedding.
    ///
    /// Used for starter-seed schemas committed as per-schema JSON files
    /// under `schema_service_core/data/schema_org/schemas/*.json` and
    /// loaded at cold start by
    /// [`crate::schema_org_seeds::load_validated_schemas`]. Those files
    /// are the source of truth — the Lambda binary `include_dir!`s
    /// them — so copying them into the persistent store (S3 / Sled)
    /// on every cold start would just duplicate committed data and
    /// burn Lambda init time. The descriptive_name embedding is
    /// likewise deferred: it only matters for semantic-similarity
    /// lookups and is warmed on-demand by the
    /// `POST /v1/admin/warm-embeddings` admin endpoint.
    ///
    /// Result: cold-start cost is one HashMap insert per seed —
    /// microseconds, never blows the 10s Lambda INIT cap.
    ///
    /// Responsibilities:
    ///
    /// - **Idempotency**: bails out early if a schema with the same
    ///   name (the identity_hash used as the storage key) is already
    ///   loaded. Also honors user ownership: a user-proposed schema
    ///   with the same identity_hash that arrived via `load_schemas`
    ///   before this call wins and the starter copy is skipped.
    /// - **In-memory caches**: populates `schemas` and
    ///   `descriptive_name_index` so lookups by hash or descriptive
    ///   name work immediately.
    ///
    /// NOT responsibilities:
    ///
    /// - **Persistence** — the committed JSON is already the source of
    ///   truth and will be reloaded from the binary on every cold
    ///   start. S3 / Sled holds user schemas only.
    /// - **descriptive_name embedding** — deferred to the
    ///   warm-embeddings admin route.
    ///
    /// Use this for pre-validated snapshots only. User-proposed schemas
    /// must go through [`SchemaServiceState::add_schema`] to hit
    /// validation + classification.
    pub fn insert_pre_validated_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        // Idempotent: a second call with the same identity_hash is a
        // no-op. We use schema.name as the key because by the time a
        // schema has been through the validation pipeline, its name
        // equals its identity_hash.
        {
            let schemas = read_lock(&self.schemas, "schemas")?;
            if schemas.contains_key(&schema.name) {
                return Ok(());
            }
        }

        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.insert(schema.name.clone(), schema.clone());
        }

        if let Some(ref desc_name) = schema.descriptive_name {
            let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
            if let Ok(mut index) = self.descriptive_name_index.write() {
                index.insert(key, schema.name.clone());
            }
        }

        self.bump_state_version();
        Ok(())
    }

    /// Install a pre-classified canonical field into the in-memory
    /// registry **only**. Canonical-field counterpart to
    /// [`SchemaServiceState::insert_pre_validated_schema`].
    ///
    /// Skips:
    /// - **Persistence** — committed
    ///   `validated_canonical_fields.json` is the source of truth,
    ///   reloaded from the Lambda binary on every cold start.
    /// - **Embedding** — `add_canonical_field` normally runs fastembed
    ///   (~40ms per entry, ~66s for the 1,642-entry Schema.org pool).
    ///   Deferred to `POST /v1/admin/warm-embeddings`.
    ///
    /// Idempotent: no-op if `name` is already registered. Keeps
    /// `add_canonical_field`'s "already present wins" semantics so a
    /// user's earlier canonical override isn't overwritten.
    pub fn insert_pre_validated_canonical_field(
        &self,
        name: &str,
        canonical: super::types::CanonicalField,
    ) {
        if let Ok(mut fields) = self.canonical_fields.write() {
            fields.entry(name.to_string()).or_insert(canonical);
        }
        self.bump_state_version();
    }

    /// Record an identity hash as a system/infrastructure schema.
    ///
    /// Called by `builtin_schemas::seed()` during startup for each
    /// Phase 1 built-in. Idempotent. See
    /// [`SchemaServiceState::system_schema_hashes`] for background.
    pub fn mark_system_schema(&self, identity_hash: String) {
        if let Ok(mut set) = self.system_schema_hashes.write() {
            set.insert(identity_hash);
        }
    }

    /// Mark the start/end of an authoritative seed load. Wrap each committed
    /// seeder body in `set_seeding_in_progress(true)` … `(false)` so the fuzzy
    /// reuse-before-NEW path is disabled for those `add_schema` calls — seed
    /// schemas must register exactly as committed, never fuzzily merge into a
    /// near-neighbour. See [`Self::seeding_in_progress`].
    pub fn set_seeding_in_progress(&self, on: bool) {
        self.seeding_in_progress
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether an authoritative seeder is currently loading committed schemas.
    pub(crate) fn is_seeding_in_progress(&self) -> bool {
        self.seeding_in_progress
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// True if the given schema is a system/infrastructure schema
    /// (one of the Phase 1 built-ins such as Fingerprint, Edge,
    /// Identity, Persona, etc.).
    ///
    /// Accepts either an identity hash (the canonical storage key)
    /// or a descriptive_name (`"Persona"`, `"Fingerprint"`, …). This
    /// matches `get_schema_by_name`'s lookup semantics so callers
    /// can classify a schema by whatever key they already have.
    pub fn is_system_schema(&self, schema_name_or_hash: &str) -> bool {
        if let Ok(set) = self.system_schema_hashes.read() {
            if set.contains(schema_name_or_hash) {
                return true;
            }
        }

        let resolved_hash = match self.descriptive_name_index.read() {
            Ok(index) => index.get(schema_name_or_hash).cloned(),
            Err(_) => return false,
        };

        if let Some(hash) = resolved_hash {
            if let Ok(set) = self.system_schema_hashes.read() {
                return set.contains(&hash);
            }
        }

        false
    }
}
