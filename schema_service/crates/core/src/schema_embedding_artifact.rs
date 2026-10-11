//! Standalone schema-embedding artifact contract.
//!
//! # Why this exists
//!
//! Recomputing a local node's *right-side schema embedding cache* is
//! minutes-scale: on the current benchmark, 3,254 schema field-context
//! embeddings take ~175s batched and 1,642 canonical-field embeddings take
//! ~58s batched. Incoming-record embeddings, by contrast, are subsecond.
//! So the full-cache recompute must be a **degraded fallback**, not the
//! normal path.
//!
//! Instead, `schema_service` computes the canonical schema embeddings once
//! and publishes them as a downloadable, content-addressed artifact on
//! Cloudflare R2. A local node downloads the artifact next to its schema
//! snapshot, verifies it, imports the three embedding caches directly, and
//! skips the recompute. It falls back to local compute (with visible
//! progress) only when R2/schema_service is unavailable or the artifact
//! fails validation.
//!
//! # Relationship to `resolver_pack`
//!
//! [`crate::resolver_pack`] already defines an `EmbeddingArtifact` that ships
//! *inside* a signed resolver-pack bundle (WASM + snapshot + embeddings +
//! policy, Ed25519-signed as one unit). That is the trust-rooted delivery
//! path for the resolver. This module is the *standalone* cache-hydration
//! path: the same three embedding classes, but published and discovered on
//! their own so a plain local node can skip recompute without pulling a
//! whole resolver pack. The two formats share the `format_version = 1`
//! discipline, SHA-256 lowercase-hex content addressing, and the
//! embedder-id / snapshot-hash / format-version validation gate, so a later
//! change can unify them without a wire break.
//!
//! # The three embedding classes
//!
//! These mirror [`crate::snapshot::SnapshotEmbeddings`] one-to-one:
//!
//! * `descriptive_names` — `descriptive_name` → vector; schema-level
//!   similarity.
//! * `schema_field_contexts` — `"<descriptive_name>:<field_name>"` → vector;
//!   field-level matching (the expensive class).
//! * `canonical_fields` — canonical field name → vector.
//!
//! # Format & determinism
//!
//! The artifact serializes to canonical JSON with each class sorted by
//! `target_id`, so identical registry state produces byte-identical
//! artifact bytes — which makes the content address stable and lets two
//! independent services publish to the same R2 key.

use serde::{Deserialize, Serialize};

use crate::state::SchemaServiceState;

/// Wire-format version of [`SchemaEmbeddingArtifact`] and
/// [`SchemaEmbeddingArtifactManifest`]. Importers refuse unknown versions
/// rather than silently degrading. Kept in lockstep with
/// [`crate::resolver_pack::RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION`]
/// so the two embedding formats can converge without a break.
pub const SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION: u32 = 1;

/// R2 object-key prefix for standalone schema-embedding artifacts. Distinct
/// from the resolver-pack prefix (`schema-resolver-packs/`) so the two
/// publish paths never collide.
pub const R2_PREFIX: &str = "schema-embedding-artifacts";

/// One `(target_id, vector)` embedding entry. `target_id` is the cache key
/// for its class (a descriptive name, a `"desc:field"` context key, or a
/// canonical field name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingVectorRecord {
    pub target_id: String,
    pub vector: Vec<f32>,
}

/// Per-class counts, duplicated into the discovery manifest so a dashboard
/// or a node can size an import without downloading and parsing the whole
/// artifact body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaEmbeddingArtifactCounts {
    pub descriptive_names: u32,
    pub schema_field_contexts: u32,
    pub canonical_fields: u32,
}

/// Discovery metadata for one published artifact. This is the small JSON a
/// node fetches from the "latest compatible" pointer key to learn which
/// content-addressed artifact to download, and to verify it after download.
///
/// The manifest is intentionally self-describing: it carries the artifact's
/// SHA-256 (its content address), the embedder id and snapshot hash it was
/// built for, its byte size, and its counts. A node can therefore decide
/// *before* downloading whether the artifact is compatible with its
/// embedder, and *after* downloading whether the bytes it got hash to the
/// address the manifest claims.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaEmbeddingArtifactManifest {
    pub format_version: u32,
    pub embedder_id: String,
    pub schema_snapshot_hash: String,
    pub generated_at: String,
    pub dimensions: u32,
    /// SHA-256 (lowercase hex) of the canonical artifact JSON bytes. This is
    /// the artifact's content address; the node downloads
    /// [`schema_embedding_artifact_key`]`(env, artifact_sha256)` and checks
    /// the bytes hash back to this value.
    pub artifact_sha256: String,
    /// Byte length of the canonical artifact JSON. Lets a node bound its
    /// download and detect truncation.
    pub artifact_bytes: u64,
    pub counts: SchemaEmbeddingArtifactCounts,
}

/// Deployment environment, mirrored from the resolver-pack layout so R2 keys
/// segregate dev and prod. Kept local (not re-exported from
/// `app_identity_crypto`) so this module has no crypto dependency — a
/// standalone embedding artifact carries no signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactEnv {
    Dev,
    Prod,
}

impl ArtifactEnv {
    fn segment(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }
}

/// SHA-256 of `bytes` as lowercase hex. Same helper contract as
/// [`crate::resolver_pack::artifact_sha256_hex`].
pub fn artifact_sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

/// Parse manifest bytes.
pub fn parse_manifest(bytes: &[u8]) -> Result<SchemaEmbeddingArtifactManifest, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// R2 object key for the mutable "latest compatible" discovery pointer.
///
/// A node that only knows its own embedder id reads this key to discover the
/// most recently published compatible artifact's manifest, then follows the
/// manifest's `artifact_sha256` to the immutable body. The embedder id is
/// hashed into the key so an arbitrary model-id string can't produce an
/// unsafe key.
///
/// ```text
/// schema-embedding-artifacts/{dev|prod}/latest/format-v{N}/embedder-sha256-{H}/manifest.json
/// ```
pub fn latest_compatible_manifest_pointer_key(env: ArtifactEnv, embedder_id: &str) -> String {
    let embedder_digest = artifact_sha256_hex(embedder_id.as_bytes());
    format!(
        "{R2_PREFIX}/{}/latest/format-v{SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION}/embedder-sha256-{embedder_digest}/manifest.json",
        env.segment()
    )
}

/// One object write in an atomic publish plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishObject {
    /// R2 object key.
    pub key: String,
    /// `application/json` for both objects.
    pub content_type: &'static str,
    /// Write precondition — see [`PutPrecondition`].
    pub precondition: PutPrecondition,
}

/// Conditional-write precondition for a published object. Mirrors the
/// `schema_service_s3` write model exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutPrecondition {
    /// `If-None-Match: *` — create only if absent. Used for the immutable
    /// content-addressed objects: a 412 means the byte-identical object is
    /// already published, which the publisher treats as idempotent success.
    IfAbsent,
    /// Unconditional PUT. Used for the mutable "latest" pointer, which is
    /// meant to be overwritten to advance discovery.
    Overwrite,
}

/// A fully-resolved plan for atomically publishing one artifact to R2.
///
/// **Ordering is load-bearing.** The publisher writes the immutable
/// content-addressed objects *first* (manifest then artifact body, both
/// `IfAbsent`), and only after both succeed does it overwrite the mutable
/// `latest` pointer. A node that discovers the pointer therefore always
/// finds a fully-materialized artifact behind it — it can never observe the
/// pointer advanced to an artifact whose body isn't there yet. This is how
/// the card's "publishing is atomic; local nodes never download a partial
/// artifact" requirement is met on an object store with no multi-key
/// transaction.
///
/// The plan is a pure value so it can be unit-tested (key selection,
/// ordering, preconditions) with no network. A thin R2 executor walks
/// `content_addressed` in order, then writes `latest_pointer`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPlan {
    /// Immutable objects, in the order they must be written: manifest, then
    /// artifact body. Both use [`PutPrecondition::IfAbsent`].
    pub content_addressed: Vec<PublishObject>,
    /// The mutable discovery pointer, written last with
    /// [`PutPrecondition::Overwrite`].
    pub latest_pointer: PublishObject,
    /// The artifact's content address (SHA-256 of the serialized body),
    /// echoed for logging/observability.
    pub artifact_sha256: String,
}

impl SchemaServiceState {}
