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

use std::collections::BTreeMap;

use schema_types::FoldDbResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::lock_helpers::read_lock;
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

/// The publishable schema-embedding artifact: the three canonical embedding
/// classes plus enough metadata to validate them against a local node's
/// embedder and schema snapshot before import.
///
/// This is the object stored at the content-addressed R2 key. Its SHA-256
/// (see [`artifact_sha256_hex`]) is the content address and is recorded in
/// the [`SchemaEmbeddingArtifactManifest`] so the two can be cross-checked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaEmbeddingArtifact {
    /// See [`SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION`].
    pub format_version: u32,
    /// Stable identity of the embedder that produced these vectors (e.g.
    /// `"fastembed/all-MiniLM-L6-v2"`). A node refuses an artifact whose
    /// `embedder_id` does not equal its own embedder's id — vectors from
    /// different models live in different spaces and cannot be mixed.
    pub embedder_id: String,
    /// SHA-256 (lowercase hex) of the schema snapshot these embeddings were
    /// computed against. Content-addresses the artifact to a specific
    /// registry state so a node never imports embeddings that don't match
    /// the schemas it just installed.
    pub schema_snapshot_hash: String,
    /// RFC 3339 UTC timestamp produced by the artifact builder.
    pub generated_at: String,
    /// Embedding vector length every record must have (e.g. 384). A record
    /// whose vector length differs is rejected at validation — a corrupt or
    /// mixed-model artifact.
    pub dimensions: u32,
    /// `descriptive_name` → vector. Sorted by `target_id`.
    pub descriptive_names: Vec<EmbeddingVectorRecord>,
    /// `"<descriptive_name>:<field_name>"` → vector. Sorted by `target_id`.
    /// The expensive class.
    pub schema_field_contexts: Vec<EmbeddingVectorRecord>,
    /// canonical field name → vector. Sorted by `target_id`.
    pub canonical_fields: Vec<EmbeddingVectorRecord>,
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

/// Which object a key names inside one artifact's content-addressed
/// directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactObject {
    /// `manifest.json` — the discovery metadata.
    Manifest,
    /// `artifact.json` — the embedding vectors themselves.
    Artifact,
}

/// Errors from validating a downloaded artifact against a local node's
/// embedder and the snapshot it just installed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EmbeddingArtifactVerifyError {
    #[error("unsupported schema-embedding artifact format version: {0}")]
    UnsupportedFormatVersion(u32),
    #[error("artifact embedder_id {artifact:?} does not match local embedder {local:?}")]
    EmbedderMismatch { artifact: String, local: String },
    #[error(
        "artifact schema_snapshot_hash {artifact:?} does not match installed snapshot {expected:?}"
    )]
    SnapshotHashMismatch { artifact: String, expected: String },
    #[error("artifact hash is not lowercase hex sha256: {field}")]
    MalformedHash { field: &'static str },
    #[error("artifact bytes hash {actual} does not match manifest artifact_sha256 {expected}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("manifest metadata field {field} disagrees with artifact body")]
    ManifestBodyMismatch { field: &'static str },
    #[error(
        "vector for {class} target {target:?} has length {actual}, expected dimensions {expected}"
    )]
    DimensionMismatch {
        class: &'static str,
        target: String,
        expected: u32,
        actual: usize,
    },
    #[error("artifact JSON parse failed: {0}")]
    ArtifactJson(String),
    #[error("manifest JSON parse failed: {0}")]
    ManifestJson(String),
}

/// SHA-256 of `bytes` as lowercase hex. Same helper contract as
/// [`crate::resolver_pack::artifact_sha256_hex`].
pub fn artifact_sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

/// Serialize an artifact to its canonical, deterministic JSON bytes.
///
/// The three embedding classes are already stored sorted by `target_id` (see
/// [`SchemaServiceState::export_embedding_artifact`]); `serde_json` preserves
/// struct field order, so identical registry state yields byte-identical
/// output — which is what makes the SHA-256 content address stable across
/// independent publishers.
pub fn serialize_artifact(
    artifact: &SchemaEmbeddingArtifact,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(artifact)
}

/// Parse artifact bytes.
pub fn parse_artifact(bytes: &[u8]) -> Result<SchemaEmbeddingArtifact, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Parse manifest bytes.
pub fn parse_manifest(bytes: &[u8]) -> Result<SchemaEmbeddingArtifactManifest, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Build the [`SchemaEmbeddingArtifactManifest`] that describes a serialized
/// artifact. `artifact_bytes` must be the exact canonical bytes produced by
/// [`serialize_artifact`] — the manifest's `artifact_sha256` and
/// `artifact_bytes` are computed from them.
pub fn build_manifest(
    artifact: &SchemaEmbeddingArtifact,
    artifact_bytes: &[u8],
) -> SchemaEmbeddingArtifactManifest {
    SchemaEmbeddingArtifactManifest {
        format_version: artifact.format_version,
        embedder_id: artifact.embedder_id.clone(),
        schema_snapshot_hash: artifact.schema_snapshot_hash.clone(),
        generated_at: artifact.generated_at.clone(),
        dimensions: artifact.dimensions,
        artifact_sha256: artifact_sha256_hex(artifact_bytes),
        artifact_bytes: artifact_bytes.len() as u64,
        counts: SchemaEmbeddingArtifactCounts {
            descriptive_names: artifact.descriptive_names.len() as u32,
            schema_field_contexts: artifact.schema_field_contexts.len() as u32,
            canonical_fields: artifact.canonical_fields.len() as u32,
        },
    }
}

/// R2 object key for one object of a content-addressed artifact.
///
/// Layout (mirrors the resolver-pack scheme):
///
/// ```text
/// schema-embedding-artifacts/{dev|prod}/artifacts/sha256/{artifact_sha256}/manifest.json
/// schema-embedding-artifacts/{dev|prod}/artifacts/sha256/{artifact_sha256}/artifact.json
/// ```
///
/// Content addressing makes every published key immutable: the same bytes
/// always land at the same key, and a byte-different artifact lands at a
/// different key. Publishers therefore never overwrite in place, and a node
/// that fetched a key can cache it forever.
pub fn schema_embedding_artifact_object_key(
    env: ArtifactEnv,
    object: ArtifactObject,
    artifact_sha256: &str,
) -> String {
    let filename = match object {
        ArtifactObject::Manifest => "manifest.json",
        ArtifactObject::Artifact => "artifact.json",
    };
    format!(
        "{R2_PREFIX}/{}/artifacts/sha256/{artifact_sha256}/{filename}",
        env.segment()
    )
}

/// Convenience: the content-addressed key of the artifact body itself.
pub fn schema_embedding_artifact_key(env: ArtifactEnv, artifact_sha256: &str) -> String {
    schema_embedding_artifact_object_key(env, ArtifactObject::Artifact, artifact_sha256)
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

/// Build the atomic [`PublishPlan`] for a manifest describing a serialized
/// artifact in the given environment.
///
/// The manifest's `artifact_sha256` is the content address every immutable
/// key is derived from, so the plan is fully determined by
/// `(env, manifest)`. The caller uploads `manifest`'s JSON to the manifest
/// key, the artifact body's JSON to the artifact key, then the manifest JSON
/// again to the `latest` pointer key.
pub fn build_publish_plan(
    env: ArtifactEnv,
    manifest: &SchemaEmbeddingArtifactManifest,
) -> PublishPlan {
    let sha = &manifest.artifact_sha256;
    PublishPlan {
        content_addressed: vec![
            PublishObject {
                key: schema_embedding_artifact_object_key(env, ArtifactObject::Manifest, sha),
                content_type: "application/json",
                precondition: PutPrecondition::IfAbsent,
            },
            PublishObject {
                key: schema_embedding_artifact_object_key(env, ArtifactObject::Artifact, sha),
                content_type: "application/json",
                precondition: PutPrecondition::IfAbsent,
            },
        ],
        latest_pointer: PublishObject {
            key: latest_compatible_manifest_pointer_key(env, &manifest.embedder_id),
            content_type: "application/json",
            precondition: PutPrecondition::Overwrite,
        },
        artifact_sha256: sha.clone(),
    }
}

/// The three validated embedding caches a node imports in place of a full
/// recompute. Field names mirror [`crate::snapshot::SnapshotEmbeddings`] so a
/// caller can move the maps straight into that struct.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ImportedEmbeddings {
    pub descriptive_names: BTreeMap<String, Vec<f32>>,
    pub fields: BTreeMap<String, Vec<f32>>,
    pub canonical_fields: BTreeMap<String, Vec<f32>>,
}

impl ImportedEmbeddings {
    /// Total entries across the three classes.
    pub fn total(&self) -> usize {
        self.descriptive_names.len() + self.fields.len() + self.canonical_fields.len()
    }
}

/// Validate a downloaded manifest + artifact against the local node's
/// embedder and the schema snapshot it just installed, then produce the
/// embedding caches to persist.
///
/// This is the local-import gate the card requires. It rejects:
///
/// * an unsupported `format_version` (manifest or artifact),
/// * an `embedder_id` that doesn't match the local embedder (vectors from a
///   different model),
/// * a `schema_snapshot_hash` that doesn't match the snapshot just installed
///   (embeddings for a different registry state),
/// * a checksum failure (the artifact bytes don't hash to the manifest's
///   `artifact_sha256`),
/// * a manifest whose metadata disagrees with the artifact body,
/// * any vector whose length isn't `dimensions` (a corrupt or truncated
///   artifact).
///
/// On success the caller writes [`ImportedEmbeddings`] into its three caches
/// and skips the minutes-scale recompute entirely. On *any* error the caller
/// falls back to local compute with visible progress — see the module docs
/// and `docs/designs/schema_embedding_artifacts_r2.md`.
///
/// `artifact_bytes` must be the exact bytes downloaded from the
/// content-addressed key (so the checksum reflects what was received, not a
/// re-serialization).
pub fn import_embedding_artifact(
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
    local_embedder_id: &str,
    installed_snapshot_hash: &str,
) -> Result<ImportedEmbeddings, EmbeddingArtifactVerifyError> {
    let manifest: SchemaEmbeddingArtifactManifest = parse_manifest(manifest_bytes)
        .map_err(|e| EmbeddingArtifactVerifyError::ManifestJson(e.to_string()))?;

    if manifest.format_version != SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION {
        return Err(EmbeddingArtifactVerifyError::UnsupportedFormatVersion(
            manifest.format_version,
        ));
    }
    validate_hash_field("schema_snapshot_hash", &manifest.schema_snapshot_hash)?;
    validate_hash_field("artifact_sha256", &manifest.artifact_sha256)?;

    // Compatibility checks happen before parsing the (potentially large)
    // body: a node can bail on a wrong-embedder artifact without paying to
    // deserialize thousands of vectors.
    if manifest.embedder_id != local_embedder_id {
        return Err(EmbeddingArtifactVerifyError::EmbedderMismatch {
            artifact: manifest.embedder_id,
            local: local_embedder_id.to_string(),
        });
    }
    if manifest.schema_snapshot_hash != installed_snapshot_hash {
        return Err(EmbeddingArtifactVerifyError::SnapshotHashMismatch {
            artifact: manifest.schema_snapshot_hash,
            expected: installed_snapshot_hash.to_string(),
        });
    }

    // Checksum the received bytes against the content address the manifest
    // claims — this is what guarantees the node never imports a partial or
    // tampered download.
    let actual_sha = artifact_sha256_hex(artifact_bytes);
    if actual_sha != manifest.artifact_sha256 {
        return Err(EmbeddingArtifactVerifyError::ChecksumMismatch {
            expected: manifest.artifact_sha256,
            actual: actual_sha,
        });
    }

    let artifact: SchemaEmbeddingArtifact = parse_artifact(artifact_bytes)
        .map_err(|e| EmbeddingArtifactVerifyError::ArtifactJson(e.to_string()))?;

    if artifact.format_version != SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION {
        return Err(EmbeddingArtifactVerifyError::UnsupportedFormatVersion(
            artifact.format_version,
        ));
    }
    // The manifest is derived from the body, so the two must agree. A
    // disagreement means the manifest was built for different bytes — treat
    // it as corruption even though the checksum passed (which would only
    // happen if a publisher mislabeled).
    if artifact.embedder_id != manifest.embedder_id {
        return Err(EmbeddingArtifactVerifyError::ManifestBodyMismatch {
            field: "embedder_id",
        });
    }
    if artifact.schema_snapshot_hash != manifest.schema_snapshot_hash {
        return Err(EmbeddingArtifactVerifyError::ManifestBodyMismatch {
            field: "schema_snapshot_hash",
        });
    }
    if artifact.dimensions != manifest.dimensions {
        return Err(EmbeddingArtifactVerifyError::ManifestBodyMismatch {
            field: "dimensions",
        });
    }

    let descriptive_names = collect_class(
        "descriptive_names",
        &artifact.descriptive_names,
        artifact.dimensions,
    )?;
    let fields = collect_class(
        "schema_field_contexts",
        &artifact.schema_field_contexts,
        artifact.dimensions,
    )?;
    let canonical_fields = collect_class(
        "canonical_fields",
        &artifact.canonical_fields,
        artifact.dimensions,
    )?;

    Ok(ImportedEmbeddings {
        descriptive_names,
        fields,
        canonical_fields,
    })
}

fn collect_class(
    class: &'static str,
    records: &[EmbeddingVectorRecord],
    dimensions: u32,
) -> Result<BTreeMap<String, Vec<f32>>, EmbeddingArtifactVerifyError> {
    let mut out = BTreeMap::new();
    for record in records {
        if record.vector.len() != dimensions as usize {
            return Err(EmbeddingArtifactVerifyError::DimensionMismatch {
                class,
                target: record.target_id.clone(),
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        out.insert(record.target_id.clone(), record.vector.clone());
    }
    Ok(out)
}

fn validate_hash_field(
    field: &'static str,
    hash: &str,
) -> Result<(), EmbeddingArtifactVerifyError> {
    let ok = hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase());
    if ok {
        Ok(())
    } else {
        Err(EmbeddingArtifactVerifyError::MalformedHash { field })
    }
}

/// Sort a cache HashMap's `(key, vector)` pairs into `EmbeddingVectorRecord`s
/// ordered by `target_id`, so the generated artifact is byte-stable.
fn records_sorted(map: &std::collections::HashMap<String, Vec<f32>>) -> Vec<EmbeddingVectorRecord> {
    let mut records: Vec<EmbeddingVectorRecord> = map
        .iter()
        .map(|(target_id, vector)| EmbeddingVectorRecord {
            target_id: target_id.clone(),
            vector: vector.clone(),
        })
        .collect();
    records.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    records
}

/// Infer the embedding dimension from the first non-empty vector across the
/// three classes. Every model this service uses emits fixed-width vectors, so
/// the first present vector's length is the artifact's `dimensions`. Returns
/// 0 for an empty registry (no vectors at all) — an artifact a node imports
/// as a valid no-op.
fn infer_dimensions(classes: &[&[EmbeddingVectorRecord]]) -> u32 {
    for class in classes {
        if let Some(first) = class.iter().find(|r| !r.vector.is_empty()) {
            return first.vector.len() as u32;
        }
    }
    0
}

impl SchemaServiceState {
    /// Assemble a standalone [`SchemaEmbeddingArtifact`] from the three
    /// in-memory embedding caches, content-addressed to `schema_snapshot_hash`
    /// (the SHA-256 of the schema snapshot these embeddings were computed
    /// against — the caller supplies it because snapshot hashing is a
    /// snapshot-layer concern).
    ///
    /// The artifact is deterministic: each class is sorted by `target_id` and
    /// the `embedder_id` is read from the injected embedder, so two services
    /// with identical registry state and the same snapshot hash produce
    /// byte-identical artifacts (and therefore the same content address).
    ///
    /// This is the *generation* half of the publish path. A publisher then
    /// [`serialize_artifact`]s it, [`build_manifest`]s the description, and
    /// walks [`build_publish_plan`] to atomically upload to R2.
    pub fn export_embedding_artifact(
        &self,
        schema_snapshot_hash: &str,
    ) -> FoldDbResult<SchemaEmbeddingArtifact> {
        let descriptive = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )?;
        let fields = read_lock(&self.field_embeddings, "field_embeddings")?;
        let canonical = read_lock(
            &self.canonical_field_embeddings,
            "canonical_field_embeddings",
        )?;

        let descriptive_names = records_sorted(&descriptive);
        let schema_field_contexts = records_sorted(&fields);
        let canonical_fields = records_sorted(&canonical);

        let dimensions = infer_dimensions(&[
            &descriptive_names,
            &schema_field_contexts,
            &canonical_fields,
        ]);

        Ok(SchemaEmbeddingArtifact {
            format_version: SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION,
            embedder_id: self.embedder.embedder_id().to_string(),
            schema_snapshot_hash: schema_snapshot_hash.to_string(),
            generated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            dimensions,
            descriptive_names,
            schema_field_contexts,
            canonical_fields,
        })
    }
}
