//! schema_service_core
//!
//! The registry brain of the schema service. Ported from
//! `fold_db::schema_service::*` during Phase 2 of
//! `projects/extract-schema-service-repo`.
//!
//! This crate has ZERO fastembed/ONNX dependencies — callers inject an
//! [`Embedder`] implementation via [`SchemaServiceState::new`] (local path
//! opens a **Last Store** home; cloud Lambda uses S3 via
//! [`SchemaServiceState::new_with_external`]).
//!
//! # Immutability contract
//!
//! **Everything the schema service approves is immutable forever.**
//!
//! Schemas, views, transforms, classifications, and any other artifact
//! issued by this service is content-addressed and never mutated in place
//! after approval. There is no in-place "update" path: a change ships as
//! a NEW artifact with a NEW identity, registered alongside the old one.
//! Old artifacts continue to exist and resolve until something explicitly
//! removes them (a deliberate, observable action — not a side effect of
//! upgrading).
//!
//! This is a hard contract, not a convention. Downstream correctness
//! depends on it:
//!
//! * **Derived-mutation provenance is permanently verifiable.** A
//!   `Provenance::Derived { wasm_hash, input_snapshot_hash, sources_merkle_root }`
//!   stamped at write time can be re-checked years later: the WASM
//!   identified by `wasm_hash` will still hash to the same bytes, because
//!   the schema service never reissued or replaced them. See
//!   `projects/view-compute-as-mutations` for the full chain.
//! * **Sync replay is convergent.** Two devices replaying the same log
//!   compute identical atoms because the WASM they fetch (by hash, from
//!   here) is bit-identical. There is no version-skew failure mode —
//!   a `wasm_hash` mismatch is forgery, not stale data.
//! * **fold_db needs no migration plumbing for view changes.** The local
//!   schema cache can assume a view's shape is final from registration
//!   on; reorganizing the data model means registering new schemas
//!   that field-map onto the old molecules, not editing the old schema.
//!
//! "Approved" is the gate. Pre-approval, drafts are scratchpad. Once
//! approval succeeds the artifact is frozen; any subsequent change is
//! a distinct artifact registration with its own hash.

pub mod app_identity;
pub mod app_release;
pub mod builtin_canonical_fields;
pub mod builtin_schemas;
pub mod declared_fields;
pub mod embedder;
pub mod external_persistence;
#[cfg(feature = "local-store")]
pub mod laststore_persistence;
mod lock_helpers;
/// Same-product multi-key tip regeneration (background reindex job class).
pub mod multi_key_reindex;
pub mod name_validator;
pub mod native_schema_resolver;
pub mod near_miss;
pub mod registry_index;
pub mod resolver_config;
pub mod resolver_pack;
pub mod resolver_pack_consumer;
pub mod resolver_runtime;
pub mod schema_embedding_artifact;
pub mod schema_mutation_gate;
pub mod schema_org_seeds;
pub mod schema_resolver_abi;
pub mod schema_resolver_enforce_gate;
pub mod shared_surface;
pub mod snapshot;
pub mod state;
mod state_canonicalization;
mod state_compositional;
mod state_expansion;
mod state_fields;
mod state_matching;
mod state_native_resolve;
pub use multi_key_reindex::{
    plan_keyed_tip_regeneration, KeyedMembershipIndex, RegeneratedTip, SourceRecord,
};
pub use state_expansion::{
    apply_shared_field_mappers, is_cross_key_layout, key_layout_fingerprint, shared_field_names,
    KeyLayoutFingerprint,
};
pub use state_native_resolve::{
    FieldMatchProbeHit, FieldMatchProbeItem, FieldMatchProbeRequest, FieldMatchProbeResponse,
};
pub mod types;

pub use app_identity::{
    env_label, AppIdentityConfig, AppPromoteError, AppPromoteOutcome, AppRegisterError,
    AppRegisterOutcome, AppRegisterRequest, AppUpdateError, AppUpdateOutcome, AppUpdateRequest,
    SchemaClaimError,
};
pub use declared_fields::{
    mint_declaration_id, DeclareFieldInput, DeclareFieldRequest, DeclaredField, DeclaredFieldError,
    DeclaredFieldRecord, DeclaredFieldRegistry,
};
pub use embedder::{cosine_similarity, DisabledEmbeddingModel, EmbedError, Embedder};
pub use external_persistence::ExternalSchemaPersistence;
#[cfg(feature = "local-store")]
pub use laststore_persistence::{
    collections as laststore_registry_collections, LastStoreSchemaPersistence,
};
pub use native_schema_resolver::{evaluate_native, NativeResolverError, NativeResolverInput};
pub use near_miss::{NearMissDecision, NearMissRecord};
pub use registry_index::{
    RegistryIndexEntry, RegistryIndexEnvelope, RegistryIndexSignature,
    REGISTRY_INDEX_FORMAT_VERSION,
};
pub use resolver_config::{
    AlgorithmSelector, ResolverConfig, ResolverConfigError, NATIVE_COMPONENT_COVER_ALGORITHM_ID,
    NATIVE_COMPONENT_COVER_ALGORITHM_VERSION, RESOLVER_CONFIG_FORMAT_VERSION,
};
pub use resolver_pack_consumer::{
    validate_bootstrap_base_url, FsResolverPackCache, ImportedResolverPackEmbeddings,
    LoadedResolverPack, ObjectFetchMeta, ObjectFetchResult, ResolverBootstrapConfig,
    ResolverBootstrapConfigError, ResolverPackConsumer, ResolverPackConsumerConfig,
    ResolverPackFallbackReason, ResolverPackFetchError, ResolverPackLoadOutcome,
    ResolverPackLoadSource, ResolverPackObjectStore, ResolverPackResolutionRoute,
    ResolverPackTelemetrySnapshot,
};
pub use resolver_runtime::{NativeResolverState, ResolverRuntime, RuntimeTelemetry};
pub use schema_core_resolver::{
    resolve as resolve_schema, verdict_from_output, ResolveVerdict, Unresolvable,
};
pub use schema_embedding_artifact::{
    artifact_sha256_hex as schema_embedding_artifact_sha256_hex, build_manifest,
    build_publish_plan, import_embedding_artifact, latest_compatible_manifest_pointer_key,
    schema_embedding_artifact_key, ArtifactEnv, ArtifactObject, EmbeddingArtifactVerifyError,
    EmbeddingVectorRecord, ImportedEmbeddings, PublishObject, PublishPlan, PutPrecondition,
    SchemaEmbeddingArtifact, SchemaEmbeddingArtifactCounts, SchemaEmbeddingArtifactManifest,
    SCHEMA_EMBEDDING_ARTIFACT_FORMAT_VERSION,
};
pub use schema_mutation_gate::{
    catalog_size_difficulty_bits, header_name_ascii_lowercase, log_schema_mutation_gate_result,
    max_difficulty_bits_for_challenge_ttl, node_signature_payload, pow_input, pow_satisfies,
    schema_payload_hash, SchemaMutationChallengeRequest, SchemaMutationChallengeResponse,
    SchemaMutationGateConfig, SchemaMutationGateError, SchemaMutationGateHeaders,
    SchemaMutationGateQuotaStore, SchemaMutationGateStore, HEADER_DEV_PUBKEY,
    HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE, HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC,
    HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS, HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};
pub use schema_resolver_enforce_gate::{
    schema_resolver_enforce_gate_failures, schema_resolver_enforce_gate_pass,
    SchemaResolverEnforceGateReport, DEFAULT_ENFORCE_MIN_FIELD_COVERAGE,
    DEFAULT_ENFORCE_MIN_MATCH_PRECISION, DEFAULT_ENFORCE_MIN_REQUIRED_FIELD_COVERAGE,
};
pub use shared_surface::{
    classify_legacy_schema_caller, classify_registration, include_in_shared_only_projection,
    inventory_registrations, observe_legacy_schema_caller,
    private_declare_body_is_not_shared_surface, project_shared_only_schemas,
    validate_shared_surface_request, validate_surface_metadata, LegacySchemaCallerKind,
    RegistrationClass, RegistrationInventory, SharedSurfaceAttachment, SharedSurfaceCompatibility,
    SharedSurfaceMetadata, SharedSurfaceProposal, SharedSurfaceProvenance,
    SharedSurfacePublishAttachRequest, SharedSurfacePurpose, SharedSurfaceValidationError,
    SharedSurfaceVisibility,
};
pub use snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, registry_is_seeds_only, AppArtifact,
    AppMetadata, AppRecord, AppTier, SnapshotEmbeddings, SnapshotEnvelope, SnapshotImportReport,
    SNAPSHOT_FORMAT_VERSION,
};
pub use state::{BackfillPurposeReport, SchemaServiceState, SchemaStorage};
pub use state_compositional::ComponentReuseAdvice;
pub use state_matching::{REUSE_PURPOSE_THRESHOLD, REUSE_STRUCT_FLOOR};
