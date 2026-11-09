//! External persistence trait for the schema service.
//!
//! This trait lets deployments plug in their own storage backend without
//! the `fold_db` library having to know about any particular cloud service.
//! The built-in local backend is **in-memory**
//! (`InMemorySchemaPersistence`). Remote deployments (e.g. the
//! schema-infra Lambda) implement this trait with S3 and construct the
//! schema service via `SchemaServiceState::new_with_external`.
//!
//! Design notes:
//! - All methods are async so implementations can talk to network services
//!   (S3, DynamoDB, etc.) without blocking the tokio runtime.
//! - The trait is intentionally low-level: it owns persistence only. The
//!   schema service in `fold_db` keeps all business logic (canonicalization,
//!   similarity, expansion, classification) — the implementation only has
//!   to answer "given this key, save/load these bytes."
//! - `save_*` methods take already-serialized domain objects. Implementations
//!   serialize to whatever on-disk format they like (JSON blob, DynamoDB
//!   attribute map, etc.).
//! - `load_all_*` methods return the full domain set because the schema
//!   service caches everything in memory at startup and serves reads from
//!   the cache.

use std::collections::HashMap;

use async_trait::async_trait;

use super::declared_fields::DeclaredFieldRecord;
use super::snapshot::AppRecord;
use super::types::CanonicalField;
use crate::CanonicalVocabularyWrite;
use schema_types::{FoldDbError, FoldDbResult, PortableSchemaDefinition, PortableSchemaId, Schema};

/// Persistence backend for the schema service.
///
/// Implementations live outside `fold_db` (for example, in the
/// schema-infra Lambda). Each method is a single storage primitive —
/// no business logic — so backends stay easy to build and test.
#[async_trait]
pub trait ExternalSchemaPersistence: Send + Sync {
    // ============== Schemas ==============

    /// Commit schemas and their admitted canonical definitions atomically.
    /// A failed commit must expose neither side. Backends must validate field
    /// conflicts inside the same transaction/conditional write as the schemas.
    async fn save_schema_definitions(
        &self,
        schemas: &[Schema],
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()>;

    /// Commit only if the durable vocabulary still matches the admission
    /// snapshot. Verify it inside the same lock/conditional write, on every
    /// retry. Unsupported backends fail closed rather than split the check.
    async fn save_schema_definitions_if_vocabulary(
        &self,
        _schemas: &[Schema],
        _vocabulary: &CanonicalVocabularyWrite,
    ) -> FoldDbResult<()> {
        Err(schema_types::FoldDbError::Config(
            "conditional vocabulary commit is not supported by this backend".into(),
        ))
    }

    /// Load every schema from storage.
    ///
    /// Called once during schema service startup to populate the
    /// in-memory cache. Returns a map from `schema.name` → Schema.
    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>>;

    /// Insert the verified version by its immutable content ID. A retry must
    /// return success only when the stored definition matches that ID.
    async fn pin_portable_schema(
        &self,
        _definition: &PortableSchemaDefinition,
    ) -> FoldDbResult<()> {
        Err(FoldDbError::Config(
            "portable schema storage is unavailable".into(),
        ))
    }

    /// Read one immutable version by ID, independently of the catalog head.
    async fn load_portable_schema(
        &self,
        _id: &PortableSchemaId,
    ) -> FoldDbResult<Option<PortableSchemaDefinition>> {
        Err(FoldDbError::Config(
            "portable schema storage is unavailable".into(),
        ))
    }

    // ============== Canonical fields ==============

    /// Load every canonical field from storage.
    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>>;

    // ============== Embeddings (persisted for fast cold start) ==============
    //
    // Embedding caches (used by semantic-similarity endpoints) used to be
    // computed at every cold start by running fastembed on every loaded
    // schema and canonical field. At a few hundred records that's tens of
    // seconds per cold start — well past Lambda's 10s init cap. Persist
    // them instead; cold start just loads the blob.
    //
    // The write path keeps fastembed so new schemas/fields added at
    // runtime can be compared against the existing cache for dedup +
    // alias detection. New embeddings from write paths get persisted
    // via `save_*_embedding` so the next cold start includes them.
    //
    // Backfill for pre-existing records happens once via
    // `POST /v1/admin/warm-embeddings`.

    /// Persist a single schema's descriptive_name embedding keyed by
    /// the schema's `identity_hash`. Backends must be idempotent.
    async fn save_descriptive_name_embedding(
        &self,
        schema_hash: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()>;

    /// Load every persisted descriptive_name embedding. Returns an
    /// empty map if no blob exists yet (first cold start after the
    /// feature lands, pre-backfill).
    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>>;

    /// Persist a single canonical field embedding keyed by field name.
    async fn save_canonical_field_embedding(
        &self,
        field_name: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()>;

    /// Load every persisted canonical field embedding.
    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>>;

    // ============== Apps (canonical app registry, app_identity v3.1) =====
    //
    // First-write-wins, immutable. The in-memory registry enforces
    // first-write-wins before `save_app` is called; backends should make
    // `save_app` insert-if-absent so a cross-instance race can't clobber
    // the winner.

    /// Persist a single app registration, keyed by `app.app_id`. Should be
    /// insert-if-absent (first-write-wins) — never overwrite an existing
    /// `app_id` with a different owner.
    async fn save_app(&self, app: &AppRecord) -> FoldDbResult<()>;

    /// Persist an owner-authenticated metadata update to an existing app
    /// (app_identity v3.1 `PUT /v1/apps/{id}`). The in-memory registry
    /// already verified that the signer is the owner and that
    /// `display_name` is unchanged; implementations must overwrite the
    /// stored record's `metadata` while preserving `owner_dev_pubkey`
    /// and `registered_at`.
    async fn update_app(&self, app: &AppRecord) -> FoldDbResult<()>;

    /// Load every registered app, keyed by `app_id`. Called once at
    /// startup to populate the in-memory registry.
    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>>;

    /// Load a single app from durable storage by `app_id`. Used by the
    /// release and channel operations to verify ownership from the source
    /// of truth before any mutation. Returns `None` if the app does not
    /// exist.
    async fn load_app(&self, app_id: &str) -> FoldDbResult<Option<AppRecord>>;

    // ============== App releases + channels (`/v2` registry) ============
    //
    // A release is content-addressed: its key is the SHA-256 of its
    // canonical manifest, so `save_release` is an upsert of an immutable
    // body. The only field that ever changes on an existing row is the
    // revocation annotation, which is why upsert (not insert-if-absent) is
    // the right shape here.

    /// Persist one release, keyed by `record.release_id`.
    async fn save_release(&self, record: &crate::app_release::ReleaseRecord) -> FoldDbResult<()>;

    /// Load every published release, keyed by `release_id`.
    async fn load_all_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ReleaseRecord>>;

    /// Read one release by its exact key. `None` means the release is absent.
    async fn load_release(
        &self,
        release_id: &str,
    ) -> FoldDbResult<Option<crate::app_release::ReleaseRecord>>;

    /// Persist one channel, keyed by
    /// [`crate::app_release::channel_key`]`(app_id, channel)`.
    async fn save_channel(&self, record: &crate::app_release::ChannelRecord) -> FoldDbResult<()>;

    /// Load every channel, keyed by
    /// [`crate::app_release::channel_key`]`(app_id, channel)`.
    async fn load_all_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ChannelRecord>>;

    /// Read one channel by its exact key. `None` means the channel is absent.
    async fn load_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> FoldDbResult<Option<crate::app_release::ChannelRecord>>;

    // ============== Declared fields (coherence, not meaning) ============
    //
    // An app declares a field, gets a handle, and reuses it across schemas so
    // their data stays in step (brain `design-lastdb-declared-fields`).
    //
    // Distinct from the canonical-field registry above, which answers "what
    // does this field mean" globally and is unchanged. These answer "should a
    // write here land there", which needs a scope one level narrower.
    //
    // A declaration and its canonical definitions share one backend commit.
    // No default may silently split publication into independent writes.

    /// Check both the declaration precondition and the admission snapshot
    /// inside one atomic commit. No unguarded fallback is permitted.
    async fn save_declared_field_definitions_if_vocabulary(
        &self,
        _record: &DeclaredFieldRecord,
        _vocabulary: &CanonicalVocabularyWrite,
        _expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        Err(schema_types::FoldDbError::Config(
            "conditional vocabulary commit is not supported by this backend".into(),
        ))
    }

    /// Load every declaration. Called once at startup to rebuild both the
    /// id index and the `(owner, handle)` index that keeps re-declare
    /// idempotent across restarts.
    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>>;

    /// Clear all schemas from durable storage (dev reset). Default:
    /// unsupported (cloud backends own their own reset semantics).
    async fn clear_all_schemas(&self) -> FoldDbResult<()> {
        Err(schema_types::FoldDbError::Config(
            "clear_all_schemas is not supported for this storage backend".to_string(),
        ))
    }

    /// Wipe schemas + canonical fields + apps before a local snapshot
    /// import. Default: unsupported.
    async fn clear_registry_for_snapshot_import(&self) -> FoldDbResult<()> {
        Err(schema_types::FoldDbError::Config(
            "snapshot import clear is not supported for this storage backend".to_string(),
        ))
    }
}
