//! External persistence trait for the schema service.
//!
//! This trait lets deployments plug in their own storage backend without
//! the `fold_db` library having to know about any particular cloud service.
//! The built-in local backend is **Last Store**
//! (`LastStoreSchemaPersistence`). Remote deployments (e.g. the
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
use super::near_miss::NearMissRecord;
use super::snapshot::AppRecord;
use super::types::CanonicalField;
use schema_types::FoldDbResult;
use schema_types::Schema;

/// Persistence backend for the schema service.
///
/// Implementations live outside `fold_db` (for example, in the
/// schema-infra Lambda). Each method is a single storage primitive —
/// no business logic — so backends stay easy to build and test.
#[async_trait]
pub trait ExternalSchemaPersistence: Send + Sync {
    // ============== Schemas ==============

    /// Persist a single schema. Schemas are keyed by `schema.name`
    /// (which is the content-hash identity).
    ///
    /// Must be idempotent: a second call with the same schema must
    /// succeed as a no-op.
    async fn save_schema(&self, schema: &Schema) -> FoldDbResult<()>;

    /// Persist many schemas at once. Backends whose `save_schema` is a
    /// read-modify-write of one shared blob (the S3 backend) should
    /// override this to apply the whole batch in a single RMW cycle
    /// instead of N; the default just loops the single-item form.
    async fn save_schemas(&self, schemas: &[Schema]) -> FoldDbResult<()> {
        for schema in schemas {
            self.save_schema(schema).await?;
        }
        Ok(())
    }

    /// Load every schema from storage.
    ///
    /// Called once during schema service startup to populate the
    /// in-memory cache. Returns a map from `schema.name` → Schema.
    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>>;

    // ============== Canonical fields ==============

    /// Persist a single canonical field entry keyed by `name`.
    ///
    /// Must be idempotent.
    async fn save_canonical_field(&self, name: &str, field: &CanonicalField) -> FoldDbResult<()>;

    /// Persist many canonical fields at once. Same batching contract as
    /// [`Self::save_schemas`]: blob-RMW backends should override this to
    /// do one RMW for the whole batch.
    async fn save_canonical_fields(&self, fields: &[(String, CanonicalField)]) -> FoldDbResult<()> {
        for (name, field) in fields {
            self.save_canonical_field(name, field).await?;
        }
        Ok(())
    }

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

    // ============== Canonicalization near-misses (Phase C) ==============
    //
    // Shadow-mode observability for dual-signal schema canonicalization.
    // Append-only audit log of registrations where the single-signal and
    // dual-signal algorithms produced different outcomes. Surfaced via
    // `GET /v1/canonicalization-near-misses` for τ_purpose tuning.
    //
    // Default impls are no-ops so a backend that hasn't opted in silently
    // disables persistence (shadow mode is best-effort observability — it
    // must never block a registration). Backends that care override both
    // methods together.

    /// Append a single near-miss record. Keyed by `registration_id`
    /// (a UUID v4 generated at the call site); a second call with the
    /// same id must be a no-op.
    async fn append_near_miss(&self, _record: &NearMissRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Load every persisted near-miss record. Called once at startup to
    /// populate the in-memory cache that the audit endpoint reads.
    async fn load_all_near_misses(&self) -> FoldDbResult<Vec<NearMissRecord>> {
        Ok(Vec::new())
    }

    // ============== Apps (canonical app registry, app_identity v3.1) =====
    //
    // First-write-wins, immutable. The in-memory registry enforces
    // first-write-wins before `save_app` is called; backends should make
    // `save_app` insert-if-absent so a cross-instance race can't clobber
    // the winner. Default impls are no-ops / empty so a backend that
    // predates Lane B2b silently runs without an app registry rather than
    // failing startup.

    /// Persist a single app registration, keyed by `app.app_id`. Should be
    /// insert-if-absent (first-write-wins) — never overwrite an existing
    /// `app_id` with a different owner.
    async fn save_app(&self, _app: &AppRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Persist an owner-authenticated metadata update to an existing app
    /// (app_identity v3.1 `PUT /v1/apps/{id}`). The in-memory registry
    /// already verified that the signer is the owner and that
    /// `display_name` is unchanged; implementations must overwrite the
    /// stored record's `metadata` while preserving `owner_dev_pubkey`
    /// and `registered_at`.
    async fn update_app(&self, _app: &AppRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Load every registered app, keyed by `app_id`. Called once at
    /// startup to populate the in-memory registry.
    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        Ok(HashMap::new())
    }

    // ============== App releases + channels (`/v2` registry) ============
    //
    // A release is content-addressed: its key is the SHA-256 of its
    // canonical manifest, so `save_release` is an upsert of an immutable
    // body. The only field that ever changes on an existing row is the
    // revocation annotation, which is why upsert (not insert-if-absent) is
    // the right shape here.
    //
    // Default impls are no-ops / empty so a backend that predates the `/v2`
    // registry runs without it rather than failing startup — the same
    // opt-in shape as the app registry above.

    /// Persist one release, keyed by `record.release_id`.
    async fn save_release(&self, _record: &crate::app_release::ReleaseRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Load every published release, keyed by `release_id`.
    async fn load_all_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ReleaseRecord>> {
        Ok(HashMap::new())
    }

    /// Persist one channel, keyed by
    /// [`crate::app_release::channel_key`]`(app_id, channel)`.
    async fn save_channel(&self, _record: &crate::app_release::ChannelRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Load every channel, keyed by
    /// [`crate::app_release::channel_key`]`(app_id, channel)`.
    async fn load_all_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ChannelRecord>> {
        Ok(HashMap::new())
    }

    // ============== Declared fields (coherence, not meaning) ============
    //
    // An app declares a field, gets a handle, and reuses it across schemas so
    // their data stays in step (brain `design-lastdb-declared-fields`).
    //
    // Distinct from the canonical-field registry above, which answers "what
    // does this field mean" globally and is unchanged. These answer "should a
    // write here land there", which needs a scope one level narrower.
    //
    // Default impls are no-ops / empty so a backend that predates declared
    // fields runs without them rather than failing startup — the same
    // opt-in shape as the app registry.

    /// Persist one declaration, keyed by `declaration_id`. Upsert: a
    /// re-declare adds fields or grants to an existing row, and the id itself
    /// is immutable, so overwriting the row is correct.
    async fn save_declared_field(&self, _record: &DeclaredFieldRecord) -> FoldDbResult<()> {
        Ok(())
    }

    /// Load every declaration. Called once at startup to rebuild both the
    /// id index and the `(owner, handle)` index that keeps re-declare
    /// idempotent across restarts.
    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        Ok(Vec::new())
    }

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
