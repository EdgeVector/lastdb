//! Schema service snapshot envelope — wire format for `GET /v1/snapshot`
//! and `POST /v1/snapshot/import`.
//!
//! A snapshot is the schema service's persisted state at a point in time:
//! schemas, views, canonical fields, and the three embedding caches.
//!
//! See `projects/schema-service-dev-hydration` for the design rationale.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize, Serializer};

use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use crate::lock_helpers::{read_lock, write_lock};
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;

/// Snapshot wire format version. Bumped on any breaking change to
/// [`SnapshotEnvelope`]; importers refuse unknown versions rather than
/// silently degrading. The `embedder_version` field is a separate axis
/// — version stability of the JSON shape, not of the embedding model.
///
/// v2 (app_identity sandbox tier): [`AppRecord`] grew a `tier` field.
/// Old blobs without it deserialize as [`AppTier::Live`] via the field's
/// serde default, so the bump is informational for fresh exports — a v1
/// snapshot is still refused on import (dev-only path; prod never imports).
pub const SNAPSHOT_FORMAT_VERSION: u32 = 2;

/// Top-level snapshot envelope. One self-contained JSON document.
///
/// **Determinism.** [`SchemaServiceState::export_snapshot`] produces
/// byte-stable JSON for a given state: schemas/views/apps are
/// emitted in sorted order by their natural key, and `canonical_fields`
/// serializes through a sorted `BTreeMap` so clients can compare snapshots
/// for equality (e.g. cache-busting on app identity changes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    /// Wire format version. See [`SNAPSHOT_FORMAT_VERSION`].
    pub format_version: u32,
    /// Monotonic state version. Advances on every state-changing write
    /// (schema add, view add, canonical field insertion, app register).
    /// Clients use this
    /// to detect mid-flight changes when refreshing — if the version
    /// they last cached is older than the one they see now, the
    /// registry has moved.
    ///
    /// Bumped via [`SchemaServiceState::bump_state_version`]. Starts at
    /// 0 on a fresh state; persistence across restarts is intentionally
    /// out of scope for Lane B2a (every redeploy currently resets the
    /// counter — clients refresh against the actual snapshot bytes, not
    /// against the version alone).
    #[serde(default)]
    pub version: u64,
    /// RFC 3339 UTC timestamp when the snapshot was produced.
    pub captured_at: String,
    /// Stable identity of the embedder that produced the included
    /// embedding vectors (e.g. `"fastembed/all-MiniLM-L6-v2"`). Importers
    /// refuse a snapshot whose `embedder_version` does not equal their
    /// own embedder's id — vectors from different models live in
    /// different spaces and cannot be mixed.
    pub embedder_version: String,
    /// All registered schemas, keyed by their content-addressed
    /// identity hash (which is also `Schema::name`). Sorted by name in
    /// the output for deterministic JSON bytes.
    pub schemas: Vec<Schema>,
    /// Registered apps (canonical app registry). Sorted by `app_id` in
    /// the output. **Empty in Lane B2a** — Lane B2b populates this when
    /// `POST /v1/apps` lands; the field ships now so clients depending
    /// on the snapshot shape can deserialize either way without an
    /// intermediate wire change.
    #[serde(default)]
    pub apps: Vec<AppRecord>,
    /// Global canonical-field registry, keyed by canonical name.
    /// Serialized in sorted key order for determinism.
    #[serde(serialize_with = "serialize_sorted_map")]
    pub canonical_fields: HashMap<String, CanonicalField>,
    /// The three embedding caches the registry maintains.
    pub embeddings: SnapshotEmbeddings,
}

/// Registered app entry in the canonical app registry.
///
/// The shape matches the design doc at
/// `exemem-workspace/docs/designs/app_identity.md#get-v1snapshot--extended`.
/// Populated by `POST /v1/apps` (Lane B2b); dev and prod are independent
/// registries — the cross-env mirror that once filled this from a peer
/// env was decommissioned in #517.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppRecord {
    /// Lowercase ASCII, kebab-style app identifier. Matches
    /// `^[a-z][a-z0-9-]{0,39}$`. First-write-wins; immutable.
    pub app_id: String,
    /// Ed25519 public key (base64) of the developer who owns this app.
    /// Established at `POST /v1/apps` time via the `DevCert` envelope.
    pub owner_dev_pubkey: String,
    /// Strict-schema metadata rendered as plain text in consent prompts.
    pub metadata: AppMetadata,
    /// SemVer version of the latest published app record. Older persisted rows
    /// without a version deserialize as `0.0.0` so first versioned publish can
    /// move them forward monotonically.
    #[serde(default = "default_app_version")]
    pub version: String,
    /// RFC 3339 timestamp when the app was first registered.
    pub registered_at: String,
    /// Lifecycle tier. Every `POST /v1/apps` registration starts as
    /// [`AppTier::Sandbox`]; an owner-authenticated `POST /v1/apps/{id}/promote`
    /// (gated on `authorized_publisher`) flips it to [`AppTier::Live`].
    ///
    /// `#[serde(default = ...)]` makes the field back-compatible: records
    /// persisted before the tier landed (no `tier` key) deserialize as
    /// [`AppTier::Live`], so the existing prod registry keeps working
    /// without a migration.
    #[serde(default = "default_app_tier")]
    pub tier: AppTier,
    /// macOS code-signature requirement for the app's shipped binary
    /// (app-isolation invariant **I3b**): the bundle identifier (+ optional
    /// Developer ID team id) a node-side verifier checks a connecting
    /// process against. Declared by the developer in the app manifest at
    /// publish time (`POST /v1/apps`) and owner-rotatable via
    /// `PUT /v1/apps/{app_id}`.
    ///
    /// `#[serde(default)]` + skip-when-`None` keeps the field fully
    /// back-compatible in both directions (the same pattern as `tier`):
    /// legacy records without the key deserialize as `None`, and records
    /// without a code signature serialize byte-identically to before, so
    /// no [`SNAPSHOT_FORMAT_VERSION`] bump is needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_signature: Option<AppCodeSignature>,
    /// Source checkout URL for source-first installs. Use a GitHub URL.
    /// The parser still accepts a legacy `lastdb:///<slug>` value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Optional signed release tarball pointer. The object itself lives in R2
    /// and is addressed by content hash; the registry stores only the pointer
    /// metadata needed by installers to fetch and verify it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<AppArtifact>,
    /// Cross-app schemas this app declares, up front in its manifest, that it
    /// intends to **consume**. Each entry is a canonical schema/output name
    /// (e.g. `"appa/SummaryView"`); the owning app is whatever `owner_app_id`
    /// the registry resolves the name to, not parsed out of the string here.
    ///
    /// Declaring a name here is *intent*, not a grant: it drives the
    /// consent/grant request (so the prompt can say "App B wants to consume
    /// your SummaryView") and, once the owner grants it, lets scoped vector
    /// search rank the granted cross-app output. It does NOT itself bypass
    /// any read gate.
    ///
    /// `#[serde(default)]` + skip-when-empty keeps the field fully
    /// back-compatible in both directions (same pattern as `code_signature`):
    /// legacy records without the key deserialize as an empty list, and a
    /// record that declares no uses serializes byte-identically to before, so
    /// no [`SNAPSHOT_FORMAT_VERSION`] bump is needed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uses: Vec<String>,
}

pub fn default_app_version() -> String {
    "0.0.0".to_string()
}

/// The macOS code-signing identity of an app's shipped binary. See
/// [`AppRecord::code_signature`].
///
/// This describes the **binary**, not the fold namespace: a node-side
/// verifier (fold_db `access::code_signature`) binds the namespace-owning
/// `app_id` to this signing identity to build the designated-requirement
/// string a caller process must satisfy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppCodeSignature {
    /// macOS bundle identifier (e.g. `com.acme.fbrain`). <= 200 chars.
    pub bundle_identifier: String,
    /// Apple Developer ID team identifier (10 alphanumeric chars, e.g.
    /// `AB12CD34EF`). With it the node builds a production-strength
    /// Developer-ID requirement; without it, a dev-strength
    /// Apple-generic-anchor requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
}

/// Content-addressed app artifact pointer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppArtifact {
    /// Content hash of the publisher-signed tarball. R2 keys are derived from
    /// this hash rather than from mutable app/version names.
    pub hash: String,
    /// Tarball size in bytes, used as a cheap preflight before download and
    /// verification.
    pub size_bytes: u64,
}

/// Lifecycle tier of a registered app. See [`AppRecord::tier`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AppTier {
    /// Reserved + iterating: the developer owns the name and can publish
    /// schemas under it, but the app has not been promoted to production.
    Sandbox,
    /// Promoted to production. The default for legacy records that predate
    /// the tier field.
    #[default]
    Live,
}

/// Serde default for [`AppRecord::tier`] — legacy records load as `Live`.
fn default_app_tier() -> AppTier {
    AppTier::Live
}

/// Strict-schema metadata for an app. Total blob is bounded at
/// registration time (< 2 KB after JCS) so the snapshot stays small and
/// the consent prompt is safe to render as plain text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppMetadata {
    /// Display name shown in consent prompts and app lists. <= 80 chars.
    pub display_name: String,
    /// One-paragraph description. <= 500 chars.
    pub description: String,
    /// Homepage URL. <= 200 chars. Plain text in prompts.
    pub homepage_url: String,
    /// Optional icon URL. <= 200 chars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Serialize a `HashMap` through a `BTreeMap` so output ordering is
/// deterministic — load-bearing for snapshot byte stability. Generic
/// over the value type so the same helper covers `CanonicalField` (the
/// envelope's `canonical_fields` field) and `Vec<f32>` (the three
/// embedding caches inside [`SnapshotEmbeddings`]).
fn serialize_sorted_map<S, V>(map: &HashMap<String, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    let sorted: BTreeMap<&String, &V> = map.iter().collect();
    sorted.serialize(serializer)
}

/// The three pre-computed embedding caches the schema service maintains.
/// Shipping these in the snapshot lets a hydrated dev binary perform
/// semantic-similarity matching without re-embedding every name on
/// startup.
///
/// Each map serializes through a sorted `BTreeMap` so the snapshot
/// envelope stays byte-stable across processes (HashMap iteration order
/// is `RandomState`-seeded per instance, so two services with identical
/// state would otherwise produce different JSON bytes — breaking the
/// byte-equality contract documented on [`SnapshotEnvelope`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotEmbeddings {
    /// `descriptive_name` → embedding vector. Drives schema-level
    /// similarity matching.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub descriptive_names: HashMap<String, Vec<f32>>,
    /// `"<descriptive_name>:<field_name>"` → embedding vector. Drives
    /// field-level matching for proposal canonicalization.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub fields: HashMap<String, Vec<f32>>,
    /// Canonical field name → embedding vector.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub canonical_fields: HashMap<String, Vec<f32>>,
}

/// Counts returned by `POST /v1/snapshot/import` so a client can confirm
/// what landed without re-fetching the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotImportReport {
    pub schemas: usize,
    pub canonical_fields: usize,
    /// Total entries summed across the three embedding caches.
    pub embeddings: usize,
    /// Echoed straight from the imported envelope so callers can log
    /// what they just hydrated from.
    pub captured_at: String,
    pub embedder_version: String,
}

/// Offline system-schema heuristic for snapshot projection when the live
/// `system_schema_hashes` set is not available (e.g. pack publisher reading
/// a JSON file). Matches seed sources that classify as system-owned.
pub fn offline_is_system_schema(schema: &Schema) -> bool {
    matches!(
        schema.source,
        schema_types::SchemaSource::SystemSeed | schema_types::SchemaSource::StarterSeed
    )
}

/// Project a full registry snapshot to the shared-only resolver dataset.
///
/// Keeps [`crate::shared_surface::RegistrationClass::Shared`] and
/// [`crate::shared_surface::RegistrationClass::SystemOwned`] active rows,
/// drops private legacy bootstrap / unknown / superseded schemas, and
/// trims descriptive-name and field embedding caches to surviving rows.
/// Canonical-field registry + embeddings stay (they are shared vocabulary).
///
/// `is_system` classifies system seeds the same way as
/// [`SchemaServiceState::is_system_schema`] when available; pack publishers
/// should pass [`offline_is_system_schema`].
pub fn project_snapshot_shared_only<F>(
    mut envelope: SnapshotEnvelope,
    mut is_system: F,
) -> SnapshotEnvelope
where
    F: FnMut(&Schema) -> bool,
{
    use crate::shared_surface::{include_in_shared_only_projection, SharedSurfaceAttachment};
    use std::collections::HashSet;

    let attachment = SharedSurfaceAttachment::default();
    envelope
        .schemas
        .retain(|schema| include_in_shared_only_projection(schema, is_system(schema), &attachment));
    envelope.schemas.sort_by(|a, b| a.name.cmp(&b.name));

    let mut kept_desc_names: HashSet<String> = HashSet::new();
    let mut kept_schema_names: HashSet<String> = HashSet::new();
    for schema in &envelope.schemas {
        kept_schema_names.insert(schema.name.clone());
        if let Some(d) = schema.descriptive_name.as_ref() {
            kept_desc_names.insert(d.clone());
        }
        kept_desc_names.insert(schema.name.clone());
        // Namespaced descriptive-name keys used by the pack publisher.
        if let Some(owner) = schema.owner_app_id.as_deref() {
            if let Some(d) = schema.descriptive_name.as_ref() {
                kept_desc_names.insert(format!("{owner}/{d}"));
            }
        }
    }

    envelope
        .embeddings
        .descriptive_names
        .retain(|k, _| kept_desc_names.contains(k) || kept_schema_names.contains(k));

    // Field embedding keys are `{descriptive_name}:{field_name}:{desc_hash}`
    // (see pack publisher `field_embedding_cache_key`). Keep any key whose
    // first segment matches a surviving schema descriptive name.
    envelope.embeddings.fields.retain(|k, _| {
        let head = k.split(':').next().unwrap_or(k);
        kept_desc_names.contains(head) || kept_schema_names.contains(head)
    });

    envelope
}

impl SchemaServiceState {
    /// Capture the current registry state as a [`SnapshotEnvelope`].
    ///
    /// Pulls read locks across every cache the registry maintains. The
    /// envelope is fully self-contained; no follow-up calls are required
    /// for an importer to reach a working state.
    ///
    /// **Full registry** (includes private legacy bootstrap rows). For
    /// resolver-pack publishing use [`Self::export_shared_only_snapshot`].
    pub fn export_snapshot(&self) -> FoldDbResult<SnapshotEnvelope> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        let canonical_fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        let descriptive_name_embeddings = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )?;
        let field_embeddings = read_lock(&self.field_embeddings, "field_embeddings")?;
        let canonical_field_embeddings = read_lock(
            &self.canonical_field_embeddings,
            "canonical_field_embeddings",
        )?;

        // Sort the Vec fields by their natural key so the envelope serializes
        // to identical bytes across reads of identical state. canonical_fields
        // is serialized through a BTreeMap (see `serialize_canonical_fields_sorted`).
        let mut schemas_vec: Vec<Schema> = schemas
            .values()
            .filter(|schema| !crate::builtin_schemas::is_schema_org_leftover(schema))
            .cloned()
            .collect();
        schemas_vec.sort_by(|a, b| a.name.cmp(&b.name));
        let apps = read_lock(&self.apps, "apps")?;
        let mut apps_vec: Vec<AppRecord> = apps.values().cloned().collect();
        apps_vec.sort_by(|a, b| a.app_id.cmp(&b.app_id));

        Ok(SnapshotEnvelope {
            format_version: SNAPSHOT_FORMAT_VERSION,
            version: self.current_state_version(),
            captured_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            embedder_version: self.embedder.embedder_id().to_string(),
            schemas: schemas_vec,
            // Populated from the canonical app registry (app_identity v3.1
            // Lane B2b). Sorted by app_id for byte-stable snapshot output.
            apps: apps_vec,
            canonical_fields: canonical_fields.clone(),
            embeddings: SnapshotEmbeddings {
                descriptive_names: descriptive_name_embeddings.clone(),
                fields: field_embeddings.clone(),
                canonical_fields: canonical_field_embeddings.clone(),
            },
        })
    }

    /// Capture a **shared-only** snapshot for resolver-pack publishing.
    ///
    /// Equivalent to [`Self::export_snapshot`] followed by
    /// [`project_snapshot_shared_only`]. Private legacy bootstrap and
    /// unknown rows never appear in the result.
    pub fn export_shared_only_snapshot(&self) -> FoldDbResult<SnapshotEnvelope> {
        let full = self.export_snapshot()?;
        Ok(project_snapshot_shared_only(full, |schema| {
            let key = schema
                .identity_hash
                .as_deref()
                .unwrap_or(schema.name.as_str());
            self.is_system_schema(key)
                || self.is_system_schema(&schema.name)
                || offline_is_system_schema(schema)
        }))
    }

    /// Replace every persisted dimension of the registry with the contents
    /// of `envelope`. **Sled-only.**
    ///
    /// Validates `format_version` and `embedder_version` against the local
    /// binary's capabilities first; on any mismatch the call returns an
    /// error and no state is touched. On the happy path, this clears
    /// every owned Sled tree (schemas, canonical_fields, apps),
    /// writes the envelope's contents in their
    /// place, flushes, then refreshes the in-memory caches and
    /// rebuilds the descriptive_name index.
    ///
    /// **Not transactional across trees.** A crash mid-import can leave
    /// trees out of sync — the dev binary tolerates this because the
    /// remedy is `--rehydrate`, which runs the full replace from scratch.
    /// Production never imports.
    ///
    /// **`system_schema_hashes` is not modified.** It was populated by
    /// `seed_builtins()` at boot and reflects which identity hashes the
    /// local binary classifies as system. The snapshot has no opinion on
    /// system-vs-user classification (it's a per-binary policy), so
    /// import leaves the set as-is.
    pub fn import_snapshot(
        &self,
        envelope: SnapshotEnvelope,
    ) -> FoldDbResult<SnapshotImportReport> {
        if envelope.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(FoldDbError::Config(format!(
                "snapshot format_version {} not supported (this binary supports {})",
                envelope.format_version, SNAPSHOT_FORMAT_VERSION
            )));
        }

        let local_embedder_id = self.embedder.embedder_id();
        if envelope.embedder_version != local_embedder_id {
            return Err(FoldDbError::Config(format!(
                "snapshot embedder_version '{}' does not match local embedder '{}' — \
                 importing across embedder versions is unsupported",
                envelope.embedder_version, local_embedder_id
            )));
        }

        // Durable wipe + rewrite via the storage backend (Last Store locally;
        // S3 backends typically return "unsupported" and the caller should
        // hydrate cloud via normal write paths instead).
        // `import_snapshot` is sync (callers include non-async paths); run the
        // async backend ops on a dedicated thread like `SchemaServiceState::new`.
        let backend = self.storage.backend().clone();
        let schemas = envelope.schemas.clone();
        let cf_batch: Vec<(String, _)> = envelope
            .canonical_fields
            .iter()
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect();
        let apps = envelope.apps.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("schema-snapshot-import".into())
            .spawn(move || {
                let result = (|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            FoldDbError::Config(format!("snapshot import runtime failed: {e}"))
                        })?;
                    rt.block_on(async {
                        backend.clear_registry_for_snapshot_import().await?;
                        backend.save_schemas(&schemas).await?;
                        backend.save_canonical_fields(&cf_batch).await?;
                        for app in &apps {
                            backend.update_app(app).await?;
                        }
                        Ok::<(), FoldDbError>(())
                    })
                })();
                let _ = tx.send(result);
            })
            .map_err(|e| FoldDbError::Config(format!("snapshot import spawn failed: {e}")))?;
        rx.recv().map_err(|_| {
            FoldDbError::Config("snapshot import thread ended without a result".to_string())
        })??;

        let SnapshotEnvelope {
            captured_at,
            embedder_version,
            schemas: schemas_vec,
            apps: apps_vec,
            canonical_fields: canonical_fields_map,
            embeddings,
            ..
        } = envelope;

        let counts = SnapshotImportReport {
            schemas: schemas_vec.len(),
            canonical_fields: canonical_fields_map.len(),
            embeddings: embeddings.descriptive_names.len()
                + embeddings.fields.len()
                + embeddings.canonical_fields.len(),
            captured_at,
            embedder_version,
        };

        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.clear();
            for schema in schemas_vec {
                schemas.insert(schema.name.clone(), schema);
            }
        }
        {
            let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
            fields.clear();
            fields.extend(canonical_fields_map);
        }
        {
            let mut apps = write_lock(&self.apps, "apps")?;
            apps.clear();
            for app in apps_vec {
                apps.insert(app.app_id.clone(), app);
            }
        }
        {
            let mut e = write_lock(
                &self.descriptive_name_embeddings,
                "descriptive_name_embeddings",
            )?;
            e.clear();
            e.extend(embeddings.descriptive_names);
        }
        {
            let mut e = write_lock(&self.field_embeddings, "field_embeddings")?;
            e.clear();
            e.extend(embeddings.fields);
        }
        {
            let mut e = write_lock(
                &self.canonical_field_embeddings,
                "canonical_field_embeddings",
            )?;
            e.clear();
            e.extend(embeddings.canonical_fields);
        }

        self.rebuild_descriptive_name_index();

        // An import wholesale replaces the registry — clients absolutely
        // need to see the version advance.
        self.bump_state_version();

        tracing::info!(
            target: "schema_service::snapshot",
            schemas = counts.schemas,
            canonical_fields = counts.canonical_fields,
            embeddings = counts.embeddings,
            captured_at = %counts.captured_at,
            embedder_version = %counts.embedder_version,
            "Imported snapshot into local Sled storage"
        );

        Ok(counts)
    }
}

/// Whether a fresh registry holds only seed-loaded artifacts — no
/// user-authored schemas.
///
/// The dev binary uses this as the auto-hydrate guard: with
/// `--hydrate-from` set and without `--rehydrate`, it imports a remote
/// snapshot only when this predicate is true, so an in-progress local
/// edit is never silently blown away on restart.
///
/// "Seeds only" means every schema's `source` is `SystemSeed` or
/// `StarterSeed` (i.e. not `User`).
pub fn registry_is_seeds_only(state: &Arc<SchemaServiceState>) -> bool {
    use schema_types::SchemaSource;

    let Ok(schemas) = state.schemas.read() else {
        return false;
    };
    schemas
        .values()
        .all(|schema| schema.source != SchemaSource::User)
}
