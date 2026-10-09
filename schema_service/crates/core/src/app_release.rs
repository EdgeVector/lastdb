//! Exemem app registry — release, channel, and revocation records (`/v2`).
//!
//! The `/v1` app surface answers "who owns this namespace". This module
//! answers "which exact bytes is this app, right now" — the four-way bind a
//! release carries:
//!
//! 1. the **locked schema identities** the Schema Service already returned,
//! 2. the **source commit** that produced the artifact,
//! 3. the **artifact digest**, `SHA-256` over the artifact bytes in R2, and
//! 4. the **release id**, `SHA-256` over the canonical release manifest.
//!
//! ## No schema write after the lock
//!
//! Publishing a release is a *read* of the catalog, never a write. Every
//! identity in `manifest.schemas` must already resolve; an unresolved one
//! fails the publish with [`ReleaseError::UnresolvedSchema`] instead of
//! quietly registering a schema at release time. Install is the same: it
//! reads a channel, reads a release, and fetches bytes. Nothing on the
//! release or install path touches the schema catalog.
//!
//! ## Exact-key access only
//!
//! LastDB is Dynamo-style: there is no scan and no field filter. Every
//! record here has one deterministic key and every read is a point get.
//!
//! | Record | Key |
//! |---|---|
//! | Release | `release_id` |
//! | Channel | [`channel_key`]`(app_id, channel)` |
//! | App | `app_id` (owned by [`crate::app_identity`]) |
//!
//! ## Writes need a DevCert; reads need nothing
//!
//! `POST /v2/apps/{app_id}/releases`, `PUT …/channels/{channel}`, and
//! `POST …/revocations` each carry one `X-Exemem-Dev-Cert` plus one
//! `X-Signature` envelope whose purpose is pinned per route
//! ([`Purpose::AppReleasePublish`], [`Purpose::AppChannelSet`],
//! [`Purpose::AppReleaseRevoke`]). The three `GET` routes are anonymous.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use app_identity_crypto::{canonicalize, Purpose};

use crate::app_identity::{
    env_label, is_valid_app_id, verify_cert_and_signature, AppIdentityConfig, AuthFailure,
};
use crate::lock_helpers::{read_lock, write_lock};
use crate::state::SchemaServiceState;

// ─── Grammar bounds ───────────────────────────────────────────────────────

/// A channel name is a short lowercase label (`stable`, `beta`, `canary-1`).
const CHANNEL_MAX_LEN: usize = 32;
/// `release_id` and `artifact_digest` are both SHA-256 hex.
const SHA256_HEX_LEN: usize = 64;
/// A git commit id: 40 hex for SHA-1, 64 for SHA-256 repositories.
const COMMIT_MIN_LEN: usize = 7;
const COMMIT_MAX_LEN: usize = 64;
/// The app UUID that anchors the release execution identity.
const APP_UUID_MAX_LEN: usize = 64;
/// Artifact URL bound — the same ceiling the app metadata URLs use.
const ARTIFACT_URL_MAX_LEN: usize = 300;
/// Base64 Ed25519 signature over the artifact digest.
const ARTIFACT_SIGNATURE_MAX_LEN: usize = 200;
/// A release manifest may lock at most this many schemas. Generous for any
/// realistic app, and bounded so one manifest cannot bloat the registry.
const MANIFEST_SCHEMAS_MAX: usize = 256;
/// Revocation reason bound.
const REVOCATION_REASON_MAX_LEN: usize = 500;

/// The record separator that joins an app id and a channel name into one
/// exact key. Neither grammar admits `0x1f`, so the join is unambiguous.
const CHANNEL_KEY_SEPARATOR: char = '\u{1f}';

/// Exact key for a channel record: `<app_id>\x1f<channel>`.
///
/// A channel is scoped to its app, so its key needs both. Joining them with
/// a byte neither grammar allows keeps the key a single point-get string
/// rather than a prefix query.
#[must_use]
pub fn channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}{CHANNEL_KEY_SEPARATOR}{channel}")
}

/// `^[a-z][a-z0-9-]{0,31}$` — the channel-name grammar, hand-checked so the
/// crate keeps its zero-regex dependency profile.
#[must_use]
pub fn is_valid_channel(channel: &str) -> bool {
    let bytes = channel.as_bytes();
    if bytes.is_empty() || bytes.len() > CHANNEL_MAX_LEN {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Is `value` a well-formed release id or artifact digest (SHA-256 hex)?
#[must_use]
pub fn is_sha256_hex(value: &str) -> bool {
    is_lower_hex(value, SHA256_HEX_LEN)
}

// ─── Records ──────────────────────────────────────────────────────────────

/// The canonical release manifest. Its JCS bytes hash to the `release_id`,
/// so every field here is part of the release identity: change any one of
/// them and you get a different release, never a mutated one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseManifest {
    /// The registry namespace this release belongs to.
    pub app_id: String,
    /// Stable app UUID. Anchors the release execution identity
    /// `release:<app_uuid>:<release_id>:<activation_epoch>` on the host, so
    /// renaming a namespace cannot silently re-point a running release.
    pub app_uuid: String,
    /// The final schema identities the Schema Service returned during
    /// development, copied from the app lockfile without change. A
    /// `BTreeMap` so the JCS bytes — and therefore the release id — do not
    /// depend on insertion order.
    pub schemas: BTreeMap<String, String>,
    /// The commit that produced the artifact.
    pub source_commit: String,
    /// `SHA-256` of the artifact bytes stored in R2.
    pub artifact_digest: String,
    /// Where the artifact bytes live.
    pub artifact_url: String,
    /// Base64 Ed25519 signature over `artifact_digest`, by the publisher.
    pub artifact_signature: String,
}

/// A published release: the immutable manifest plus registry bookkeeping.
///
/// The manifest never changes. Revocation annotates the *record*, which is
/// why `revoked_at` lives here and not inside the hashed manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseRecord {
    /// `SHA-256` over `JCS(manifest)`. The exact key for this record.
    pub release_id: String,
    pub manifest: ReleaseManifest,
    /// Ed25519 public key (base64) of the developer that published it.
    pub publisher_dev_pubkey: String,
    /// RFC 3339 publish timestamp.
    pub published_at: String,
    /// Set by `POST /v2/apps/{app_id}/revocations`. `None` while live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_reason: Option<String>,
}

impl ReleaseRecord {
    #[must_use]
    pub fn revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    /// The anonymous `GET /v2/releases/{release_id}` body.
    #[must_use]
    pub fn to_public_json(&self) -> Value {
        json!({
            "release_id": self.release_id,
            "app_id": self.manifest.app_id,
            "manifest": self.manifest,
            "publisher_dev_pubkey": self.publisher_dev_pubkey,
            "published_at": self.published_at,
            "revoked": self.revoked(),
            "revoked_at": self.revoked_at,
            "revocation_reason": self.revocation_reason,
        })
    }
}

/// A channel: the app's currently *desired* release, under a generation
/// counter that makes concurrent re-pointing fail loudly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelRecord {
    pub app_id: String,
    pub channel: String,
    pub release_id: String,
    /// Monotonic. A writer sends the generation it read; the registry
    /// accepts the write only if that value still matches, then stores
    /// `generation + 1`. A stale writer gets a conflict, never a silent
    /// overwrite.
    pub generation: u64,
    pub updated_at: String,
}

impl ChannelRecord {
    /// The anonymous `GET /v2/apps/{app_id}/channels/{channel}` body.
    #[must_use]
    pub fn to_public_json(&self) -> Value {
        json!({
            "app_id": self.app_id,
            "channel": self.channel,
            "release_id": self.release_id,
            "generation": self.generation,
            "updated_at": self.updated_at,
        })
    }
}

// ─── Release id ───────────────────────────────────────────────────────────

/// `release_id = SHA-256(JCS(manifest))`.
///
/// The value is content-addressed, so two publishers that submit the same
/// manifest submit the same release, and no publisher can move a release id
/// onto different bytes.
///
/// # Errors
/// Returns the canonicalization failure if the manifest does not serialize
/// to canonical JSON (NaN, non-string keys — unreachable for this type).
pub fn compute_release_id(manifest: &ReleaseManifest) -> Result<String, String> {
    let value = serde_json::to_value(manifest)
        .map_err(|e| format!("release manifest does not serialize: {e}"))?;
    compute_release_id_from_value(&value)
}

/// [`compute_release_id`] over an already-parsed manifest body — used on the
/// request path so the digest covers exactly the JSON the developer signed.
///
/// # Errors
/// Returns the canonicalization failure.
pub fn compute_release_id_from_value(manifest: &Value) -> Result<String, String> {
    let bytes = canonicalize(manifest).map_err(|e| format!("manifest canonicalize failed: {e}"))?;
    Ok(schema_types::hex::sha256_hex(&bytes))
}

/// `SHA-256` hex of raw bytes — the artifact-digest check an installer runs
/// against the bytes it downloaded.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

// ─── Validation ───────────────────────────────────────────────────────────

fn validate_manifest(manifest: &ReleaseManifest) -> Result<(), String> {
    if !is_valid_app_id(&manifest.app_id) {
        return Err("manifest app_id does not match ^[a-z][a-z0-9-]{0,39}$".to_string());
    }
    if manifest.app_uuid.trim().is_empty() || manifest.app_uuid.len() > APP_UUID_MAX_LEN {
        return Err(format!(
            "manifest app_uuid must be 1..={APP_UUID_MAX_LEN} characters"
        ));
    }
    if manifest.schemas.is_empty() {
        return Err("manifest schemas must lock at least one schema identity".to_string());
    }
    if manifest.schemas.len() > MANIFEST_SCHEMAS_MAX {
        return Err(format!(
            "manifest locks {} schemas; the bound is {MANIFEST_SCHEMAS_MAX}",
            manifest.schemas.len()
        ));
    }
    for (name, identity) in &manifest.schemas {
        if name.trim().is_empty() {
            return Err("manifest schemas contains an empty schema name".to_string());
        }
        if identity.trim().is_empty() {
            return Err(format!("manifest schema '{name}' has an empty identity"));
        }
    }
    let commit_len = manifest.source_commit.len();
    if !(COMMIT_MIN_LEN..=COMMIT_MAX_LEN).contains(&commit_len)
        || !manifest
            .source_commit
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "manifest source_commit must be {COMMIT_MIN_LEN}..={COMMIT_MAX_LEN} lowercase hex characters"
        ));
    }
    if !is_sha256_hex(&manifest.artifact_digest) {
        return Err("manifest artifact_digest must be 64 lowercase hex characters".to_string());
    }
    if manifest.artifact_url.trim().is_empty() || manifest.artifact_url.len() > ARTIFACT_URL_MAX_LEN
    {
        return Err(format!(
            "manifest artifact_url must be 1..={ARTIFACT_URL_MAX_LEN} characters"
        ));
    }
    if manifest.artifact_signature.trim().is_empty()
        || manifest.artifact_signature.len() > ARTIFACT_SIGNATURE_MAX_LEN
    {
        return Err(format!(
            "manifest artifact_signature must be 1..={ARTIFACT_SIGNATURE_MAX_LEN} characters"
        ));
    }
    Ok(())
}

// ─── Errors ───────────────────────────────────────────────────────────────

/// Every way a `/v2` write can be refused. Each maps to exactly one status
/// and one stable `reason` string, so a client can branch on the reason
/// rather than parse prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    /// Body did not parse, or a field broke its grammar.
    InvalidManifest(String),
    /// Cert missing, malformed, wrong env, or not signed by a trusted root.
    CertInvalid,
    CertExpired,
    /// The `X-Signature` envelope did not verify against the cert's key, or
    /// its purpose or payload hash did not match this request.
    EnvelopeInvalid,
    /// The developer's key sits on the offline revocation denylist.
    DevRevoked,
    /// No trusted root is configured — the deployment cannot verify anything.
    NotConfigured,
    /// The signer is not the app's registered owner.
    NotAppOwner,
    /// `manifest.app_id` is not a registered app.
    UnknownApp(String),
    /// The path `app_id` and the signed body disagree.
    AppIdMismatch,
    /// A locked schema identity does not resolve in the catalog. The release
    /// is refused rather than registering the schema at release time.
    UnresolvedSchema(String),
    /// The release id in a channel or revocation write is not published.
    UnknownRelease(String),
    /// A channel write carried a generation that is no longer current.
    GenerationConflict {
        expected: u64,
        supplied: u64,
    },
    Internal(String),
}

impl ReleaseError {
    /// The HTTP status and body for this failure.
    #[must_use]
    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidManifest(detail) => (
                400,
                json!({ "reason": "invalid_manifest", "detail": detail }),
            ),
            Self::CertInvalid => (401, json!({ "reason": "cert_invalid" })),
            Self::CertExpired => (401, json!({ "reason": "cert_expired" })),
            Self::EnvelopeInvalid => (401, json!({ "reason": "envelope_invalid" })),
            Self::DevRevoked => (403, json!({ "reason": "dev_revoked" })),
            Self::NotConfigured => (503, json!({ "reason": "app_identity_not_configured" })),
            Self::NotAppOwner => (403, json!({ "reason": "not_app_owner" })),
            Self::UnknownApp(app_id) => (404, json!({ "reason": "unknown_app", "app_id": app_id })),
            Self::AppIdMismatch => (400, json!({ "reason": "app_id_mismatch" })),
            Self::UnresolvedSchema(name) => (
                422,
                json!({ "reason": "unresolved_schema", "schema": name }),
            ),
            Self::UnknownRelease(release_id) => (
                404,
                json!({ "reason": "unknown_release", "release_id": release_id }),
            ),
            Self::GenerationConflict { expected, supplied } => (
                409,
                json!({
                    "reason": "generation_conflict",
                    "expected_generation": expected,
                    "supplied_generation": supplied,
                }),
            ),
            Self::Internal(detail) => (500, json!({ "reason": "internal", "detail": detail })),
        }
    }

    fn metric_status(&self) -> &'static str {
        match self {
            Self::InvalidManifest(_) => "invalid_manifest",
            Self::CertInvalid => "cert_invalid",
            Self::CertExpired => "cert_expired",
            Self::EnvelopeInvalid => "envelope_invalid",
            Self::DevRevoked => "dev_revoked",
            Self::NotConfigured => "not_configured",
            Self::NotAppOwner => "not_app_owner",
            Self::UnknownApp(_) => "unknown_app",
            Self::AppIdMismatch => "app_id_mismatch",
            Self::UnresolvedSchema(_) => "unresolved_schema",
            Self::UnknownRelease(_) => "unknown_release",
            Self::GenerationConflict { .. } => "generation_conflict",
            Self::Internal(_) => "internal",
        }
    }
}

fn map_auth_failure(failure: &AuthFailure) -> ReleaseError {
    match failure {
        AuthFailure::CertExpired => ReleaseError::CertExpired,
        AuthFailure::EnvelopeInvalid => ReleaseError::EnvelopeInvalid,
        AuthFailure::DevRevoked => ReleaseError::DevRevoked,
        AuthFailure::CertInvalid => ReleaseError::CertInvalid,
    }
}

/// What a successful `POST /v2/apps/{app_id}/releases` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleasePublishOutcome {
    /// The release id was new. 201.
    Created(ReleaseRecord),
    /// The same manifest was already published. 200 — a release id is the
    /// digest of its manifest, so a repeat publish is byte-identical by
    /// construction and re-publishing is safe to retry.
    Idempotent(ReleaseRecord),
}

impl ReleasePublishOutcome {
    #[must_use]
    pub fn record(&self) -> &ReleaseRecord {
        match self {
            Self::Created(r) | Self::Idempotent(r) => r,
        }
    }

    /// The HTTP status and body.
    #[must_use]
    pub fn to_http(&self) -> (u16, Value) {
        let (status, label) = match self {
            Self::Created(_) => (201, "created"),
            Self::Idempotent(_) => (200, "idempotent"),
        };
        let record = self.record();
        (
            status,
            json!({
                "status": label,
                "release_id": record.release_id,
                "app_id": record.manifest.app_id,
                "artifact_digest": record.manifest.artifact_digest,
                "source_commit": record.manifest.source_commit,
                "published_at": record.published_at,
            }),
        )
    }
}

// ─── State entry points ───────────────────────────────────────────────────

impl SchemaServiceState {
    /// Point get of one release by its exact key.
    #[must_use]
    pub fn get_release(&self, release_id: &str) -> Option<ReleaseRecord> {
        read_lock(&self.app_releases, "app_releases")
            .ok()
            .and_then(|releases| releases.get(release_id).cloned())
    }

    /// Point get of one channel by its exact key.
    #[must_use]
    pub fn get_channel(&self, app_id: &str, channel: &str) -> Option<ChannelRecord> {
        read_lock(&self.app_channels, "app_channels")
            .ok()
            .and_then(|channels| channels.get(&channel_key(app_id, channel)).cloned())
    }

    /// `POST /v2/apps/{app_id}/releases` — publish an immutable release.
    ///
    /// `body` is the parsed request JSON `{ "manifest": … }`, and it is the
    /// exact object the `app_release_publish` envelope signed. The release id
    /// is the digest of `body["manifest"]`, so what the developer signed and
    /// what the registry keys on are the same bytes.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn publish_release(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleasePublishOutcome, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .publish_release_inner(&config, app_id, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "release_publish",
            match &result {
                Ok(ReleasePublishOutcome::Created(_)) => "created",
                Ok(ReleasePublishOutcome::Idempotent(_)) => "idempotent",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn publish_release_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleasePublishOutcome, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        let manifest_value = body
            .get("manifest")
            .cloned()
            .ok_or_else(|| ReleaseError::InvalidManifest("body has no 'manifest'".to_string()))?;
        let manifest: ReleaseManifest = serde_json::from_value(manifest_value.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed manifest: {e}")))?;
        validate_manifest(&manifest).map_err(ReleaseError::InvalidManifest)?;
        if manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppReleasePublish,
        )
        .map_err(|f| map_auth_failure(&f))?;

        // Ownership: only the registered owner of the namespace publishes
        // releases into it.
        let app = self
            .get_app(app_id)
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        // Every locked identity must already resolve. This is the gate that
        // keeps the release path read-only against the catalog: an
        // unresolved schema fails the publish, it does not get registered.
        for (name, identity) in &manifest.schemas {
            let resolved = self
                .get_schema_by_name(identity)
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if resolved.is_none() {
                return Err(ReleaseError::UnresolvedSchema(name.clone()));
            }
        }

        // The id is the digest of the manifest the developer signed, not of
        // a re-serialization of our parsed struct.
        let release_id =
            compute_release_id_from_value(&manifest_value).map_err(ReleaseError::Internal)?;

        {
            let releases = read_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if let Some(existing) = releases.get(&release_id) {
                return Ok(ReleasePublishOutcome::Idempotent(existing.clone()));
            }
        }

        let record = ReleaseRecord {
            release_id: release_id.clone(),
            manifest,
            publisher_dev_pubkey: verified.dev_pubkey,
            published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            revoked_at: None,
            revocation_reason: None,
        };

        // Insert-if-absent under one write lock: a concurrent publish of the
        // same manifest converges on the first winner rather than racing.
        let stored = {
            let mut releases = write_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if let Some(existing) = releases.get(&release_id) {
                return Ok(ReleasePublishOutcome::Idempotent(existing.clone()));
            }
            releases.insert(release_id, record.clone());
            record
        };
        self.persist_release(&stored).await?;
        Ok(ReleasePublishOutcome::Created(stored))
    }

    /// `PUT /v2/apps/{app_id}/channels/{channel}` — point a channel at a
    /// release under a generation check.
    ///
    /// `body` is `{ app_id, channel, release_id, generation }`, signed with
    /// purpose `app_channel_set`. `generation` is what the writer read; a
    /// stale value fails with [`ReleaseError::GenerationConflict`] instead of
    /// clobbering a concurrent write.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn set_channel(
        &self,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ChannelRecord, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .set_channel_inner(&config, app_id, channel, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "channel_set",
            match &result {
                Ok(_) => "updated",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn set_channel_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ChannelRecord, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        if !is_valid_app_id(app_id) {
            return Err(ReleaseError::InvalidManifest(
                "app_id does not match ^[a-z][a-z0-9-]{0,39}$".to_string(),
            ));
        }
        if !is_valid_channel(channel) {
            return Err(ReleaseError::InvalidManifest(
                "channel does not match ^[a-z][a-z0-9-]{0,31}$".to_string(),
            ));
        }

        #[derive(Deserialize)]
        struct ChannelSetRequest {
            app_id: String,
            channel: String,
            release_id: String,
            generation: u64,
        }
        let request: ChannelSetRequest = serde_json::from_value(body.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed body: {e}")))?;
        if request.app_id != app_id || request.channel != channel {
            return Err(ReleaseError::AppIdMismatch);
        }
        if !is_sha256_hex(&request.release_id) {
            return Err(ReleaseError::InvalidManifest(
                "release_id must be 64 lowercase hex characters".to_string(),
            ));
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppChannelSet,
        )
        .map_err(|f| map_auth_failure(&f))?;
        let app = self
            .get_app(app_id)
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        // A channel can only point at a release that exists, in this app.
        let release = self
            .get_release(&request.release_id)
            .ok_or_else(|| ReleaseError::UnknownRelease(request.release_id.clone()))?;
        if release.manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        let key = channel_key(app_id, channel);
        let record = {
            let mut channels = write_lock(&self.app_channels, "app_channels")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            let current = channels.get(&key).map_or(0, |c| c.generation);
            if current != request.generation {
                return Err(ReleaseError::GenerationConflict {
                    expected: current,
                    supplied: request.generation,
                });
            }
            let record = ChannelRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                release_id: request.release_id,
                generation: current + 1,
                updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            };
            channels.insert(key, record.clone());
            record
        };
        self.persist_channel(&record).await?;
        Ok(record)
    }

    /// `POST /v2/apps/{app_id}/revocations` — revoke a published release.
    ///
    /// The manifest stays immutable; revocation annotates the record, and
    /// an installed host reacts on its next drift check.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn revoke_release(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleaseRecord, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .revoke_release_inner(&config, app_id, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "release_revoke",
            match &result {
                Ok(_) => "revoked",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn revoke_release_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleaseRecord, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        #[derive(Deserialize)]
        struct RevokeRequest {
            app_id: String,
            release_id: String,
            #[serde(default)]
            reason: Option<String>,
        }
        let request: RevokeRequest = serde_json::from_value(body.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed body: {e}")))?;
        if request.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }
        if !is_sha256_hex(&request.release_id) {
            return Err(ReleaseError::InvalidManifest(
                "release_id must be 64 lowercase hex characters".to_string(),
            ));
        }
        if let Some(reason) = &request.reason {
            if reason.len() > REVOCATION_REASON_MAX_LEN {
                return Err(ReleaseError::InvalidManifest(format!(
                    "reason must be at most {REVOCATION_REASON_MAX_LEN} characters"
                )));
            }
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppReleaseRevoke,
        )
        .map_err(|f| map_auth_failure(&f))?;
        let app = self
            .get_app(app_id)
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        let record = {
            let mut releases = write_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            let existing = releases
                .get_mut(&request.release_id)
                .ok_or_else(|| ReleaseError::UnknownRelease(request.release_id.clone()))?;
            if existing.manifest.app_id != app_id {
                return Err(ReleaseError::AppIdMismatch);
            }
            if existing.revoked_at.is_none() {
                existing.revoked_at =
                    Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
                existing.revocation_reason = request.reason;
            }
            existing.clone()
        };
        self.persist_release(&record).await?;
        Ok(record)
    }

    async fn persist_release(&self, record: &ReleaseRecord) -> Result<(), ReleaseError> {
        self.storage
            .backend()
            .save_release(record)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))
    }

    async fn persist_channel(&self, record: &ChannelRecord) -> Result<(), ReleaseError> {
        self.storage
            .backend()
            .save_channel(record)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))
    }

    /// Replace the in-memory release + channel stores from a backend load.
    /// Called once at startup, next to the app-registry load.
    ///
    /// # Errors
    /// Returns a lock-poison error.
    pub fn install_loaded_releases(
        &self,
        releases: HashMap<String, ReleaseRecord>,
        channels: HashMap<String, ChannelRecord>,
    ) -> Result<(), schema_types::FoldDbError> {
        {
            let mut store = write_lock(&self.app_releases, "app_releases")?;
            *store = releases;
        }
        let mut store = write_lock(&self.app_channels, "app_channels")?;
        *store = channels;
        Ok(())
    }
}

fn record_release_op(env: &str, op: &str, status: &str) {
    tracing::info!(
        target: "schema_service::app_release",
        metric = "app_release_op_total",
        env = %env,
        op = %op,
        status = %status,
        "app release registry outcome"
    );
}
