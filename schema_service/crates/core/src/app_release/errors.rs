//! Publish errors and outcomes.

use super::*;

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
    /// The publisher's dev key published [`PUBLISH_RATE_LIMIT`] new releases
    /// in the last [`PUBLISH_RATE_WINDOW_SECS`].
    PublishRateLimited,
    Internal(String),
}

impl ReleaseError {
    /// The HTTP status and body for this failure.
    #[must_use]
    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidManifest(detail) => (
                400,
                crate::app_identity::reason_detail_body("invalid_manifest", detail),
            ),
            Self::CertInvalid => (401, json!({ "reason": "cert_invalid" })),
            Self::CertExpired => (401, json!({ "reason": "cert_expired" })),
            Self::EnvelopeInvalid => (401, json!({ "reason": "envelope_invalid" })),
            Self::DevRevoked => (403, json!({ "reason": "dev_revoked" })),
            Self::NotConfigured => (503, json!({ "reason": "app_identity_not_configured" })),
            Self::NotAppOwner => (403, json!({ "reason": "not_app_owner" })),
            Self::UnknownApp(app_id) => (404, unknown_app_json(app_id)),
            Self::AppIdMismatch => (400, json!({ "reason": "app_id_mismatch" })),
            Self::UnresolvedSchema(name) => (
                422,
                json!({ "reason": "unresolved_schema", "schema": name }),
            ),
            Self::UnknownRelease(release_id) => (404, unknown_release_json(release_id)),
            Self::GenerationConflict { expected, supplied } => (
                409,
                json!({
                    "reason": "generation_conflict",
                    "expected_generation": expected,
                    "supplied_generation": supplied,
                }),
            ),
            Self::PublishRateLimited => (
                429,
                json!({
                    "reason": "publish_rate_limited",
                    "limit": PUBLISH_RATE_LIMIT,
                    "window_secs": PUBLISH_RATE_WINDOW_SECS,
                }),
            ),
            Self::Internal(detail) => (
                500,
                crate::app_identity::reason_detail_body("internal", detail),
            ),
        }
    }

    pub(super) fn metric_status(&self) -> &'static str {
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
            Self::PublishRateLimited => "publish_rate_limited",
            Self::Internal(_) => "internal",
        }
    }
}

pub(super) fn map_auth_failure(failure: &AuthFailure) -> ReleaseError {
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

pub(super) fn record_release_op(env: &str, op: &str, status: &str) {
    tracing::info!(
        target: "schema_service::app_release",
        metric = "app_release_op_total",
        env = %env,
        op = %op,
        status = %status,
        "app release registry outcome"
    );
}

/// Anonymous `GET` miss for an app id. A release write uses the same body.
#[must_use]
pub fn unknown_app_json(app_id: &str) -> Value {
    json!({ "reason": "unknown_app", "app_id": app_id })
}

/// Anonymous `GET` miss for a release id. A release write uses the same body.
#[must_use]
pub fn unknown_release_json(release_id: &str) -> Value {
    json!({ "reason": "unknown_release", "release_id": release_id })
}

/// Anonymous `GET` miss for one channel.
#[must_use]
pub fn unknown_channel_json(app_id: &str, channel: &str) -> Value {
    json!({ "reason": "unknown_channel", "app_id": app_id, "channel": channel })
}

/// Storage read failed. The `error` text is the storage error.
#[must_use]
pub fn registry_unavailable_json(error: &str) -> Value {
    json!({ "reason": "registry_unavailable", "error": error })
}
