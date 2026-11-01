//! Request, outcome and error types for app registration and schema claims.

use super::*;

/// `POST /v1/apps` request body. The signed `app_register` payload is
/// this whole object (JCS-canonicalized).
///
/// `code_signature` is optional and **skip-serialized when `None`**: an
/// old client's body (and therefore its signed payload) contains no
/// `code_signature` key at all, so its signature keeps verifying against
/// a new server, and a new client that doesn't declare one signs the
/// exact same bytes an old client would.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRegisterRequest {
    pub app_id: String,
    pub metadata: AppMetadata,
    #[serde(default = "crate::snapshot::default_app_version")]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_signature: Option<AppCodeSignature>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<AppArtifact>,
    /// Cross-app schemas/outputs the app declares it consumes
    /// ([`AppRecord::uses`]). Skip-serialized when empty so an app that
    /// declares none signs the exact bytes an older client would — the
    /// signed envelope stays back-compatible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uses: Vec<String>,
}

/// Successful `POST /v1/apps` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppRegisterOutcome {
    /// First write — the namespace is now owned. 201.
    Created(AppRecord),
    /// Same owner published a strictly newer SemVer version. 200.
    Updated(AppRecord),
    /// Same dev re-posted the same app_id + identical metadata. 200.
    Idempotent(AppRecord),
}

impl AppRegisterOutcome {
    fn record(&self) -> &AppRecord {
        match self {
            Self::Created(r) | Self::Updated(r) | Self::Idempotent(r) => r,
        }
    }

    /// HTTP status + JSON body. `env_label` is the deployment env that
    /// committed this write, echoed as the `env` string — response
    /// metadata so the caller can confirm which registry (dev or prod)
    /// saw the write. Dev and prod are independent registries; this field
    /// is **not** a mirroring claim. (Earlier wire versions named this
    /// `mirrored_envs` and shipped it as a one-element array; that name
    /// predated the cross-env mirror's decommissioning in #517 and was
    /// renamed to the plain `env` string once the last array-shaped
    /// reader, `fold_db_node`'s `folddb push`, was updated in the same
    /// monorepo PR.)
    ///
    /// `metadata` echoes the canonical metadata the registry committed so
    /// the client can confirm what was stored without a follow-up snapshot
    /// fetch. Particularly load-bearing for the `Idempotent` arm: a 200
    /// reply tells the caller "your re-post matched what's already there",
    /// and shipping the stored metadata in the same body lets them verify
    /// that match field-by-field. Matches [`AppUpdateOutcome::to_http`]
    /// so a client can read either path the same way.
    pub fn to_http(&self, env_label: &str) -> (u16, Value) {
        let status = match self {
            Self::Created(_) => 201,
            Self::Updated(_) | Self::Idempotent(_) => 200,
        };
        let outcome = match self {
            Self::Created(_) => "created",
            Self::Updated(_) => "updated",
            Self::Idempotent(_) => "idempotent",
        };
        let r = self.record();
        (
            status,
            r.mutation_json([("status", json!(outcome)), ("env", json!(env_label))]),
        )
    }
}

/// `POST /v1/apps` rejection. Maps to the discriminated 4xx in the design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppRegisterError {
    InvalidAppId,
    /// The `app_id` carries non-ASCII or ASCII control characters
    /// ([`validate_app_id_charset`] — registration-time anti-confusable
    /// guard for the exact-byte principal semantics). 400.
    InvalidAppIdCharset(String),
    /// The `app_id` is in [`RESERVED_APP_IDS`]. 400.
    ReservedAppId,
    InvalidMetadata(String),
    InvalidVersion(String),
    /// The declared `code_signature` violates the bounds
    /// ([`validate_code_signature`]). 400.
    InvalidCodeSignature(String),
    /// The declared `[uses]` list violates the bounds ([`validate_uses`]). 400.
    InvalidUses(String),
    CertInvalid,
    CertExpired,
    EnvelopeInvalid,
    /// The cert verified, but the developer is not an authorized publisher
    /// (no paid plan and no `developer_access` grant). Enforced identically
    /// in dev and prod so a registration that succeeds in dev is guaranteed
    /// to be allowed in prod, and an unauthorized free account cannot
    /// reserve an app namespace in either env. Maps to 403.
    NotAuthorizedPublisher,
    /// The developer already owns [`MAX_APPS_PER_DEVELOPER`] apps; a NEW
    /// registration is rejected. Updates and idempotent re-posts of an
    /// existing app never trip this. Maps to 429.
    QuotaExceeded,
    NonMonotonicVersion {
        current_version: String,
        requested_version: String,
    },
    Conflict {
        current_owner_dev_pubkey: String,
    },
    Internal(String),
}

impl AppRegisterError {
    pub(super) fn metric_status(&self) -> &'static str {
        match self {
            Self::InvalidAppId => "invalid_app_id",
            Self::InvalidAppIdCharset(_) => "invalid_app_id_charset",
            Self::ReservedAppId => "reserved_app_id",
            Self::InvalidMetadata(_) => "invalid_metadata",
            Self::InvalidVersion(_) => "invalid_version",
            Self::InvalidCodeSignature(_) => "invalid_code_signature",
            Self::InvalidUses(_) => "invalid_uses",
            Self::CertInvalid => "cert_invalid",
            Self::CertExpired => "cert_expired",
            Self::EnvelopeInvalid => "envelope_invalid",
            Self::NotAuthorizedPublisher => "not_authorized_publisher",
            Self::QuotaExceeded => "quota_exceeded",
            Self::NonMonotonicVersion { .. } => "non_monotonic_version",
            Self::Conflict { .. } => "conflict",
            Self::Internal(_) => "error",
        }
    }

    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidAppId => (400, invalid_app_id_body()),
            Self::InvalidAppIdCharset(detail) => {
                (400, reason_detail_body("invalid_app_id_charset", detail))
            }
            Self::ReservedAppId => (400, reason_only_body("reserved_app_id")),
            Self::InvalidMetadata(detail) => (400, reason_detail_body("invalid_metadata", detail)),
            Self::InvalidVersion(detail) => (400, reason_detail_body("invalid_version", detail)),
            Self::InvalidCodeSignature(detail) => {
                (400, reason_detail_body("invalid_code_signature", detail))
            }
            Self::InvalidUses(detail) => (400, reason_detail_body("invalid_uses", detail)),
            Self::CertInvalid => (401, reason_only_body("cert_invalid")),
            Self::CertExpired => (401, reason_only_body("cert_expired")),
            Self::EnvelopeInvalid => (401, reason_only_body("envelope_invalid")),
            Self::NotAuthorizedPublisher => (403, reason_only_body("not_authorized_publisher")),
            Self::QuotaExceeded => (
                429,
                json!({
                    "reason": "quota_exceeded",
                    "max_apps_per_developer": MAX_APPS_PER_DEVELOPER,
                }),
            ),
            Self::NonMonotonicVersion {
                current_version,
                requested_version,
            } => (
                409,
                json!({
                    "reason": "non_monotonic_version",
                    "current_version": current_version,
                    "requested_version": requested_version,
                }),
            ),
            Self::Conflict {
                current_owner_dev_pubkey,
            } => (
                409,
                json!({ "reason": "app_id_taken", "current_owner_dev_pubkey": current_owner_dev_pubkey }),
            ),
            Self::Internal(detail) => (500, reason_detail_body("internal", detail)),
        }
    }
}

/// `POST /v1/schemas` owner-app-id gate rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaClaimError {
    /// `owner_app_id` set but the cert/signature headers are missing. 401.
    CertRequired,
    /// Exemem cert lacks publication entitlement. 403.
    NotAuthorizedPublisher,
    CertInvalid,
    CertExpired,
    EnvelopeInvalid,
    /// Cert is valid but its `dev_pubkey` is not this app's owner. 401.
    CertForWrongApp,
    /// `owner_app_id` references an app that isn't registered. 403.
    AppNotRegistered,
    /// Cert's `dev_pubkey` is on the offline revocation denylist. 403.
    DevRevoked,
    /// `owner_app_id` is set but no `APP_IDENTITY_ROOT_PUBKEYS` are
    /// configured, so the cert/signature pair that should bind this publish
    /// to an app owner cannot be verified. Before this guard the path was a
    /// silent passthrough — the schema landed in the registry with its
    /// declared `owner_app_id` but without any owner verification, and a
    /// misconfigured deployment looked indistinguishable from a healthy one.
    /// Surface it as 503 `app_identity_not_configured` so an app-tagged
    /// publish against a not-yet-configured stage fails loudly. 503.
    AppIdentityNotConfigured,
    Internal(String),
}

impl SchemaClaimError {
    fn http_reason(&self) -> &'static str {
        match self {
            Self::CertRequired => "cert_required",
            Self::NotAuthorizedPublisher => "not_authorized_publisher",
            Self::CertInvalid => "cert_invalid",
            Self::CertExpired => "cert_expired",
            Self::EnvelopeInvalid => "envelope_invalid",
            Self::CertForWrongApp => "cert_for_wrong_app",
            Self::AppNotRegistered => "app_not_registered",
            Self::DevRevoked => "dev_revoked",
            Self::AppIdentityNotConfigured => "app_identity_not_configured",
            Self::Internal(_) => "internal",
        }
    }

    /// Metric name. `Internal` stays `error`. Its HTTP reason stays `internal`.
    pub(super) fn metric_status(&self) -> &'static str {
        match self {
            Self::Internal(_) => "error",
            _ => self.http_reason(),
        }
    }

    pub fn to_http(&self) -> (u16, Value) {
        let reason = self.http_reason();
        match self {
            Self::CertRequired
            | Self::CertInvalid
            | Self::CertExpired
            | Self::EnvelopeInvalid
            | Self::CertForWrongApp => (401, reason_only_body(reason)),
            Self::NotAuthorizedPublisher | Self::AppNotRegistered | Self::DevRevoked => {
                (403, reason_only_body(reason))
            }
            Self::AppIdentityNotConfigured => (503, reason_only_body(reason)),
            Self::Internal(detail) => (500, reason_detail_body(reason, detail)),
        }
    }
}
