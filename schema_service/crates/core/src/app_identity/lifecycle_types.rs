//! Request, outcome and error types for app update and promotion.

use super::*;

/// `PUT /v1/apps/{app_id}` request body. The signed `app_update` payload
/// is this whole object plus the path's `app_id` (the handler folds the
/// two into `{ app_id, metadata }` so the signed bytes match the same
/// shape `POST /v1/apps` signs — one canonical envelope payload across
/// the register + update paths).
/// `code_signature` semantics on update: **absent means "leave as-is"**
/// (an old client that doesn't know the field can keep updating metadata
/// without clobbering a registered signing identity); present means
/// "set/replace" — the owner's add/rotate path. There is no clear-to-`None`
/// in v1. When present, the field is folded into the signed payload
/// alongside `app_id` + `metadata`; when absent the signed payload is
/// byte-identical to the pre-code-signature shape, so old clients'
/// signatures still verify.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppUpdateRequest {
    pub metadata: AppMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_signature: Option<AppCodeSignature>,
    /// Source checkout pointer ([`AppRecord::source`]). Absent means leave
    /// unchanged on update, matching `code_signature`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Signed tarball pointer ([`AppRecord::artifact`]). Absent means leave
    /// unchanged on update, matching `code_signature`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<AppArtifact>,
    /// Updated cross-app `[uses]` declaration ([`AppRecord::uses`]). Same
    /// "absent = leave as-is, present = replace" rule as `code_signature`:
    /// an empty list (the default when the key is omitted) keeps the
    /// registered declaration; a non-empty list replaces it wholesale.
    /// Folded into the signed payload ONLY when non-empty so an old
    /// client's signature (which never saw the field) still verifies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uses: Vec<String>,
}

/// Successful `PUT /v1/apps/{app_id}` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppUpdateOutcome {
    /// Metadata changed; the registry has been updated. 200.
    Updated(AppRecord),
    /// Same owner re-posted the identical metadata. 200, no-op.
    NoChange(AppRecord),
}

impl AppUpdateOutcome {
    fn record(&self) -> &AppRecord {
        match self {
            Self::Updated(r) | Self::NoChange(r) => r,
        }
    }

    /// HTTP status + JSON body. Always 200 (the update either took effect
    /// or was a no-op; both are equally successful from the client's
    /// perspective). The `env` string reports the deployment env that
    /// committed — response metadata only, matching the `POST /v1/apps`
    /// shape so clients can read either path the same way. (See
    /// [`AppRegisterOutcome::to_http`] for the rename from the historical
    /// `mirrored_envs` array.)
    pub fn to_http(&self, env_label: &str) -> (u16, Value) {
        let r = self.record();
        (
            200,
            json!({
                "app_id": r.app_id,
                "owner_dev_pubkey": r.owner_dev_pubkey,
                "registered_at": r.registered_at,
                "metadata": r.metadata,
                "version": r.version,
                "code_signature": r.code_signature,
                "source": r.source,
                "artifact": r.artifact,
                "uses": r.uses,
                "env": env_label,
            }),
        )
    }
}

/// `PUT /v1/apps/{app_id}` rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppUpdateError {
    InvalidAppId,
    InvalidMetadata(String),
    /// The declared `code_signature` violates the bounds
    /// ([`validate_code_signature`]). 400.
    InvalidCodeSignature(String),
    /// The declared `[uses]` list violates the bounds ([`validate_uses`]). 400.
    InvalidUses(String),
    CertInvalid,
    CertExpired,
    EnvelopeInvalid,
    /// `app_id` is not registered — there is no record to update. 404.
    AppNotRegistered,
    /// Cert is valid but its `dev_pubkey` is not the registered owner of
    /// this app. A bare DevCert is NOT sufficient to mutate someone
    /// else's metadata. 401.
    NotOwner,
    /// The incoming `metadata.display_name` differs from the stored
    /// display_name. Per design, display_name is set at registration time
    /// and cannot change. 409. The other metadata fields
    /// (description / homepage_url / icon_url) can change.
    DisplayNameImmutable {
        current_display_name: String,
    },
    Internal(String),
}

impl AppUpdateError {
    pub(super) fn metric_status(&self) -> &'static str {
        match self {
            Self::InvalidAppId => "invalid_app_id",
            Self::InvalidMetadata(_) => "invalid_metadata",
            Self::InvalidCodeSignature(_) => "invalid_code_signature",
            Self::InvalidUses(_) => "invalid_uses",
            Self::CertInvalid => "cert_invalid",
            Self::CertExpired => "cert_expired",
            Self::EnvelopeInvalid => "envelope_invalid",
            Self::AppNotRegistered => "app_not_registered",
            Self::NotOwner => "not_owner",
            Self::DisplayNameImmutable { .. } => "display_name_immutable",
            Self::Internal(_) => "error",
        }
    }

    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidAppId => (400, json!({ "reason": "invalid_app_id" })),
            Self::InvalidMetadata(detail) => (
                400,
                json!({ "reason": "invalid_metadata", "detail": detail }),
            ),
            Self::InvalidCodeSignature(detail) => (
                400,
                json!({ "reason": "invalid_code_signature", "detail": detail }),
            ),
            Self::InvalidUses(detail) => {
                (400, json!({ "reason": "invalid_uses", "detail": detail }))
            }
            Self::CertInvalid => (401, json!({ "reason": "cert_invalid" })),
            Self::CertExpired => (401, json!({ "reason": "cert_expired" })),
            Self::EnvelopeInvalid => (401, json!({ "reason": "envelope_invalid" })),
            Self::NotOwner => (401, json!({ "reason": "not_owner" })),
            Self::AppNotRegistered => (404, json!({ "reason": "app_not_registered" })),
            Self::DisplayNameImmutable {
                current_display_name,
            } => (
                409,
                json!({
                    "reason": "display_name_immutable",
                    "current_display_name": current_display_name,
                }),
            ),
            Self::Internal(detail) => (500, json!({ "reason": "internal", "detail": detail })),
        }
    }
}

/// Successful `POST /v1/apps/{app_id}/promote` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppPromoteOutcome {
    /// The app was sandbox and is now live. 200.
    Promoted(AppRecord),
    /// The app was already live — idempotent no-op. 200.
    AlreadyLive(AppRecord),
}

impl AppPromoteOutcome {
    fn record(&self) -> &AppRecord {
        match self {
            Self::Promoted(r) | Self::AlreadyLive(r) => r,
        }
    }

    /// HTTP status + JSON body. Always 200 (the promotion either took
    /// effect or the app was already live; both are success). Echoes the
    /// committed record (including its `tier`) so the caller can confirm
    /// the flip without a follow-up snapshot fetch.
    pub fn to_http(&self) -> (u16, Value) {
        let r = self.record();
        (
            200,
            json!({
                "app_id": r.app_id,
                "owner_dev_pubkey": r.owner_dev_pubkey,
                "registered_at": r.registered_at,
                "metadata": r.metadata,
                "version": r.version,
                "code_signature": r.code_signature,
                "source": r.source,
                "artifact": r.artifact,
                "uses": r.uses,
                "tier": r.tier,
            }),
        )
    }
}

/// `POST /v1/apps/{app_id}/promote` rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppPromoteError {
    InvalidAppId,
    CertInvalid,
    CertExpired,
    EnvelopeInvalid,
    /// `app_id` is not registered — there is no record to promote. 404.
    AppNotRegistered,
    /// Cert is valid but its `dev_pubkey` is not the registered owner. 401.
    NotOwner,
    /// The cert verified and the signer owns the app, but the developer is
    /// not an authorized publisher (no paid plan and no `developer_access`
    /// grant). Promotion to live requires authorized-publisher status. 403.
    NotAuthorizedPublisher,
    /// Sandbox→live requires an install pointer so the public shelf is not
    /// full of apps that `lastdb app install` must reject. Either
    /// [`AppRecord::source`] (git checkout URL) or [`AppRecord::artifact`]
    /// (signed tarball pointer) must be set on the registry row (publish
    /// body or a later owner `PUT`). 400.
    MissingInstallPointer,
    Internal(String),
}

impl AppPromoteError {
    pub(super) fn metric_status(&self) -> &'static str {
        match self {
            Self::InvalidAppId => "invalid_app_id",
            Self::CertInvalid => "cert_invalid",
            Self::CertExpired => "cert_expired",
            Self::EnvelopeInvalid => "envelope_invalid",
            Self::AppNotRegistered => "app_not_registered",
            Self::NotOwner => "not_owner",
            Self::NotAuthorizedPublisher => "not_authorized_publisher",
            Self::MissingInstallPointer => "missing_install_pointer",
            Self::Internal(_) => "error",
        }
    }

    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidAppId => (400, json!({ "reason": "invalid_app_id" })),
            Self::CertInvalid => (401, json!({ "reason": "cert_invalid" })),
            Self::CertExpired => (401, json!({ "reason": "cert_expired" })),
            Self::EnvelopeInvalid => (401, json!({ "reason": "envelope_invalid" })),
            Self::NotOwner => (401, json!({ "reason": "not_owner" })),
            Self::AppNotRegistered => (404, json!({ "reason": "app_not_registered" })),
            Self::NotAuthorizedPublisher => (403, json!({ "reason": "not_authorized_publisher" })),
            Self::MissingInstallPointer => (
                400,
                json!({
                    "reason": "missing_install_pointer",
                    "detail": "promote to live requires a source checkout URL or artifact pointer so `lastdb app install` can fetch the app; re-publish or PUT with source/artifact first",
                }),
            ),
            Self::Internal(detail) => (500, json!({ "reason": "internal", "detail": detail })),
        }
    }
}
