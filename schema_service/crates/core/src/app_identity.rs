//! App-identity verification for the canonical app registry.
//!
//! Lane B2b of [app_identity v3.1](../../../../exemem-workspace/docs/designs/app_identity.md).
//! This module owns the publish-side trust gate:
//!
//! - `POST /v1/apps` — a developer claims an `app_id` namespace. Gated by
//!   a `X-Exemem-Dev-Cert` (the exemem root vouching for the dev's
//!   Ed25519 key) plus a `X-Signature` `app_register` envelope (the dev
//!   signing the request body).
//! - `POST /v1/schemas` with `owner_app_id` — a schema is claimed under an
//!   already-registered app. Same cert, plus a `schema_claim` envelope,
//!   and the cert's `dev_pubkey` must match the app's registered owner.
//!
//! ## Two algorithms, by necessity
//!
//! The **DevCert** is `ES256` (ECDSA P-256), because the exemem root key
//! lives in AWS KMS and KMS has no Ed25519 SIGN_VERIFY (see
//! [`app_identity_crypto::dev_cert`] and
//! `schema_service/docs/app_identity_envelope_notes.md`). The
//! **X-Signature** envelope is `Ed25519`, signed by the developer's own
//! key. Verification therefore uses two crate entry points:
//! [`verify_dev_cert`] for the cert and [`verify_envelope`] for the
//! envelope.
//!
//! ## Developer accounts for new registrations
//!
//! New schemas, expansions and declared fields require an `owner_app_id`,
//! an Exemem DevCert with publication access, and a signature from that
//! registered app's owner. Local claims have no exemption. A public exact
//! repost creates no definitions and may return before the account gate.
//!
//! DEV exception (Tom, 2026-10-30): a DEV deployment with
//! `SCHEMA_OPEN_REGISTRATION=true` accepts new registrations without a
//! developer account. PROD ignores the flag and keeps the gate. Proof of
//! work, quotas and the vocabulary policy still apply in DEV.
//!
//! Cert verification requires the exemem root public key baked into
//! config (`APP_IDENTITY_ROOT_PUBKEYS`). When **no** trusted roots are
//! configured, new registrations fail loudly with
//! `app_identity_not_configured` so a misconfigured stage is visible at
//! first publish (the 2026-05-30 fbrain dogfood false-green) rather than
//! showing up days later in a snapshot diff. `POST /v1/apps` always
//! requires a trusted root (with none configured, every cert fails to
//! match and the request is a 401).

use std::collections::{HashMap, HashSet};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::snapshot::{AppArtifact, AppCodeSignature, AppMetadata, AppRecord, AppTier};
use crate::state::SchemaServiceState;
use app_identity_crypto::{
    canonicalize, compute_payload_hash, verify_dev_cert, verify_envelope,
    verifying_key_from_base64, DevCert, DevCertVerifyError, Env, Purpose, SignatureEnvelope,
    PURPOSE_DEV_CERT,
};

/// HTTP 401 body when an app write lacks `X-Exemem-Dev-Cert` or `X-Signature`.
///
/// Both HTTP routers write this body. Each router still reads its own headers.
#[must_use]
pub fn missing_dev_cert_body() -> Value {
    reason_detail_body(
        "cert_invalid",
        "missing X-Exemem-Dev-Cert or X-Signature header",
    )
}

/// HTTP body with one `reason` field and no other fields.
///
/// Each caller keeps its own status code. A body with a field other than
/// `reason` and `detail` stays at that caller.
#[must_use]
pub fn reason_only_body(reason: &str) -> Value {
    json!({ "reason": reason })
}

/// HTTP body with one `error` field and no other fields.
///
/// Each caller keeps its own status code and its own message. A body with a
/// field other than `error` stays at that caller.
#[must_use]
pub fn error_only_body(error: &str) -> Value {
    json!({ "error": error })
}

/// HTTP body with `reason` and `detail` and no other fields.
///
/// Each caller keeps its own status code and its own detail text.
#[must_use]
pub fn reason_detail_body(reason: &str, detail: &str) -> Value {
    json!({ "reason": reason, "detail": detail })
}

/// HTTP 400 body when an app id is refused.
///
/// The body is `{ "reason": "invalid_app_id" }`. The Lambda path checks and
/// the register, update, and promote errors share it.
#[must_use]
pub fn invalid_app_id_body() -> Value {
    reason_only_body("invalid_app_id")
}

/// HTTP 400 body when declare-field JSON has the wrong shape.
///
/// The Lambda route and the actix route both write this body. The detail is
/// `expected { owner_app_id, handle, fields[] }: <error>`.
#[must_use]
pub fn invalid_declare_field_body(error: &str) -> Value {
    reason_detail_body(
        "invalid_body",
        &format!("expected {{ owner_app_id, handle, fields[] }}: {error}"),
    )
}

// ─── Validation bounds (design § "Metadata bounds: strict") ───────────────

mod validation;
pub use validation::*;

// ─── Config ───────────────────────────────────────────────────────────────

/// App-identity verification config. The process shell builds this once
/// at startup and passes it in on [`crate::SchemaConfig`].
#[derive(Clone)]
pub struct AppIdentityConfig {
    /// Deployment environment the envelopes must be bound to.
    pub deployment_env: Env,
    /// Trusted exemem root public keys: `key_id` (hex `sha256(SPKI DER)`)
    /// → SubjectPublicKeyInfo DER. A set, not a single key, so a key
    /// rotation can be staged by adding the new key alongside the old
    /// (`SignatureEnvelope.key_id` selects which to verify against).
    pub trusted_roots: HashMap<String, Vec<u8>>,
    /// Developer Ed25519 pubkeys (base64) refused even with an unexpired
    /// cert. schema_service verifies certs **offline**, so it cannot see
    /// exemem's `developers` table in real time; this denylist is the
    /// offline mechanism the design's "redeploy schema_service" remedy
    /// relies on for key compromise. Refreshed on deploy.
    pub revoked_dev_pubkeys: HashSet<String>,
    /// Requested DEV open registration (`SCHEMA_OPEN_REGISTRATION`). Read it
    /// through [`Self::registration_is_open`], which refuses it in PROD.
    pub open_registration: bool,
}

impl Default for AppIdentityConfig {
    fn default() -> Self {
        Self {
            deployment_env: Env::Dev,
            trusted_roots: HashMap::new(),
            revoked_dev_pubkeys: HashSet::new(),
            open_registration: false,
        }
    }
}

impl AppIdentityConfig {
    /// Whether app-identity enforcement is active (a trusted root is
    /// configured). See module docs.
    pub fn is_active(&self) -> bool {
        !self.trusted_roots.is_empty()
    }

    /// Whether new schema and field registrations skip the developer-account
    /// gate. Only a DEV deployment opens registration. PROD keeps the gate
    /// even when the flag is set, so a copied DEV config cannot open PROD.
    pub fn registration_is_open(&self) -> bool {
        self.open_registration && self.deployment_env == Env::Dev
    }
}

/// Label for the deployment env. Stamped on metric lines and echoed in
/// the env-tag field of `POST/PUT /v1/apps` responses (one-element array
/// — response metadata only, not a mirroring claim; the cross-env mirror
/// was decommissioned in #517).
pub fn env_label(env: Env) -> &'static str {
    match env {
        Env::Dev => "dev",
        Env::Prod => "prod",
    }
}

// ─── Submodules ───────────────────────────────────────────────────────────

mod claim;
mod lifecycle_types;
mod list_reply;
mod metrics;
mod promote;
mod promote_reply;
mod register;
mod register_reply;
mod registry;
mod types;
mod update;
mod update_reply;
mod verify;

pub use lifecycle_types::*;
pub use list_reply::ListLiveAppsReply;
pub use promote_reply::PromoteAppReply;
pub use register_reply::RegisterAppReply;
pub use registry::PublicAppReply;
pub use types::*;
pub use update_reply::UpdateAppReply;
pub(crate) use verify::*;

use metrics::*;
