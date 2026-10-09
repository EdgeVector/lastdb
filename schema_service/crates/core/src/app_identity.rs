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
//! ## Namespacing, not gatekeeping
//!
//! Submissions WITHOUT `owner_app_id` are un-namespaced user proposals:
//! the registry accepts them unconditionally and canonicalizes
//! (identity-hash dedup, descriptive-name correction, similarity dedup).
//! Submissions WITH `owner_app_id` reserve a namespace and require a
//! valid dev-cert + signature whose `dev_pubkey` matches the app's
//! registered owner — apps cannot forge ownership of namespaces they
//! don't control.
//!
//! Cert verification requires the exemem root public key baked into
//! config (`APP_IDENTITY_ROOT_PUBKEYS`). When **no** trusted roots are
//! configured, namespaced publishes fail loudly with
//! `app_identity_not_configured` so a misconfigured stage is visible at
//! first publish (the 2026-05-30 fbrain dogfood false-green) rather than
//! showing up days later in a snapshot diff. Un-namespaced publishes are
//! unaffected by the active/inactive state. `POST /v1/apps` always
//! requires a trusted root (with none configured, every cert fails to
//! match and the request is a 401).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::lock_helpers::read_lock;
use crate::snapshot::{AppArtifact, AppCodeSignature, AppMetadata, AppRecord, AppTier};
use crate::state::SchemaServiceState;
use app_identity_crypto::{
    canonicalize, compute_payload_hash, root_key_id, verify_dev_cert, verify_envelope,
    verifying_key_from_base64, DevCert, DevCertVerifyError, Env, Purpose, SignatureEnvelope,
    PURPOSE_DEV_CERT,
};

// ─── Validation bounds (design § "Metadata bounds: strict") ───────────────

mod validation;
pub use validation::*;

// ─── Config ───────────────────────────────────────────────────────────────

/// App-identity verification config. Built once at startup (from env on
/// the binaries, injected directly in tests) and read per request.
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
}

impl Default for AppIdentityConfig {
    fn default() -> Self {
        Self {
            deployment_env: Env::Dev,
            trusted_roots: HashMap::new(),
            revoked_dev_pubkeys: HashSet::new(),
        }
    }
}

impl AppIdentityConfig {
    /// Build config from the process environment:
    /// - `ENVIRONMENT` — `prod`/`production` → [`Env::Prod`], else [`Env::Dev`].
    /// - `APP_IDENTITY_ROOT_PUBKEYS` — comma-separated base64 P-256
    ///   SubjectPublicKeyInfo DER blobs (exemem-infra wires this from the
    ///   KMS GetPublicKey output of the `exemem-app-identity-root` key).
    /// - `APP_IDENTITY_REVOKED_DEV_PUBKEYS` — comma-separated base64
    ///   Ed25519 dev pubkeys to refuse.
    pub fn from_env() -> Self {
        let deployment_env = deployment_env_from_process();
        let trusted_roots = std::env::var("APP_IDENTITY_ROOT_PUBKEYS")
            .ok()
            .map(|raw| parse_trusted_roots(&raw))
            .unwrap_or_default();
        let revoked_dev_pubkeys = std::env::var("APP_IDENTITY_REVOKED_DEV_PUBKEYS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            deployment_env,
            trusted_roots,
            revoked_dev_pubkeys,
        }
    }

    /// Whether app-identity enforcement is active (a trusted root is
    /// configured). See module docs.
    pub fn is_active(&self) -> bool {
        !self.trusted_roots.is_empty()
    }
}

fn parse_trusted_roots(raw: &str) -> HashMap<String, Vec<u8>> {
    let mut roots = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        match BASE64.decode(entry.as_bytes()) {
            Ok(der) => {
                roots.insert(root_key_id(&der), der);
            }
            Err(_) => {
                tracing::warn!(
                    target: "schema_service::app_identity",
                    "Skipping malformed APP_IDENTITY_ROOT_PUBKEYS entry (not base64)"
                );
            }
        }
    }
    roots
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

/// Inverse of [`env_label`]: parse a wire label back into [`Env`].
/// Unknown labels return `None`.
pub fn env_from_label(label: &str) -> Option<Env> {
    match label {
        "dev" => Some(Env::Dev),
        "prod" => Some(Env::Prod),
        _ => None,
    }
}

/// Resolve the deployment env from the `ENVIRONMENT` process variable.
/// Single source of truth for what env this deployment is — every
/// app-identity verification path reads through this so an
/// `ENVIRONMENT=prod` Lambda will refuse a dev-signed envelope (and
/// vice versa), even though dev and prod are otherwise independent
/// registries with independent trusted roots.
pub fn deployment_env_from_process() -> Env {
    match std::env::var("ENVIRONMENT").as_deref() {
        Ok("prod" | "production") => Env::Prod,
        _ => Env::Dev,
    }
}
// ─── Submodules ───────────────────────────────────────────────────────────

mod claim;
mod lifecycle_types;
mod metrics;
mod promote;
mod register;
mod registry;
mod types;
mod update;
mod verify;

pub use lifecycle_types::*;
pub use types::*;
pub(crate) use verify::*;

use metrics::*;
