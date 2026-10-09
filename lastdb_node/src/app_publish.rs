//! `lastdb app …` — developer-facing app registration + publish flow.
//!
//! Implements the canonical three-step registration process against the
//! LastDB app registry (design: brain `design-lastdb-app-registry`):
//!
//! 1. **check** — by default a read-only plan: post each manifest schema to
//!    the local Mini's app-facing route (`POST /api/apps/declare-schema`) with
//!    intent `check`, which reports what a sync would do (reuse / compose /
//!    register / expand, or the error a sync would hit) and writes nothing.
//!    `--sync` uses intent `catalog_sync` instead and reports the catalog
//!    identity only when the audited result is bind-eligible.
//! 2. **register-schemas** — compatibility alias for the same Mini-owned sync;
//!    it never calls Schema Service directly.
//! 3. **publish** — reserve the app namespace as a sandbox row, then
//!    **promote** — HARD GATE: refuse unless every manifest schema resolves to
//!    catalog identities, then mint a short-TTL DevCert and promote the row
//!    (`POST /v1/apps/{app_id}/promote`, purpose `app_promote`).
//!
//! The signing identity is a developer-local Ed25519 keypair (`dev-init`).
//! Read surfaces: `list` (`GET /v1/apps`) and `info` (`GET /v1/apps/{app_id}`).

use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use app_identity_crypto::{
    compute_payload_hash, key_id, sign as ed25519_sign, sign_envelope, DevCert, Env,
    Purpose as AppIdentityPurpose, SignatureEnvelope, SigningKey, ALG_ED25519, ENVELOPE_VERSION,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use fold_db::schema::types::Schema;
use schema_service_client::{
    DevSchemaClaim, SchemaServiceClient, SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT,
};
use schema_service_core::app_identity::{app_version_is_greater, validate_app_version};
use schema_service_core::app_release::{sha256_hex, ReleaseManifest};
use schema_service_core::snapshot::{AppArtifact, AppMetadata, AppRecord, AppTier};
use schema_service_core::{
    SharedSurfaceCompatibility, SharedSurfaceMetadata, SharedSurfaceProvenance,
    SharedSurfacePurpose, SharedSurfaceVisibility,
};
use schema_types::Schema as ServiceSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod devkey;
mod http;
mod install;
mod manifest;
mod publish;
mod release;
mod schema_check;

pub use devkey::*;
use http::*;
pub use install::*;
pub use manifest::*;
pub use publish::*;
pub use release::*;
pub use schema_check::*;
