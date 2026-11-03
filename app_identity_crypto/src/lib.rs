//! Shared Ed25519 + JCS (RFC 8785) + signed-envelope helpers.
//!
//! Lane A of the `app_identity` v3.1 design (see
//! `exemem-workspace/docs/designs/app_identity.md`). This crate is the DRY
//! foundation for every Ed25519/JCS signature in the workspace —
//! schema_service, fold_db_node (including dev-mode), exemem-infra, and the
//! client SDK port all delegate canonicalization and envelope handling
//! here, so cross-environment signature interop is testable in one place.
//!
//! Scope of `verify_envelope` is intentionally intrinsic: it checks alg,
//! key_id, expiry, and the signature itself. Deployment-context checks
//! (`env` matches the runtime, `payload_hash` matches a recomputed
//! payload digest) belong to callers — they need information the
//! envelope alone doesn't carry. Helpers for both are exposed:
//! [`compute_payload_hash`] for the payload side and
//! [`SignatureEnvelope::env`] for the deployment side.
#![warn(missing_docs)]

mod dev_cert;
mod ed25519;
mod envelope;
mod error;
mod hex;
mod jcs;
mod registry_list;

pub use dev_cert::{
    root_key_id, verify_dev_cert, DevCert, ALG_ES256, DEV_CERT_VERSION, PURPOSE_DEV_CERT,
};
pub use ed25519::{
    key_id, sign, verify, verifying_key_from_base64, SigningKey, VerifyingKey, PUBLIC_KEY_LEN,
    SIGNATURE_LEN,
};
pub use envelope::{
    compute_payload_hash, sign_envelope, verify_envelope, Env, Purpose, SignatureEnvelope,
    ALG_ED25519, ENVELOPE_VERSION,
};
pub use error::{CanonicalizeError, DevCertVerifyError, KeyParseError, SignError, VerifyError};
pub use hex::hex_lower;
pub use jcs::canonicalize;
pub use registry_list::{
    github_repo, interface_list_path, sign_index_bytes, valid_app_id, valid_source,
    validate_channel, verify_index_bytes, IndexSignature, InterfaceList, ListEntry, LIST_CHANNELS,
    MAX_LIST_APPS, MAX_LIST_BYTES, REGISTRY_LIST_PUBKEY_B64,
};
