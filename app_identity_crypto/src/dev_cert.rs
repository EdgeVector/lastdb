//! DevCert verification — the one ES256 (ECDSA P-256) surface in this crate.
//!
//! Every *application-side* envelope in this crate is Ed25519. The
//! exemem **root** key, however, lives in AWS KMS, which does not offer
//! Ed25519 SIGN_VERIFY — so the `POST /v1/dev-cert` endpoint
//! (exemem auth_service, Lane B1) signs DevCerts with `ES256`
//! (`ECDSA_SHA_256`) instead. A DevCert is therefore NOT a
//! [`SignatureEnvelope`](crate::SignatureEnvelope): it is a flat struct
//! whose field set matches the auth_service wire format verbatim, and it
//! is verified here against the exemem root's SubjectPublicKeyInfo DER.
//!
//! The trust chain a verifier walks:
//!   1. [`verify_dev_cert`] — the DevCert is signed by a trusted exemem
//!      root (offline; the root pubkey is baked into config).
//!   2. The cert carries `dev_pubkey` (the developer's Ed25519 public
//!      key). The caller then verifies the request's `X-Signature`
//!      [`SignatureEnvelope`](crate::SignatureEnvelope) against that
//!      `dev_pubkey` with [`verify_envelope`](crate::verify_envelope).
//!
//! Signing input parity: auth_service computes the signature over
//! `SHA-256(JCS(cert_without_sig))` using `serde_jcs`. We recompute the
//! identical bytes with this crate's `json_canon`-backed
//! [`canonicalize`](crate::canonicalize) (a test asserts the two JCS
//! implementations agree byte-for-byte on a representative cert), and
//! verify with the standard SHA-256-prehashing ECDSA `Verifier`.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::DevCertVerifyError;
use crate::hex::hex_lower;
use crate::jcs::canonicalize;

/// The only `alg` a DevCert may carry. `ES256` = ECDSA over P-256 with
/// SHA-256, the algorithm AWS KMS uses for `ECDSA_SHA_256`.
pub const ALG_ES256: &str = "ES256";

/// Currently-supported DevCert version. The verifier today accepts only
/// `version = 1`, mirroring [`ENVELOPE_VERSION`](crate::ENVELOPE_VERSION):
/// a future v=2 minter that happens to ship the same field set must be
/// failed loud, not silently reinterpreted as v=1.
pub const DEV_CERT_VERSION: u32 = 1;

/// The only `purpose` a DevCert may carry. Checked by callers (verify is
/// intrinsic-only, mirroring [`verify_envelope`](crate::verify_envelope)).
pub const PURPOSE_DEV_CERT: &str = "dev_cert";

/// A developer certificate minted by the exemem root key.
///
/// Field set and names match `exemem_service/.../auth_service/src/dev_cert.rs`
/// exactly — the wire contract is shared. JCS sorts keys at
/// canonicalization time, so declaration order is irrelevant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DevCert {
    pub version: u32,
    pub purpose: String,
    pub alg: String,
    /// `sha256(SubjectPublicKeyInfo DER of the exemem root pubkey)`, hex.
    pub key_id: String,
    /// The developer's Ed25519 public key (base64). The thing this cert
    /// vouches for.
    pub dev_pubkey: String,
    pub user_hash: String,
    pub issued_at: String,
    pub expires_at: String,
    pub env: String,
    /// Whether the developer this cert vouches for is an **authorized
    /// publisher** — i.e. passed the publish gate (a paid plan OR an
    /// explicit `developer_access` grant) at mint time. Stamped by
    /// auth_service in **every** env (dev mint stays free/instant, but the
    /// flag still reflects whether the dev would clear the publish gate).
    /// schema_service requires this to be `true` to reserve an app
    /// namespace, identically in dev and prod — so a registration that
    /// succeeds in dev is guaranteed to be allowed in prod (dev/prod
    /// parity), while an unauthorized free account cannot reserve a name in
    /// either env. Part of the signed cert: tampering invalidates the sig.
    pub authorized_publisher: bool,
    /// Base64 of the DER-encoded ECDSA signature returned by KMS.
    pub sig: String,
}

/// Derive the DevCert `key_id` from an exemem root public key: lowercase
/// hex `sha256(SubjectPublicKeyInfo DER)`. Callers index their trusted
/// root set by this value so a cert's `key_id` selects which root to
/// verify against (versioning lookahead).
pub fn root_key_id(root_spki_der: &[u8]) -> String {
    hex_lower(&Sha256::digest(root_spki_der))
}

/// Verify the intrinsic claims of a DevCert against a candidate exemem
/// root public key (SubjectPublicKeyInfo DER):
///
/// 1. `version == DEV_CERT_VERSION`,
/// 2. `purpose == PURPOSE_DEV_CERT` — the same byte-identical-bytes
///    forward-compat guard as `version`. ALL DevCerts have `purpose =
///    "dev_cert"` (no per-caller variation, unlike
///    [`verify_envelope`](crate::verify_envelope)'s `purpose: Purpose`
///    which legitimately ranges across the five enum variants), so it is
///    a type-of-object discriminator and belongs in the intrinsic set,
/// 3. `alg == ES256`,
/// 4. `key_id` matches `sha256(root_spki_der)`,
/// 5. `issued_at` is not in the future (`now >= issued_at`) AND
///    `expires_at` is strictly in the future (`now < expires_at`) —
///    i.e. the cert's claimed validity window is `[issued_at,
///    expires_at)`, half-open on the upper bound. The upper bound is
///    exclusive to match RFC 7519 §4.1.4 ("on or after which the JWT
///    MUST NOT be accepted"); `iat`/`nbf` semantics stay inclusive on
///    the lower bound per §4.1.5.
/// 6. `sig` verifies over `SHA-256(JCS(cert_without_sig))`.
///
/// `env` is NOT checked here — callers verify it against their own
/// deployment context (the cert may target either `dev` or `prod`).
pub fn verify_dev_cert(root_spki_der: &[u8], cert: &DevCert) -> Result<(), DevCertVerifyError> {
    verify_dev_cert_at(root_spki_der, cert, Utc::now())
}

/// Same as [`verify_dev_cert`] but with an explicit `now` for the
/// time-bound checks (`issued_at` / `expires_at`). Lets tests pin the
/// boundary case `now == expires_at` deterministically — `Utc::now()` is
/// nanosecond-precision and never lands on a signed timestamp by accident,
/// so without an injected clock the half-open upper bound is untestable.
/// The signature math is identical.
pub(crate) fn verify_dev_cert_at(
    root_spki_der: &[u8],
    cert: &DevCert,
    now: DateTime<Utc>,
) -> Result<(), DevCertVerifyError> {
    if cert.version != DEV_CERT_VERSION {
        return Err(DevCertVerifyError::UnsupportedVersion(cert.version));
    }
    if cert.purpose != PURPOSE_DEV_CERT {
        return Err(DevCertVerifyError::PurposeMismatch);
    }
    if cert.alg != ALG_ES256 {
        return Err(DevCertVerifyError::AlgMismatch);
    }
    if root_key_id(root_spki_der) != cert.key_id {
        return Err(DevCertVerifyError::KeyIdMismatch);
    }
    let issued_at = cert
        .issued_at
        .parse::<DateTime<Utc>>()
        .map_err(|_| DevCertVerifyError::BadTimestamp)?;
    let expires_at = cert
        .expires_at
        .parse::<DateTime<Utc>>()
        .map_err(|_| DevCertVerifyError::BadTimestamp)?;
    // Not-before: a cert that claims to be issued in the future cannot
    // be valid now. Symmetric with the not-after check — together they
    // bound the cert's signed validity window. Without this, a
    // forward-dated cert (e.g. minted by a rogue insider with KMS access
    // before key rotation) verifies today, even though the cert itself
    // says it does not.
    if now < issued_at {
        return Err(DevCertVerifyError::NotYetValid);
    }
    // Not-after: the upper bound is exclusive. RFC 7519 §4.1.4: "the
    // expiration time on or after which the JWT MUST NOT be accepted
    // for processing." Pre-fix this used `now > expires_at` (inclusive)
    // — at the exact moment `now == expires_at` the cert still
    // verified, extending the signed validity window by one tick past
    // what the cert itself claims. The half-open `[issued_at,
    // expires_at)` window matches the JWT/JOSE convention every other
    // sig-verifier in the ecosystem expects, so cross-implementation
    // behaviour at the boundary agrees.
    if now >= expires_at {
        return Err(DevCertVerifyError::Expired);
    }

    let verifying_key = VerifyingKey::from_public_key_der(root_spki_der)
        .map_err(|_| DevCertVerifyError::MalformedRootKey)?;
    let sig_der = BASE64
        .decode(cert.sig.as_bytes())
        .map_err(|_| DevCertVerifyError::MalformedSig)?;
    let signature = Signature::from_der(&sig_der).map_err(|_| DevCertVerifyError::MalformedSig)?;

    let signed_bytes = canonical_signing_bytes(cert)?;
    verifying_key
        .verify(&signed_bytes, &signature)
        .map_err(|_| DevCertVerifyError::BadSig)
}

/// `JCS(cert_without_sig)` — the exact bytes the signer (auth_service)
/// hashed before signing. Serialize the cert to a JSON object, drop the
/// `sig` key entirely (not just blank it), canonicalize.
fn canonical_signing_bytes(cert: &DevCert) -> Result<Vec<u8>, DevCertVerifyError> {
    let mut value = serde_json::to_value(cert).map_err(|_| DevCertVerifyError::Canonicalize)?;
    if let Value::Object(map) = &mut value {
        map.remove("sig");
    }
    canonicalize(&value).map_err(|_| DevCertVerifyError::Canonicalize)
}
