use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ed25519::{key_id, SigningKey, VerifyingKey, SIGNATURE_LEN};
use crate::error::{CanonicalizeError, SignError, VerifyError};
use crate::hex::hex_lower;
use crate::jcs::canonicalize;

/// Currently-supported envelope version. Bumped if the envelope schema
/// changes in a non-backwards-compatible way; the verifier today
/// accepts only `v = 1` so that any older signer is failed loud rather
/// than silently misinterpreted.
pub const ENVELOPE_VERSION: u32 = 1;

/// What an envelope is signing — pinned at sign time so a signature for
/// one purpose (e.g. `app_register`) cannot be replayed as another
/// (e.g. `capability_grant`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    AppRegister,
    AppUpdate,
    AppPromote,
    SchemaClaim,
    /// Signs a `POST /v1/fields/declare` body. Declaring a field is claiming
    /// a slot others' data can fold into, so it carries the same DevCert gate
    /// as a shared-discovery schema offer — and a distinct purpose, so a
    /// schema-claim signature can never be replayed as a field declaration.
    FieldDeclare,
    /// Signs a schema resolver pack release manifest. The payload is the
    /// manifest JSON without its `signature` field; the manifest binds the
    /// resolver WASM, schema snapshot, registry embeddings, policy, ABI,
    /// embedder, environment, and artifact format version.
    SchemaResolverPack,
    /// Signs a `POST /v2/apps/{app_id}/releases` body. The payload is
    /// `{ "manifest": <release manifest> }`; the manifest binds the locked
    /// schema identities, the source commit, and the signed artifact digest.
    /// A distinct purpose so an `app_register` signature can never be
    /// replayed as a release publish.
    AppReleasePublish,
    /// Signs a `PUT /v2/apps/{app_id}/channels/{channel}` body. The payload
    /// carries the target `release_id` and the `generation` the writer read,
    /// so a replayed signature also replays a stale generation and loses the
    /// conflict check.
    AppChannelSet,
    /// Signs a `POST /v2/apps/{app_id}/revocations` body.
    AppReleaseRevoke,
    CapabilityGrant,
    DevCert,
}

/// Deployment binding — `dev` envelopes must not verify in `prod` and
/// vice versa. Callers check this against their runtime; the verifier
/// itself does not (see crate-level docs).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Env {
    Dev,
    Prod,
}

/// The only `alg` value v1 accepts. The field is a `String` on the
/// envelope (not an enum) so the verifier can reject an unknown
/// algorithm with a typed error rather than fail at deserialization —
/// matching the design's "alg matches" check at verify time.
pub const ALG_ED25519: &str = "Ed25519";

/// Typed envelope around a signed JCS payload.
///
/// Field order matches the design doc. Serde does not enforce field
/// order on output, but JCS sorts object keys lexicographically — so
/// what crosses the wire is canonical regardless of struct declaration
/// order. The `sig` field is `Option<String>` so the same struct can
/// represent an unsigned envelope (input to [`sign_envelope`]) and a
/// signed one (output, and input to [`verify_envelope`]). When `sig`
/// is `None` it is omitted from serialization — that omission is what
/// makes "JCS of envelope minus sig" mechanically equal to "JCS of the
/// unsigned envelope".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignatureEnvelope {
    pub version: u32,
    pub purpose: Purpose,
    pub alg: String,
    pub key_id: String,
    pub issued_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    pub env: Env,
    pub payload_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
}

/// Compute the `payload_hash` field for an envelope: lowercase hex
/// SHA-256 of `JCS(payload)`.
///
/// Callers use this both at sign time (to fill the envelope's
/// `payload_hash`) and at verify time (to confirm the envelope's
/// `payload_hash` matches the payload they hold).
pub fn compute_payload_hash(payload: &serde_json::Value) -> Result<String, SignError> {
    let bytes = canonicalize(payload).map_err(SignError::Canonicalize)?;
    let digest = Sha256::digest(&bytes);
    Ok(hex_lower(&digest))
}

/// Sign an envelope. The input envelope must have `sig = None`; on
/// success the returned envelope has `sig = Some(base64(signature))`.
///
/// The signature is over `JCS(envelope_without_sig)`. Because the
/// struct's `sig` field is `skip_serializing_if = "Option::is_none"`,
/// "envelope with sig = None" and "envelope minus sig" produce
/// byte-identical JCS output, which is what a verifier reconstructs.
pub fn sign_envelope(
    signing_key: &SigningKey,
    mut envelope: SignatureEnvelope,
) -> Result<SignatureEnvelope, SignError> {
    if envelope.sig.is_some() {
        return Err(SignError::SigAlreadySet);
    }
    let bytes = jcs_envelope_bytes(&envelope).map_err(SignError::Canonicalize)?;
    let sig = crate::ed25519::sign(signing_key, &bytes);
    envelope.sig = Some(BASE64.encode(sig));
    Ok(envelope)
}

/// Verify the intrinsic claims of a signed envelope:
///
/// 1. `version == ENVELOPE_VERSION`,
/// 2. `alg == Ed25519`,
/// 3. `key_id` matches `sha256(verifying_key)`,
/// 4. `issued_at` is not in the future (`now >= issued_at`) AND
///    `expires_at` (if present) is strictly in the future
///    (`now < expires_at`) — i.e. the envelope's claimed validity window
///    is `[issued_at, expires_at)`, half-open on the upper bound. The
///    upper bound is exclusive to match RFC 7519 §4.1.4: "the expiration
///    time on or after which the JWT MUST NOT be accepted for
///    processing." `iat`/`nbf` semantics stay inclusive on the lower
///    bound, matching RFC 7519 §4.1.5 "the time before which the JWT
///    MUST NOT be accepted."
/// 5. the signature in `sig` verifies over `JCS(envelope_without_sig)`.
///
/// Returns `Ok(())` on success. Note that `env` and `payload_hash` are
/// NOT checked here — see crate-level docs for why callers handle them.
pub fn verify_envelope(
    verifying_key: &VerifyingKey,
    envelope: &SignatureEnvelope,
) -> Result<(), VerifyError> {
    verify_envelope_at(verifying_key, envelope, Utc::now())
}

/// Same as [`verify_envelope`] but with an explicit `now` for the
/// time-bound checks (`issued_at` / `expires_at`). Lets tests pin the
/// boundary case `now == expires_at` deterministically — `Utc::now()` is
/// nanosecond-precision and never lands on a signed timestamp by accident,
/// so without an injected clock the half-open upper bound is untestable.
/// The signature math is identical.
pub(crate) fn verify_envelope_at(
    verifying_key: &VerifyingKey,
    envelope: &SignatureEnvelope,
    now: DateTime<Utc>,
) -> Result<(), VerifyError> {
    if envelope.version != ENVELOPE_VERSION {
        return Err(VerifyError::UnsupportedVersion(envelope.version));
    }
    if envelope.alg != ALG_ED25519 {
        return Err(VerifyError::AlgMismatch);
    }
    if key_id(verifying_key) != envelope.key_id {
        return Err(VerifyError::KeyIdMismatch);
    }
    // Not-before: an envelope that claims to be issued in the future
    // cannot be valid now. Symmetric with the not-after check — together
    // they bound the envelope's signed validity window. Without this, a
    // forward-dated envelope (e.g. minted by a signing-key holder before
    // their access was revoked, then stockpiled and replayed later)
    // verifies today, even though the envelope itself says it does not.
    // Mirrors the same fix landed for `verify_dev_cert` in #473.
    if now < envelope.issued_at {
        return Err(VerifyError::NotYetValid);
    }
    // Not-after: the upper bound is exclusive. RFC 7519 §4.1.4: "the
    // expiration time on or after which the JWT MUST NOT be accepted
    // for processing." Pre-fix this used `now > expires_at` (inclusive)
    // — at the exact moment `now == expires_at` the envelope still
    // verified, extending the signed validity window by one tick past
    // what the envelope itself claims. The half-open `[issued_at,
    // expires_at)` window matches the JWT/JOSE convention every other
    // sig-verifier in the ecosystem (and the exemem TS client SDK port)
    // expects, so cross-implementation behaviour at the boundary
    // agrees.
    if let Some(expires_at) = envelope.expires_at {
        if now >= expires_at {
            return Err(VerifyError::Expired);
        }
    }

    let sig_b64 = envelope.sig.as_ref().ok_or(VerifyError::MissingSig)?;
    let sig_bytes = BASE64
        .decode(sig_b64.as_bytes())
        .map_err(|_| VerifyError::MalformedSig)?;
    let sig_array: [u8; SIGNATURE_LEN] = sig_bytes
        .as_slice()
        .try_into()
        .map_err(|_| VerifyError::MalformedSig)?;

    let mut stripped = envelope.clone();
    stripped.sig = None;
    let bytes = jcs_envelope_bytes(&stripped).map_err(|_| VerifyError::Canonicalize)?;
    crate::ed25519::verify(verifying_key, &sig_array, &bytes).map_err(|_| VerifyError::BadSig)
}

fn jcs_envelope_bytes(envelope: &SignatureEnvelope) -> Result<Vec<u8>, CanonicalizeError> {
    json_canon::to_vec(envelope).map_err(CanonicalizeError::from)
}
