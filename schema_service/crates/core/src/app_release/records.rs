//! Release and channel records.

use super::*;

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
    /// The LastDB contract versions the publisher claims, as comma-separated
    /// comparators (`>=0.1.0-0, <0.2.0`). The verifier checks the release on
    /// each supported contract version in the range. Manifests from before
    /// the release log have none; `None` is not serialized, so their JCS
    /// bytes and release ids do not change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_range: Option<String>,
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

/// [`compute_release_id`] over an already-parsed manifest body — used on the
/// request path so the digest covers exactly the JSON the developer signed.
///
/// # Errors
/// Returns the canonicalization failure.
pub fn compute_release_id_from_value(manifest: &Value) -> Result<String, String> {
    let bytes = canonicalize(manifest).map_err(|e| format!("manifest canonicalize failed: {e}"))?;
    Ok(schema_types::hex::sha256_hex(&bytes))
}

/// Where the verifier result for a release lives: `{release_log_url}/results/{release_id}.json`.
///
/// `release_log_url` is the deployment's configured log base URL. `None`
/// means no verifier is configured. A malformed id is refused before any
/// URL is built, so the redirect never carries caller text.
///
/// # Errors
/// Returns the HTTP status and body: 400 for an id that is not 64 lowercase
/// hex, 503 when no log URL is configured.
pub fn release_verification_location(
    release_log_url: Option<&str>,
    release_id: &str,
) -> Result<String, (u16, Value)> {
    if !is_sha256_hex(release_id) {
        return Err((400, json!({ "reason": "invalid_release_id" })));
    }
    let base = release_log_url
        .map(|url| url.trim().trim_end_matches('/'))
        .filter(|url| !url.is_empty())
        .ok_or_else(|| (503, json!({ "reason": "verifier_not_configured" })))?;
    Ok(format!("{base}/results/{release_id}.json"))
}

/// `SHA-256` hex of raw bytes — the artifact-digest check an installer runs
/// against the bytes it downloaded.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

// ─── Compatibility rows ──────────────────────────────────────────────────

/// A compatibility verification record: attests that a specific release has
/// been verified to work with a specific LastDB contract version.
///
/// Compat rows are **never** written by the app publisher — only by a
/// separate verifier identity (a GitHub Actions workflow in its own repo
/// with its own signing key). This prevents developers from self-attesting
/// compatibility without actual verification.
///
/// Storage: per-record at `compat/{release_id}/{contract_version}.json`
/// with conditional writes (If-None-Match: *) so each row is immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatRow {
    /// The release this row verifies.
    pub release_id: String,
    /// LastDB contract/version this release is verified to work with.
    /// Format: semver-like "1.2.3" or "1.2.x" for a range.
    pub contract_version: String,
    /// Ed25519 public key (base64) of the verifier that signed this row.
    /// Must match the configured `verifier_pubkey` in AppIdentityConfig.
    pub verifier_pubkey: String,
    /// RFC 3339 timestamp when verification completed.
    pub verified_at: String,
    /// Optional: URL to the verification run (e.g. GitHub Actions run URL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_url: Option<String>,
    /// SHA-256 hex digest of the canonical JSON payload that was signed.
    /// Installers can recompute this over the compat row body to verify
    /// the signature offline without trusting the registry.
    pub payload_hash: String,
    /// Base64-encoded Ed25519 signature over the canonical payload.
    /// Installers re-verify: `ed25519::verify(verifier_pubkey, JCS(body), signature)`.
    pub signature: String,
}

impl CompatRow {
    /// Storage key for this compat row: `compat/{release_id}/{contract_version}.json`
    #[must_use]
    pub fn storage_key(&self) -> String {
        format!("compat/{}/{}.json", self.release_id, self.contract_version)
    }

    /// The anonymous `GET /v2/releases/{release_id}/compat` body (one row).
    #[must_use]
    pub fn to_public_json(&self) -> Value {
        json!({
            "release_id": self.release_id,
            "contract_version": self.contract_version,
            "verifier_pubkey": self.verifier_pubkey,
            "verified_at": self.verified_at,
            "verification_url": self.verification_url,
            "payload_hash": self.payload_hash,
            "signature": self.signature,
        })
    }
}
