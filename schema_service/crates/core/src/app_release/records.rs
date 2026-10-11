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

/// `release_id = SHA-256(JCS(manifest))`.
///
/// The value is content-addressed, so two publishers that submit the same
/// manifest submit the same release, and no publisher can move a release id
/// onto different bytes.
///
/// # Errors
/// Returns the canonicalization failure if the manifest does not serialize
/// to canonical JSON (NaN, non-string keys — unreachable for this type).
pub fn compute_release_id(manifest: &ReleaseManifest) -> Result<String, String> {
    let value = serde_json::to_value(manifest)
        .map_err(|e| format!("release manifest does not serialize: {e}"))?;
    compute_release_id_from_value(&value)
}

/// [`compute_release_id`] over an already-parsed manifest body — used on the
/// request path so the digest covers exactly the JSON the developer signed.
///
/// # Errors
/// Returns the canonicalization failure.
pub fn compute_release_id_from_value(manifest: &Value) -> Result<String, String> {
    let bytes = canonicalize(manifest).map_err(|e| format!("manifest canonicalize failed: {e}"))?;
    Ok(schema_types::hex::sha256_hex(&bytes))
}

/// `SHA-256` hex of raw bytes — the artifact-digest check an installer runs
/// against the bytes it downloaded.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}
