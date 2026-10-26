use super::*;

// ─── Registry client ──────────────────────────────────────────────────────

/// The anonymous read side of the `/v2` registry. No credential: public
/// reads stay anonymous.
#[derive(Debug, Clone)]
pub struct ReleaseRegistryClient {
    base_url: String,
}

/// A channel read: the desired release id and the generation that read saw.
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelRead {
    pub app_id: String,
    pub channel: String,
    pub release_id: String,
    /// Keep this. A later channel write that carries a stale generation
    /// fails with a conflict.
    pub generation: u64,
}

/// A release read: the manifest plus its registry annotations.
#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseRead {
    pub release_id: String,
    pub manifest: ReleaseManifest,
    pub publisher_dev_pubkey: String,
    #[serde(default)]
    pub revoked: bool,
}

/// One compat row from the verifier: a contract version proved compatible.
#[derive(Debug, Clone, Deserialize)]
pub struct CompatRowRead {
    pub release_id: String,
    pub contract_version: String,
    pub verifier_pubkey: String,
    pub verified_at: String,
    #[serde(default)]
    pub verification_url: Option<String>,
    pub payload_hash: String,
    pub signature: String,
}

/// App metadata from `GET /v2/apps`.
#[derive(Debug, Clone, Deserialize)]
pub struct AppListItem {
    pub app_id: String,
    pub owner_dev_pubkey: String,
    #[serde(default)]
    pub revoked: bool,
}

/// Release item from `GET /v2/apps/{app_id}/releases`.
#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseListItem {
    pub release_id: String,
    pub app_id: String,
    pub app_uuid: String,
    pub source_commit: String,
    pub artifact_digest: String,
    pub published_at: String,
    #[serde(default)]
    pub revoked: bool,
}

impl ReleaseRegistryClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn client() -> Result<reqwest::Client, String> {
        // trace-egress: propagate (public app registry reads + signed
        // artifact download; every caller of this builder is a first-party
        // registry or R2 fetch, classified at each call site below)
        reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))
    }

    /// `GET /v2/apps/{app_id}/channels/{channel}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_channel(&self, app_id: &str, channel: &str) -> Result<ChannelRead, String> {
        let url = format!("{}/v2/apps/{app_id}/channels/{channel}", self.base_url);
        // trace-egress: propagate (public app registry channel read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/releases/{release_id}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_release(&self, release_id: &str) -> Result<ReleaseRead, String> {
        let url = format!("{}/v2/releases/{release_id}", self.base_url);
        // trace-egress: propagate (public app registry release read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/apps/{app_id}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_app(&self, app_id: &str) -> Result<Value, String> {
        let url = format!("{}/v2/apps/{app_id}", self.base_url);
        // trace-egress: propagate (public app registry app read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// Download the artifact bytes. A `file://` URL reads from disk so a
    /// hermetic proof run needs no object store.
    ///
    /// # Errors
    /// Returns the transport, status, or filesystem failure.
    pub async fn fetch_artifact(&self, url: &str) -> Result<Vec<u8>, String> {
        if let Some(path) = url.strip_prefix("file://") {
            return std::fs::read(path).map_err(|e| format!("failed to read {path}: {e}"));
        }
        // trace-egress: propagate (signed release artifact download)
        let response = Self::client()?
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(format!("GET {url} returned {status}"));
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("GET {url}: body read failed: {e}"))
    }

    /// `GET /v2/apps` — list all apps (anonymous).
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn list_apps(&self) -> Result<Vec<AppListItem>, String> {
        let url = format!("{}/v2/apps", self.base_url);
        // trace-egress: propagate (public app registry list)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/apps/{app_id}/releases` — list releases for an app (anonymous).
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn list_releases(&self, app_id: &str) -> Result<Vec<ReleaseListItem>, String> {
        let url = format!("{}/v2/apps/{app_id}/releases", self.base_url);
        // trace-egress: propagate (public app registry releases list)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/releases/{release_id}/compat` — list compat rows for a release (anonymous).
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn list_compat_rows(&self, release_id: &str) -> Result<Vec<CompatRowRead>, String> {
        let url = format!("{}/v2/releases/{release_id}/compat", self.base_url);
        // trace-egress: propagate (public app registry compat rows read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }
}

// ─── Compat row signature verification ──────────────────────────────────

/// Verify a compat row's signature against a configured verifier public key.
///
/// The signature must be Ed25519 over the canonical JSON body that produced
/// the payload_hash. The verifier_pubkey in the row must match the configured
/// trusted verifier key.
///
/// # Errors
/// Returns a detailed message if the signature is invalid, the key doesn't match,
/// or the verification fails.
pub fn verify_compat_row(row: &CompatRowRead, trusted_verifier_pubkey: &str) -> Result<(), String> {
    if row.verifier_pubkey != trusted_verifier_pubkey {
        return Err(format!(
            "compat row verifier key '{}' does not match trusted key '{}'",
            row.verifier_pubkey, trusted_verifier_pubkey
        ));
    }

    let verifying_key = verifying_key_from_base64(trusted_verifier_pubkey)
        .map_err(|e| format!("failed to parse verifier public key: {e:?}"))?;

    let sig_bytes = BASE64
        .decode(row.signature.as_bytes())
        .map_err(|e| format!("failed to decode signature base64: {e}"))?;

    let signature: [u8; app_identity_crypto::SIGNATURE_LEN] = sig_bytes
        .try_into()
        .map_err(|_| "signature is not 64 bytes".to_string())?;

    let mut payload = serde_json::json!({
        "release_id": row.release_id,
        "contract_version": row.contract_version,
    });

    if let Some(ref url) = row.verification_url {
        payload["verification_url"] = serde_json::json!(url);
    }

    let canonical_body = app_identity_crypto::canonicalize(&payload)
        .map_err(|e| format!("failed to canonicalize payload: {e}"))?;

    let computed_hash = fold_db::hex::sha256_hex(&canonical_body);
    if computed_hash != row.payload_hash {
        return Err(format!(
            "payload_hash mismatch: row claims '{}', computed '{}'",
            row.payload_hash, computed_hash
        ));
    }

    verify(&verifying_key, &signature, &canonical_body)
        .map_err(|e| format!("signature verification failed: {e}"))
}
