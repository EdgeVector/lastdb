//! Release manifests, channels, and dev cert signing.

use super::*;

/// Build the release manifest for `manifest` from the app lockfile.
///
/// The lockfile already holds the final identities the Schema Service
/// returned during development. This copies them without change and adds no
/// lockfile field — the release binds identities that already exist.
///
/// # Errors
/// Returns an error when a manifest schema has no locked identity, which is
/// exactly the "release references an unresolved schema" case.
pub fn build_release_manifest(
    manifest: &AppManifest,
    manifest_path: &Path,
    app_uuid: &str,
    source_commit: &str,
    artifact_bytes: &[u8],
    artifact_url: &str,
    dev_key: &SigningKey,
) -> Result<ReleaseManifest, String> {
    let locked = load_lockfile(manifest_path);
    let mut schemas = std::collections::BTreeMap::new();
    for raw in &manifest.schemas {
        let name = raw
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "manifest schema has no name".to_string())?;
        let identity = locked.get(name).ok_or_else(|| {
            format!(
                "schema '{name}' has no locked identity in {} — resolve it before cutting a release",
                lockfile_path(manifest_path).display()
            )
        })?;
        schemas.insert(name.to_string(), identity.clone());
    }
    if schemas.is_empty() {
        return Err("the app manifest declares no schemas to lock".to_string());
    }
    // The digest covers the artifact bytes; the signature covers the digest,
    // so a signature can never be replayed onto different bytes.
    let artifact_digest = sha256_hex(artifact_bytes);
    let artifact_signature = BASE64.encode(ed25519_sign(dev_key, artifact_digest.as_bytes()));
    Ok(ReleaseManifest {
        app_id: manifest.app_id.clone(),
        app_uuid: app_uuid.to_string(),
        schemas,
        source_commit: source_commit.to_string(),
        artifact_digest,
        artifact_url: artifact_url.to_string(),
        artifact_signature,
    })
}

/// `POST /v2/apps/{app_id}/releases` — publish an immutable release.
///
/// # Errors
/// Returns the transport failure or the registry's rejection.
pub async fn publish_release(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    release: &ReleaseManifest,
) -> Result<Value, String> {
    let body = json!({
        "manifest": serde_json::to_value(release)
            .map_err(|e| format!("failed to encode release manifest: {e}"))?,
    });
    let url = format!(
        "{}/v2/apps/{}/releases",
        schema_service_url.trim_end_matches('/'),
        release.app_id
    );
    v2_write(
        exemem_api_url,
        api_key,
        dev_key,
        env,
        AppIdentityPurpose::AppReleasePublish,
        "POST",
        &url,
        &body,
        &[200, 201],
    )
    .await
}

/// `PUT /v2/apps/{app_id}/channels/{channel}` — point a channel at a
/// release under a generation check.
///
/// `generation` is what the caller's channel read returned. A stale value
/// fails with a 409 rather than overwriting a concurrent write.
///
/// # Errors
/// Returns the transport failure or the registry's rejection (409 on a
/// stale generation).
#[allow(
    clippy::too_many_arguments,
    reason = "one call site per /v2 write route; the arguments are the route's \
     own parameters, and bundling them adds a type read only by this function"
)]
pub async fn set_release_channel(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    app_id: &str,
    channel: &str,
    release_id: &str,
    generation: u64,
) -> Result<Value, String> {
    let body = json!({
        "app_id": app_id,
        "channel": channel,
        "release_id": release_id,
        "generation": generation,
    });
    let url = format!(
        "{}/v2/apps/{app_id}/channels/{channel}",
        schema_service_url.trim_end_matches('/')
    );
    v2_write(
        exemem_api_url,
        api_key,
        dev_key,
        env,
        AppIdentityPurpose::AppChannelSet,
        "PUT",
        &url,
        &body,
        &[200],
    )
    .await
}

/// `POST /v2/apps/{app_id}/revocations` — revoke a published release.
///
/// # Errors
/// Returns the transport failure or the registry's rejection.
#[allow(
    clippy::too_many_arguments,
    reason = "one call site per /v2 write route; the arguments are the route's \
     own parameters, and bundling them adds a type read only by this function"
)]
pub async fn revoke_release(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    app_id: &str,
    release_id: &str,
    reason: Option<&str>,
) -> Result<Value, String> {
    let mut body = json!({ "app_id": app_id, "release_id": release_id });
    if let Some(reason) = reason {
        body["reason"] = Value::String(reason.to_string());
    }
    let url = format!(
        "{}/v2/apps/{app_id}/revocations",
        schema_service_url.trim_end_matches('/')
    );
    v2_write(
        exemem_api_url,
        api_key,
        dev_key,
        env,
        AppIdentityPurpose::AppReleaseRevoke,
        "POST",
        &url,
        &body,
        &[200],
    )
    .await
}

/// One DevCert-gated `/v2` write: mint the cert, sign the body under
/// `purpose`, send it, and accept only `expected_status`.
#[allow(
    clippy::too_many_arguments,
    reason = "one call site per /v2 write route; \
     bundling these into a struct would add a type read only by this function"
)]
pub(super) async fn v2_write(
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    purpose: AppIdentityPurpose,
    method: &str,
    url: &str,
    body: &Value,
    expected_status: &[u16],
) -> Result<Value, String> {
    let cert = mint_dev_cert(exemem_api_url, api_key, dev_key).await?;
    let cert_b64 = BASE64
        .encode(serde_json::to_vec(&cert).map_err(|e| format!("failed to encode dev cert: {e}"))?);
    let sig_b64 = sign_envelope_b64(dev_key, purpose, env, body)?;
    let client = http_client()?;
    let request = match method {
        "PUT" => client.put(url),
        _ => client.post(url),
    };
    // trace-egress: propagate (schema_service /v2 app release registry write)
    let response = request
        .header("X-Exemem-Dev-Cert", cert_b64)
        .header("X-Signature", sig_b64)
        .json(body)
        .send()
        .await
        .map_err(|e| format!("{method} {url} failed: {e}"))?;
    let status = response.status().as_u16();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("{method} {url}: invalid JSON response: {e}"))?;
    if expected_status.contains(&status) {
        return Ok(payload);
    }
    Err(format!(
        "{method} {url} rejected ({status}): {}",
        serde_json::to_string(&payload).unwrap_or_default()
    ))
}

/// Mint a short-TTL DevCert from the exemem auth service.
pub(super) async fn mint_dev_cert(
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
) -> Result<DevCert, String> {
    let dev_pubkey = BASE64.encode(dev_key.verifying_key().to_bytes());
    let url = format!("{}/v1/dev-cert", exemem_api_url.trim_end_matches('/'));
    let client = http_client()?;
    // trace-egress: propagate (exemem auth_service /v1/dev-cert mint)
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&json!({ "dev_pubkey": dev_pubkey }))
        .send()
        .await
        .map_err(|e| format!("POST {url} failed: {e}"))?;
    let status = response.status().as_u16();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("POST {url}: invalid JSON response: {e}"))?;
    if status != 200 {
        return Err(format!(
            "dev-cert mint rejected ({status}): {} — is this developer enrolled \
             (paid plan or developer_access grant)?",
            serde_json::to_string(&payload).unwrap_or_default()
        ));
    }
    let cert = payload
        .get("cert")
        .cloned()
        .ok_or_else(|| "dev-cert response missing 'cert'".to_string())?;
    serde_json::from_value(cert).map_err(|e| format!("dev-cert response does not parse: {e}"))
}

/// Base64 `SignatureEnvelope` for an `X-Signature` header (the production
/// counterpart of the test-utils helper of the same name).
pub(super) fn sign_envelope_b64(
    dev_key: &SigningKey,
    purpose: AppIdentityPurpose,
    env: Env,
    payload: &Value,
) -> Result<String, String> {
    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose,
        alg: ALG_ED25519.to_string(),
        key_id: key_id(&dev_key.verifying_key()),
        issued_at: chrono::Utc::now(),
        expires_at: None,
        env,
        payload_hash: compute_payload_hash(payload)
            .map_err(|e| format!("payload hash failed: {e}"))?,
        sig: None,
    };
    let signed =
        sign_envelope(dev_key, unsigned).map_err(|e| format!("envelope signing failed: {e}"))?;
    Ok(BASE64.encode(
        serde_json::to_vec(&signed).map_err(|e| format!("failed to encode envelope: {e}"))?,
    ))
}

// ─── Read surfaces: list + info ────────────────────────────────────────────
