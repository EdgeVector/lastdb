//! Lockfile, publish and promote.

use super::*;

// ─── Manifest lockfile (schema name → stored catalog identity) ────────────

/// `<manifest>.lock.json` — records, per manifest schema name, the catalog
/// identity its registration actually produced. Coverage checks trust a
/// locked identity only after re-verifying it still exists in the catalog.
pub fn lockfile_path(manifest_path: &Path) -> PathBuf {
    let mut os = manifest_path.as_os_str().to_owned();
    os.push(".lock.json");
    PathBuf::from(os)
}

pub fn load_lockfile(manifest_path: &Path) -> HashMap<String, String> {
    let path = lockfile_path(manifest_path);
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|v| v.get("schemas").cloned())
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default()
}

// ─── Step 3: publish into the registry ─────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PublishOutcome {
    /// `created` (201) or `idempotent` (200 — same dev re-published).
    pub status: String,
    pub response: Value,
}

#[derive(Debug, Clone)]
pub struct PromoteOutcome {
    pub response: Value,
}

/// Names of manifest schemas that are still novel — the publish gate input.
pub fn novel_schema_names(outcomes: &[CheckOutcome]) -> Vec<String> {
    outcomes
        .iter()
        .filter(|o| !o.coverage.is_covered())
        .map(|o| o.schema_name.clone())
        .collect()
}

/// Fields of a raw manifest schema that lack a non-empty
/// `field_descriptions` entry. Novel registration requires one per field
/// (the catalog matches shapes semantically); surfacing the exact list
/// client-side beats the server's generic 400.
pub fn missing_field_descriptions(raw: &Value) -> Vec<String> {
    let descs = raw.get("field_descriptions").and_then(Value::as_object);
    raw.get("fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(Value::as_str)
                .filter(|f| {
                    descs
                        .and_then(|d| d.get(*f))
                        .and_then(Value::as_str)
                        .is_none_or(|s| s.trim().is_empty())
                })
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Publish the app into the LastDB registry as a sandbox namespace reservation.
/// Novel-schema gating happens at promote time so an app can exist before its
/// schema claims are registered.
pub async fn publish_app(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    manifest: &AppManifest,
) -> Result<PublishOutcome, String> {
    let cert = mint_dev_cert(exemem_api_url, api_key, dev_key).await?;
    let cert_b64 = BASE64
        .encode(serde_json::to_vec(&cert).map_err(|e| format!("failed to encode dev cert: {e}"))?);

    let mut body = json!({
        "app_id": manifest.app_id,
        "metadata": manifest.metadata,
        "version": manifest.version,
    });
    if let Some(source) = &manifest.source {
        body["source"] = serde_json::to_value(source)
            .map_err(|e| format!("failed to encode manifest source: {e}"))?;
    }
    if let Some(artifact) = &manifest.artifact {
        body["artifact"] = serde_json::to_value(artifact)
            .map_err(|e| format!("failed to encode manifest artifact: {e}"))?;
    }
    if !manifest.uses.is_empty() {
        body["uses"] = serde_json::to_value(&manifest.uses)
            .map_err(|e| format!("failed to encode manifest uses: {e}"))?;
    }
    let sig_b64 = sign_envelope_b64(dev_key, AppIdentityPurpose::AppRegister, env, &body)?;

    let url = format!("{}/v1/apps", schema_service_url.trim_end_matches('/'));
    let client = http_client()?;
    // trace-egress: propagate (schema_service /v1/apps app_register publish)
    let response = client
        .post(&url)
        .header("X-Exemem-Dev-Cert", cert_b64)
        .header("X-Signature", sig_b64)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("POST {url} failed: {e}"))?;
    let status = response.status().as_u16();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("POST {url}: invalid JSON response: {e}"))?;
    match status {
        201 => Ok(PublishOutcome {
            status: "created".to_string(),
            response: payload,
        }),
        200 => Ok(PublishOutcome {
            status: payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("idempotent")
                .to_string(),
            response: payload,
        }),
        _ => Err(format!(
            "publish rejected ({status}): {}",
            serde_json::to_string(&payload).unwrap_or_default()
        )),
    }
}

/// Promote a sandbox app to live after the caller has enforced the
/// novel-schema gate. The signed payload matches the schema-service
/// `app_promote` contract: `{ app_id, action: "promote" }`.
pub async fn promote_app(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    manifest: &AppManifest,
) -> Result<PromoteOutcome, String> {
    let cert = mint_dev_cert(exemem_api_url, api_key, dev_key).await?;
    let cert_b64 = BASE64
        .encode(serde_json::to_vec(&cert).map_err(|e| format!("failed to encode dev cert: {e}"))?);

    let body = json!({ "app_id": manifest.app_id, "action": "promote" });
    let sig_b64 = sign_envelope_b64(dev_key, AppIdentityPurpose::AppPromote, env, &body)?;

    let url = format!(
        "{}/v1/apps/{}/promote",
        schema_service_url.trim_end_matches('/'),
        manifest.app_id
    );
    let client = http_client()?;
    // trace-egress: propagate (schema_service /v1/apps/{app_id}/promote app_promote)
    let response = client
        .post(&url)
        .header("X-Exemem-Dev-Cert", cert_b64)
        .header("X-Signature", sig_b64)
        .send()
        .await
        .map_err(|e| format!("POST {url} failed: {e}"))?;
    let status = response.status().as_u16();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("POST {url}: invalid JSON response: {e}"))?;
    match status {
        200 => Ok(PromoteOutcome { response: payload }),
        _ => Err(format!(
            "promote rejected ({status}): {}",
            serde_json::to_string(&payload).unwrap_or_default()
        )),
    }
}

// ─── `/v2` release publishing (developer side) ────────────────────────────
//
// Three DevCert-gated writes. Each mints one short-TTL DevCert and signs
// one envelope whose purpose is pinned to the route, so no signature from
// one write is replayable as another.
//
// None of these registers a schema. The release manifest copies the schema
// identities that already sit in the app lockfile; a schema that is not
// already resolved makes the publish fail, it does not make the registry
// write one.
