use super::*;

#[derive(Debug, Clone)]
pub(crate) struct NodePublishIdentity {
    pub(crate) signing_key: Arc<SigningKey>,
    pub(crate) public_key_b64: String,
    pub(crate) public_key_hash: String,
    pub(crate) env: Env,
}

impl NodePublishIdentity {
    pub(crate) fn new(signing_key: SigningKey, env: Env) -> Self {
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key: Arc::new(signing_key),
            public_key_b64: BASE64.encode(verifying_key.to_bytes()),
            public_key_hash: key_id(&verifying_key),
            env,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaPowHeaders {
    pub(crate) challenge_id: String,
    pub(crate) nonce: String,
    pub(crate) challenge_mac: String,
    pub(crate) difficulty_bits: u8,
    pub(crate) expires_at_unix_secs: u64,
    pub(crate) counter: u64,
    pub(crate) signature_b64: String,
}

pub(crate) fn error_reason(body: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("reason").and_then(|r| r.as_str()).map(str::to_string))
}

pub(crate) fn schema_add_error(url: &str, status: StatusCode, body: &str) -> FoldDbError {
    if status == StatusCode::TOO_MANY_REQUESTS
        && error_reason(body).as_deref() == Some("quota_exceeded")
    {
        return FoldDbError::Config(format!(
            "Schema service add schema quota exceeded at {url}: {body}"
        ));
    }
    FoldDbError::Config(format!(
        "Schema service add schema failed with status {status}: {body}"
    ))
}

pub(crate) fn schema_challenge_error(url: &str, status: StatusCode, body: &str) -> FoldDbError {
    if status == StatusCode::TOO_MANY_REQUESTS
        && error_reason(body).as_deref() == Some("quota_exceeded")
    {
        return FoldDbError::Config(format!(
            "Schema service mutation challenge quota exceeded at {url}: {body}"
        ));
    }
    FoldDbError::Config(format!(
        "Schema service mutation challenge failed with status {status}: {body}"
    ))
}

pub(crate) fn schema_pow_can_solve(reason: Option<&str>) -> bool {
    matches!(reason, Some("node_key_required" | "proof_of_work_required"))
}

pub(crate) fn sign_schema_claim(
    identity: &NodePublishIdentity,
    schema_hash: &str,
    challenge_id: &str,
    nonce: &str,
    counter: u64,
) -> FoldDbResult<String> {
    let payload = node_signature_payload(
        schema_hash,
        &identity.public_key_hash,
        challenge_id,
        nonce,
        counter,
    );
    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose: Purpose::SchemaClaim,
        alg: ALG_ED25519.to_string(),
        key_id: identity.public_key_hash.clone(),
        issued_at: Utc::now(),
        expires_at: None,
        env: identity.env,
        payload_hash: compute_payload_hash(&payload).map_err(|e| {
            FoldDbError::Config(format!("Failed to hash schema claim payload: {e}"))
        })?,
        sig: None,
    };
    let signed = sign_envelope(&identity.signing_key, unsigned).map_err(|e| {
        FoldDbError::Config(format!(
            "Failed to sign schema mutation proof envelope: {e}"
        ))
    })?;
    let bytes = serde_json::to_vec(&signed).map_err(|e| {
        FoldDbError::Config(format!(
            "Failed to serialize schema mutation proof envelope: {e}"
        ))
    })?;
    Ok(BASE64.encode(bytes))
}

pub(crate) fn sign_dev_schema_claim(
    dev_key: &SigningKey,
    env: Env,
    schema_payload: &serde_json::Value,
) -> FoldDbResult<String> {
    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose: Purpose::SchemaClaim,
        alg: ALG_ED25519.to_string(),
        key_id: key_id(&dev_key.verifying_key()),
        issued_at: Utc::now(),
        expires_at: None,
        env,
        payload_hash: compute_payload_hash(schema_payload).map_err(|e| {
            FoldDbError::Config(format!("Failed to hash schema claim payload: {e}"))
        })?,
        sig: None,
    };
    let signed = sign_envelope(dev_key, unsigned)
        .map_err(|e| FoldDbError::Config(format!("Failed to sign schema claim envelope: {e}")))?;
    let bytes = serde_json::to_vec(&signed)
        .map_err(|e| FoldDbError::Config(format!("Failed to encode schema claim envelope: {e}")))?;
    Ok(BASE64.encode(bytes))
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaClaimAuth {
    pub(crate) cert_b64: String,
    pub(crate) signature_b64: String,
}
