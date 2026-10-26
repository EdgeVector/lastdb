//! Verifier-signed compat-row write.

use super::*;

impl SchemaServiceState {
    /// `POST /v2/releases/{release_id}/compat` — write a verifier-signed compat row.
    ///
    /// The envelope must be signed by the configured `verifier_pubkey` in
    /// `AppIdentityConfig`. App owner DevCerts are rejected.
    ///
    /// # Errors
    /// See [`CompatRowError`].
    pub async fn write_compat_row(
        &self,
        release_id: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<CompatRow, CompatRowError> {
        let config = self.app_identity_config();
        let result = self
            .write_compat_row_inner(&config, release_id, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "compat_row_write",
            match &result {
                Ok(_) => "created",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn write_compat_row_inner(
        &self,
        config: &AppIdentityConfig,
        release_id: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<CompatRow, CompatRowError> {
        let Some(verifier_pubkey) = &config.verifier_pubkey else {
            return Err(CompatRowError::VerifierNotConfigured);
        };

        #[derive(Deserialize)]
        struct CompatRowRequest {
            release_id: String,
            contract_version: String,
            #[serde(default)]
            verification_url: Option<String>,
        }
        let request: CompatRowRequest = serde_json::from_value(body.clone())
            .map_err(|e| CompatRowError::InvalidBody(format!("malformed body: {e}")))?;

        if request.release_id != release_id {
            return Err(CompatRowError::InvalidBody(
                "release_id in body does not match path".to_string(),
            ));
        }

        validate_contract_version(&request.contract_version)?;

        if self.get_release(release_id).is_none() {
            return Err(CompatRowError::UnknownRelease(release_id.to_string()));
        }

        let (payload_hash, signature) =
            verify_compat_signature(verifier_pubkey, sig_header, body)?;

        let record = CompatRow {
            release_id: release_id.to_string(),
            contract_version: request.contract_version,
            verifier_pubkey: verifier_pubkey.clone(),
            verified_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            verification_url: request.verification_url,
            payload_hash,
            signature,
        };

        self.storage
            .backend()
            .save_compat_row(&record)
            .await
            .map_err(|e| {
                if e.to_string().contains("409") || e.to_string().contains("already exists") {
                    CompatRowError::AlreadyExists
                } else {
                    CompatRowError::Internal(e.to_string())
                }
            })?;

        Ok(record)
    }
}

fn validate_contract_version(version: &str) -> Result<(), CompatRowError> {
    if version.is_empty()
        || version.len() > 64
        || !version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == 'x')
    {
        return Err(CompatRowError::InvalidBody(
            "contract_version must be 1-64 chars matching [a-zA-Z0-9.-x]+".to_string(),
        ));
    }
    Ok(())
}

fn verify_compat_signature(
    verifier_pubkey: &str,
    sig_header: &str,
    body: &Value,
) -> Result<(String, String), CompatRowError> {
    use app_identity_crypto::{compute_payload_hash, verifying_key_from_base64};
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;

    let envelope: app_identity_crypto::SignatureEnvelope =
        serde_json::from_str(sig_header).map_err(|_| CompatRowError::SignatureInvalid)?;

    let verifying_key =
        verifying_key_from_base64(verifier_pubkey).map_err(|_| CompatRowError::VerifierNotConfigured)?;

    let envelope_payload_hash = envelope.payload_hash.as_str();
    let computed_hash = compute_payload_hash(body).map_err(|_| CompatRowError::SignatureInvalid)?;

    if envelope_payload_hash != computed_hash {
        return Err(CompatRowError::SignatureInvalid);
    }

    let sig_b64 = envelope
        .sig
        .as_ref()
        .ok_or(CompatRowError::SignatureInvalid)?;
    let sig_bytes = BASE64
        .decode(sig_b64.as_bytes())
        .map_err(|_| CompatRowError::SignatureInvalid)?;
    let signature = ::ed25519_dalek::Signature::from_slice(&sig_bytes)
        .map_err(|_| CompatRowError::SignatureInvalid)?;

    use ::ed25519_dalek::Verifier;
    let body_canonical =
        app_identity_crypto::canonicalize(body).map_err(|_| CompatRowError::SignatureInvalid)?;
    verifying_key
        .verify(&body_canonical, &signature)
        .map_err(|_| CompatRowError::SignatureInvalid)?;

    Ok((envelope.payload_hash, sig_b64.clone()))
}
