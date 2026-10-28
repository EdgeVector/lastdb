//! Operator-only extension of the standard field vocabulary.
//!
//! The configured registry verifier key also controls canonical approval.
//! App owner certificates do not grant this permission. The signed operation
//! and environment prevent a compatibility-row or cross-environment replay.

use crate::app_identity::env_label;
use crate::field_name_policy::NAME_POLICY_VERSION;
use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use ed25519_dalek::Verifier;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishRequest {
    operation: String,
    environment: String,
    name_policy_version: u32,
    fields: HashMap<String, CanonicalField>,
}

pub struct VocabularyPublishError {
    pub status: u16,
    pub reason: &'static str,
    pub detail: String,
}

fn failure(status: u16, reason: &'static str, detail: impl Into<String>) -> VocabularyPublishError {
    VocabularyPublishError {
        status,
        reason,
        detail: detail.into(),
    }
}

impl SchemaServiceState {
    /// Public definitions, never user record values or declaration permissions.
    pub fn canonical_vocabulary(&self) -> schema_types::FoldDbResult<Value> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        Ok(json!({"name_policy_version": NAME_POLICY_VERSION, "fields": &*fields}))
    }

    pub async fn publish_canonical_vocabulary(
        &self,
        signature: Option<&str>,
        body: &Value,
    ) -> Result<usize, VocabularyPublishError> {
        let config = self.app_identity_config();
        let key = config.verifier_pubkey.as_deref().ok_or_else(|| {
            failure(
                503,
                "canonical_publisher_not_configured",
                "registry verifier key is required",
            )
        })?;
        let request: PublishRequest = serde_json::from_value(body.clone())
            .map_err(|error| failure(400, "invalid_canonical_publication", error.to_string()))?;
        if request.operation != "canonical_fields.publish"
            || request.environment != env_label(config.deployment_env)
            || request.name_policy_version != NAME_POLICY_VERSION
            || request.fields.is_empty()
            || request.fields.len() > 512
        {
            return Err(failure(400, "invalid_canonical_publication",
                "require canonical_fields.publish, this environment, the active policy and 1..512 fields"));
        }
        verify_operator_signature(key, signature, body)?;
        let count = request.fields.len();
        let fields: Vec<_> = request.fields.into_iter().collect();
        self.add_canonical_fields(&fields).await.map_err(|error| {
            let detail = error.to_string();
            let reason = if detail.contains("invalid_canonical_name:") {
                "invalid_canonical_name"
            } else if detail.contains("canonical_field_conflict:") {
                "canonical_field_conflict"
            } else {
                "canonical_publication_failed"
            };
            let status = if reason == "canonical_publication_failed" {
                500
            } else {
                400
            };
            failure(status, reason, detail)
        })?;
        Ok(count)
    }
}

fn verify_operator_signature(
    key: &str,
    signature: Option<&str>,
    body: &Value,
) -> Result<(), VocabularyPublishError> {
    let invalid = || {
        failure(
            401,
            "canonical_signature_invalid",
            "operator signature is required",
        )
    };
    let key = app_identity_crypto::verifying_key_from_base64(key).map_err(|_| {
        failure(
            503,
            "canonical_publisher_not_configured",
            "invalid registry verifier key",
        )
    })?;
    let bytes = BASE64
        .decode(signature.ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let signature = ed25519_dalek::Signature::from_slice(&bytes).map_err(|_| invalid())?;
    let canonical = app_identity_crypto::canonicalize(body).map_err(|_| invalid())?;
    key.verify(&canonical, &signature).map_err(|_| invalid())
}
