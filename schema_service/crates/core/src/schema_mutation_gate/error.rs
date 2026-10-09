//! Schema-mutation gate errors, HTTP mapping and result logging.

use super::*;

#[derive(Debug, Clone)]
pub enum SchemaMutationGateError {
    InvalidChallengeRequest(&'static str),
    NodeKeyRequired,
    NodeKeyInvalid,
    NodeSignatureRequired,
    NodeSignatureInvalid,
    ProofOfWorkRequired,
    ProofOfWorkInvalid,
    ProofOfWorkExpired,
    QuotaExceeded {
        bucket: &'static str,
        window: &'static str,
        limit: usize,
        retry_after_secs: u64,
    },
    Internal(String),
}

impl SchemaMutationGateError {
    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::InvalidChallengeRequest(detail) => (
                400,
                json!({"error": "Invalid schema mutation challenge request", "reason": "invalid_challenge_request", "detail": detail}),
            ),
            Self::NodeKeyRequired => (
                401,
                json!({"error": "Node publish key required", "reason": "node_key_required"}),
            ),
            Self::NodeKeyInvalid => (
                401,
                json!({"error": "Node publish key is invalid", "reason": "node_key_invalid"}),
            ),
            Self::NodeSignatureRequired => (
                401,
                json!({"error": "Node publish signature required", "reason": "node_signature_required"}),
            ),
            Self::NodeSignatureInvalid => (
                401,
                json!({"error": "Node publish signature is invalid", "reason": "node_signature_invalid"}),
            ),
            Self::ProofOfWorkRequired => (
                401,
                json!({"error": "Proof-of-work challenge required", "reason": "proof_of_work_required"}),
            ),
            Self::ProofOfWorkInvalid => (
                401,
                json!({"error": "Proof-of-work is invalid", "reason": "proof_of_work_invalid"}),
            ),
            Self::ProofOfWorkExpired => (
                401,
                json!({"error": "Proof-of-work challenge expired", "reason": "proof_of_work_expired"}),
            ),
            Self::QuotaExceeded {
                bucket,
                window,
                limit,
                retry_after_secs,
            } => (
                429,
                json!({"error": "Schema mutation quota exceeded", "reason": "quota_exceeded", "bucket": bucket, "window": window, "limit": limit, "retry_after_secs": retry_after_secs}),
            ),
            Self::Internal(detail) => (
                500,
                json!({"error": "Schema mutation gate failed", "reason": "internal", "detail": detail}),
            ),
        }
    }

    pub(super) fn metric_status(&self) -> &'static str {
        match self {
            Self::InvalidChallengeRequest(_) => "invalid_challenge_request",
            Self::NodeKeyRequired => "node_key_required",
            Self::NodeKeyInvalid => "node_key_invalid",
            Self::NodeSignatureRequired => "node_signature_required",
            Self::NodeSignatureInvalid => "node_signature_invalid",
            Self::ProofOfWorkRequired => "proof_of_work_required",
            Self::ProofOfWorkInvalid => "proof_of_work_invalid",
            Self::ProofOfWorkExpired => "proof_of_work_expired",
            Self::QuotaExceeded { .. } => "quota_exceeded",
            Self::Internal(_) => "internal",
        }
    }
}

pub fn log_schema_mutation_gate_result(result: &Result<(), SchemaMutationGateError>) {
    match result {
        Ok(()) => {}
        Err(SchemaMutationGateError::QuotaExceeded {
            bucket,
            window,
            limit,
            ..
        }) => {
            tracing::info!(
                target: "schema_service::schema",
                metric = "schema_mutation_gate_enforce_total",
                status = "quota_exceeded",
                bucket,
                window,
                limit,
                "schema mutation gate rejected node-key proof"
            );
        }
        Err(error) => {
            tracing::info!(
                target: "schema_service::schema",
                metric = "schema_mutation_gate_enforce_total",
                status = error.metric_status(),
                "schema mutation gate rejected node-key proof"
            );
        }
    }
}
