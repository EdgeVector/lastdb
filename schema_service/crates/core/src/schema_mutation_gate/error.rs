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
    fn reason_code(&self) -> &'static str {
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

    /// Body with `error` and `reason` and no other fields.
    fn error_reason_body(error: &'static str, reason: &str) -> Value {
        json!({"error": error, "reason": reason})
    }

    pub fn to_http(&self) -> (u16, Value) {
        let reason = self.reason_code();
        match self {
            Self::InvalidChallengeRequest(detail) => (
                400,
                json!({"error": "Invalid schema mutation challenge request", "reason": reason, "detail": detail}),
            ),
            Self::NodeKeyRequired => (
                401,
                Self::error_reason_body("Node publish key required", reason),
            ),
            Self::NodeKeyInvalid => (
                401,
                Self::error_reason_body("Node publish key is invalid", reason),
            ),
            Self::NodeSignatureRequired => (
                401,
                Self::error_reason_body("Node publish signature required", reason),
            ),
            Self::NodeSignatureInvalid => (
                401,
                Self::error_reason_body("Node publish signature is invalid", reason),
            ),
            Self::ProofOfWorkRequired => (
                401,
                Self::error_reason_body("Proof-of-work challenge required", reason),
            ),
            Self::ProofOfWorkInvalid => (
                401,
                Self::error_reason_body("Proof-of-work is invalid", reason),
            ),
            Self::ProofOfWorkExpired => (
                401,
                Self::error_reason_body("Proof-of-work challenge expired", reason),
            ),
            Self::QuotaExceeded {
                bucket,
                window,
                limit,
                retry_after_secs,
            } => (
                429,
                json!({"error": "Schema mutation quota exceeded", "reason": reason, "bucket": bucket, "window": window, "limit": limit, "retry_after_secs": retry_after_secs}),
            ),
            Self::Internal(detail) => (
                500,
                json!({"error": "Schema mutation gate failed", "reason": reason, "detail": detail}),
            ),
        }
    }

    pub(super) fn metric_status(&self) -> &'static str {
        self.reason_code()
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
