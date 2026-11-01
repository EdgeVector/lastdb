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

    fn http_status(&self) -> u16 {
        match self {
            Self::InvalidChallengeRequest(_) => 400,
            Self::NodeKeyRequired
            | Self::NodeKeyInvalid
            | Self::NodeSignatureRequired
            | Self::NodeSignatureInvalid
            | Self::ProofOfWorkRequired
            | Self::ProofOfWorkInvalid
            | Self::ProofOfWorkExpired => 401,
            Self::QuotaExceeded { .. } => 429,
            Self::Internal(_) => 500,
        }
    }

    fn http_error(&self) -> &'static str {
        match self {
            Self::InvalidChallengeRequest(_) => "Invalid schema mutation challenge request",
            Self::NodeKeyRequired => "Node publish key required",
            Self::NodeKeyInvalid => "Node publish key is invalid",
            Self::NodeSignatureRequired => "Node publish signature required",
            Self::NodeSignatureInvalid => "Node publish signature is invalid",
            Self::ProofOfWorkRequired => "Proof-of-work challenge required",
            Self::ProofOfWorkInvalid => "Proof-of-work is invalid",
            Self::ProofOfWorkExpired => "Proof-of-work challenge expired",
            Self::QuotaExceeded { .. } => "Schema mutation quota exceeded",
            Self::Internal(_) => "Schema mutation gate failed",
        }
    }

    fn error_reason_body(error: &str, reason: &str) -> Value {
        json!({"error": error, "reason": reason})
    }

    fn error_reason_detail_body(error: &str, reason: &str, detail: &str) -> Value {
        json!({"error": error, "reason": reason, "detail": detail})
    }

    pub fn to_http(&self) -> (u16, Value) {
        let reason = self.reason_code();
        let error = self.http_error();
        let body = match self {
            Self::InvalidChallengeRequest(detail) => {
                Self::error_reason_detail_body(error, reason, detail)
            }
            Self::QuotaExceeded {
                bucket,
                window,
                limit,
                retry_after_secs,
            } => json!({
                "error": error,
                "reason": reason,
                "bucket": bucket,
                "window": window,
                "limit": limit,
                "retry_after_secs": retry_after_secs
            }),
            Self::Internal(detail) => Self::error_reason_detail_body(error, reason, detail),
            Self::NodeKeyRequired
            | Self::NodeKeyInvalid
            | Self::NodeSignatureRequired
            | Self::NodeSignatureInvalid
            | Self::ProofOfWorkRequired
            | Self::ProofOfWorkInvalid
            | Self::ProofOfWorkExpired => Self::error_reason_body(error, reason),
        };
        (self.http_status(), body)
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
