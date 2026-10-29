use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ProbeArgs {
    pub(super) url: String,
    pub(super) environment: Env,
    pub(super) run_id: String,
    pub(super) request_timeout_secs: u64,
    pub(super) quota_attempts: Option<u16>,
    pub(super) negative_proof: Option<NegativeProof>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NegativeProof {
    Missing,
    Invalid,
    Expired,
}

pub(super) fn rejection_reason(body: &serde_json::Value) -> &str {
    body.get("reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("missing_reason")
}

pub(super) fn validate_idempotent_repost_identity(
    first_identity_hash: Option<&str>,
    repost_identity_hash: Option<&str>,
) -> Result<(), &'static str> {
    match (first_identity_hash, repost_identity_hash) {
        (Some(first), Some(repost)) if first == repost => Ok(()),
        (Some(_), Some(_)) => Err("identity_hash_mismatch"),
        _ => Err("identity_hash_missing"),
    }
}

impl NegativeProof {
    pub(super) fn expected_reason(self) -> &'static str {
        match self {
            Self::Missing => "node_key_required",
            Self::Invalid => "proof_of_work_invalid",
            Self::Expired => "proof_of_work_expired",
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Expired => "expired",
        }
    }
}

#[derive(Serialize)]
pub(super) struct ProbeReport {
    pub(super) status: &'static str,
    pub(super) environment: &'static str,
    pub(super) schema_name: String,
    pub(super) first_registration_ms: u128,
    pub(super) repost_ms: u128,
    pub(super) resolve_outcome: String,
    pub(super) first_identity_hash: Option<String>,
    pub(super) repost_identity_hash: Option<String>,
    pub(super) protocol_steps: ProtocolStepReport,
    pub(super) private_key_persisted: bool,
}

#[derive(Serialize)]
pub(super) struct ProtocolStepReport {
    pub(super) challenge: &'static str,
    pub(super) grind: &'static str,
    pub(super) signed_retry: &'static str,
    pub(super) idempotent_repost: &'static str,
}

#[derive(Serialize)]
pub(super) struct ProbeProgressReport<'a> {
    pub(super) status: &'static str,
    pub(super) environment: &'a str,
    pub(super) schema_name: &'a str,
    pub(super) phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) negative_proof: Option<&'static str>,
    pub(super) private_key_persisted: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct RegistrationFailureDiagnostic {
    pub(super) reason: &'static str,
    pub(super) owner_surface: &'static str,
    pub(super) diagnostic: &'static str,
}

pub(super) fn endpoint_preflight_diagnostic(url: &str) -> Option<RegistrationFailureDiagnostic> {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return Some(RegistrationFailureDiagnostic {
            reason: "target_url_invalid",
            owner_surface: "schema_service_client_config",
            diagnostic: "The target schema service URL is invalid; inspect the proof harness URL configuration.",
        });
    };
    let Some(host) = parsed.host_str() else {
        return Some(RegistrationFailureDiagnostic {
            reason: "target_url_missing_host",
            owner_surface: "schema_service_client_config",
            diagnostic: "The target schema service URL has no hostname; inspect the proof harness URL configuration.",
        });
    };
    let Some(port) = parsed.port_or_known_default() else {
        return Some(RegistrationFailureDiagnostic {
            reason: "target_url_missing_port",
            owner_surface: "schema_service_client_config",
            diagnostic: "The target schema service URL has no explicit or scheme-default port.",
        });
    };
    match (host, port).to_socket_addrs() {
        Ok(mut addrs) => {
            if addrs.next().is_some() {
                None
            } else {
                Some(RegistrationFailureDiagnostic {
                    reason: "dns_name_unresolved",
                    owner_surface: "schema_service_dns",
                    diagnostic: "The target schema service hostname does not resolve; inspect DNS/custom-domain deployment for the target environment.",
                })
            }
        }
        Err(_) => Some(RegistrationFailureDiagnostic {
            reason: "dns_name_unresolved",
            owner_surface: "schema_service_dns",
            diagnostic: "The target schema service hostname does not resolve; inspect DNS/custom-domain deployment for the target environment.",
        }),
    }
}

pub(super) fn emit_preflight_failure(
    environment: &str,
    schema_name: &str,
    diagnostic: &RegistrationFailureDiagnostic,
) -> ! {
    eprintln!(
        "{}",
        serde_json::json!({
            "status": "FAIL",
            "environment": environment,
            "schema_name": schema_name,
            "reason": diagnostic.reason,
            "owner_surface": diagnostic.owner_surface,
            "diagnostic": diagnostic.diagnostic,
            "private_key_persisted": false
        })
    );
    std::process::exit(1);
}

pub(super) fn now_secs() -> u64 {
    schema_types::clock::unix_secs()
}

pub(super) fn emit_progress(
    environment: &str,
    schema_name: &str,
    phase: &'static str,
    negative_proof: Option<NegativeProof>,
) {
    println!(
        "{}",
        serde_json::to_string(&ProbeProgressReport {
            status: "RUNNING",
            environment,
            schema_name,
            phase,
            negative_proof: negative_proof.map(NegativeProof::label),
            private_key_persisted: false,
        })
        .expect("probe progress report is serializable")
    );
    let _ = std::io::stdout().flush();
}
