//! Secret-safe live probe for the Schema Service mutation PoW path.
//!
//! This deliberately calls the production client rather than reimplementing
//! the challenge, grind, signature, or retry protocol.

use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, Env, Purpose, SignatureEnvelope, SigningKey,
    ALG_ED25519, ENVELOPE_VERSION,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::Utc;
use schema_service_client::{SchemaServiceClient, SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT};
use schema_service_core::schema_mutation_gate::{
    node_signature_payload, pow_satisfies, schema_payload_hash, SchemaMutationChallengeRequest,
    SchemaMutationChallengeResponse, HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE,
    HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC, HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS,
    HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};
use schema_service_core::types::{AddSchemaRequest, SchemaResolveOutcome, SchemaResolveProposal};
use schema_types::{Schema, SchemaType};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 20;
const READ_ONLY_RESOLVE_PREFLIGHT_REGISTRY_VERSION: u64 = 0;

#[derive(Debug, PartialEq, Eq)]
struct ProbeArgs {
    url: String,
    environment: Env,
    run_id: String,
    request_timeout_secs: u64,
    quota_attempts: Option<u16>,
    negative_proof: Option<NegativeProof>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NegativeProof {
    Missing,
    Invalid,
    Expired,
}

fn rejection_reason(body: &serde_json::Value) -> &str {
    body.get("reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("missing_reason")
}

fn validate_idempotent_repost_identity(
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
    fn expected_reason(self) -> &'static str {
        match self {
            Self::Missing => "node_key_required",
            Self::Invalid => "proof_of_work_invalid",
            Self::Expired => "proof_of_work_expired",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Expired => "expired",
        }
    }
}

#[derive(Serialize)]
struct ProbeReport {
    status: &'static str,
    environment: &'static str,
    schema_name: String,
    first_registration_ms: u128,
    repost_ms: u128,
    resolve_outcome: String,
    first_identity_hash: Option<String>,
    repost_identity_hash: Option<String>,
    protocol_steps: ProtocolStepReport,
    private_key_persisted: bool,
}

#[derive(Serialize)]
struct ProtocolStepReport {
    challenge: &'static str,
    grind: &'static str,
    signed_retry: &'static str,
    idempotent_repost: &'static str,
}

#[derive(Serialize)]
struct ProbeProgressReport<'a> {
    status: &'static str,
    environment: &'a str,
    schema_name: &'a str,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    negative_proof: Option<&'static str>,
    private_key_persisted: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct RegistrationFailureDiagnostic {
    reason: &'static str,
    owner_surface: &'static str,
    diagnostic: &'static str,
}

fn endpoint_preflight_diagnostic(url: &str) -> Option<RegistrationFailureDiagnostic> {
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

fn emit_preflight_failure(
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

fn now_secs() -> u64 {
    schema_types::clock::unix_secs()
}

fn emit_progress(
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

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ProbeArgs, String> {
    let mut url = None;
    let mut environment = Env::Dev;
    let mut allow_prod = false;
    let mut run_id = None;
    let mut request_timeout_secs = None;
    let mut quota_attempts = None;
    let mut negative_proof = None;
    let mut iter = args.into_iter();
    let _program = iter.next();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--url" => url = iter.next(),
            "--environment" => {
                environment = match iter.next().as_deref() {
                    Some("dev") => Env::Dev,
                    Some("prod") => Env::Prod,
                    _ => return Err("--environment must be dev or prod".to_string()),
                };
            }
            "--allow-prod" => allow_prod = true,
            "--run-id" => run_id = iter.next(),
            "--request-timeout-secs" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--request-timeout-secs requires an integer".to_string())?;
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| "--request-timeout-secs must be an integer".to_string())?;
                if !(1..=300).contains(&parsed) {
                    return Err("--request-timeout-secs must be between 1 and 300".to_string());
                }
                request_timeout_secs = Some(parsed);
            }
            "--quota-attempts" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--quota-attempts requires an integer".to_string())?;
                let parsed = value
                    .parse::<u16>()
                    .map_err(|_| "--quota-attempts must be an integer".to_string())?;
                if !(2..=24).contains(&parsed) {
                    return Err("--quota-attempts must be between 2 and 24".to_string());
                }
                quota_attempts = Some(parsed);
            }
            "--negative-proof" => {
                negative_proof = Some(match iter.next().as_deref() {
                    Some("missing") => NegativeProof::Missing,
                    Some("invalid") => NegativeProof::Invalid,
                    Some("expired") => NegativeProof::Expired,
                    _ => {
                        return Err(
                            "--negative-proof must be missing, invalid, or expired".to_string()
                        )
                    }
                });
            }
            "--help" | "-h" => {
                return Err(
                    "usage: schema_pow_live_probe --url URL [--environment dev|prod] \
                     [--allow-prod] [--run-id ID] [--quota-attempts N] \
                     [--request-timeout-secs N] \
                     [--negative-proof missing|invalid|expired]"
                        .to_string(),
                );
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let url = url.ok_or_else(|| "--url is required".to_string())?;
    if environment == Env::Prod && !allow_prod {
        return Err("production probe requires --allow-prod".to_string());
    }
    if environment == Env::Prod && quota_attempts.is_some() {
        return Err("quota probe is dev-only".to_string());
    }
    if quota_attempts.is_some() && negative_proof.is_some() {
        return Err("quota and negative proof modes are mutually exclusive".to_string());
    }
    let run_id = match (environment, run_id) {
        (Env::Prod, Some(value)) if value != "official-v1" => {
            return Err("production probe run id is fixed to official-v1".to_string());
        }
        (Env::Prod, _) => "official-v1".to_string(),
        (Env::Dev, Some(value)) => value,
        (Env::Dev, None) => now_secs().to_string(),
    };
    if !run_id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err("--run-id accepts only ASCII letters, digits, and '-'".to_string());
    }
    Ok(ProbeArgs {
        url,
        environment,
        run_id,
        request_timeout_secs: request_timeout_secs.unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS),
        quota_attempts,
        negative_proof,
    })
}

fn ephemeral_signing_key() -> Result<SigningKey, String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|_| "failed to obtain operating-system randomness".to_string())?;
    Ok(SigningKey::from_bytes(&bytes))
}

fn probe_schema(environment: Env, run_id: &str) -> Schema {
    let env = match environment {
        Env::Dev => "dev",
        Env::Prod => "prod",
    };
    let schema_name = format!("schema_pow_live_probe_{env}_{run_id}");
    let field_name = format!("pow_probe_token_{run_id}");
    let mut schema = Schema::new(
        schema_name,
        SchemaType::Single,
        None,
        Some(vec![field_name.clone()]),
        None,
        None,
    );
    schema.descriptive_name = Some(format!("Schema PoW live probe {env} {run_id}"));
    schema.purpose_statement =
        Some("Verify live proof-of-work admission control with a non-sensitive token.".to_string());
    schema.owner_app_id = Some("schema-pow-live-proof".to_string());
    schema.field_descriptions.insert(
        field_name,
        "A non-sensitive synthetic token used only for Schema Service PoW verification."
            .to_string(),
    );
    schema
}

fn resolve_proposal(schema: &Schema) -> SchemaResolveProposal {
    SchemaResolveProposal {
        descriptive_name: schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone()),
        fields: schema.fields.clone().unwrap_or_default(),
        field_descriptions: schema.field_descriptions.clone(),
        purpose_statement: schema.purpose_statement.clone(),
        identity_hash: schema.identity_hash.clone(),
        owner_app_id: schema.owner_app_id.clone(),
    }
}

fn outcome_label(outcome: &SchemaResolveOutcome) -> &'static str {
    match outcome {
        SchemaResolveOutcome::Reuse => "reuse",
        SchemaResolveOutcome::Novel => "novel",
        SchemaResolveOutcome::CandidateEquivalent => "candidate_equivalent",
        SchemaResolveOutcome::Refresh => "refresh",
    }
}

fn registration_failure_diagnostic(error: &dyn std::fmt::Display) -> RegistrationFailureDiagnostic {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    if lower.contains("dns error")
        || lower.contains("could not resolve host")
        || lower.contains("failed to lookup address information")
        || lower.contains("name or service not known")
        || lower.contains("nodename nor servname provided")
    {
        return RegistrationFailureDiagnostic {
            reason: "dns_name_unresolved",
            owner_surface: "schema_service_dns",
            diagnostic: "The target schema service hostname does not resolve; inspect DNS/custom-domain deployment for the target environment.",
        };
    }
    if lower.contains("quota exceeded") || lower.contains("\"reason\":\"quota_exceeded\"") {
        return RegistrationFailureDiagnostic {
            reason: "quota_exceeded",
            owner_surface: "schema_service_config",
            diagnostic: "Schema mutation quota rejected the probe; inspect dev quota/window settings or caller bucket churn.",
        };
    }
    if lower.contains("mutation challenge failed")
        || lower.contains("failed to request schema mutation pow challenge")
    {
        return RegistrationFailureDiagnostic {
            reason: "challenge_request_failed",
            owner_surface: "schema_service_mutation_challenge_endpoint",
            diagnostic:
                "The probe could not obtain a mutation PoW challenge from the target endpoint.",
        };
    }
    if lower.contains("difficulty") && lower.contains("exceeds client cap") {
        return RegistrationFailureDiagnostic {
            reason: "difficulty_exceeds_client_cap",
            owner_surface: "schema_service_pow_config",
            diagnostic: "The endpoint issued a PoW difficulty above the client safety cap.",
        };
    }
    if lower.contains("expired before a solution")
        || lower.contains("\"reason\":\"proof_of_work_expired\"")
    {
        return RegistrationFailureDiagnostic {
            reason: "proof_of_work_expired",
            owner_surface: "schema_service_pow_config",
            diagnostic: "The challenge expired before the client could submit a solution; inspect TTL and difficulty.",
        };
    }
    if lower.contains("hash mismatch") {
        return RegistrationFailureDiagnostic {
            reason: "challenge_hash_mismatch",
            owner_surface: "schema_service_client_contract",
            diagnostic: "The challenge response schema hash did not match the client request hash.",
        };
    }
    if lower.contains("node key mismatch") {
        return RegistrationFailureDiagnostic {
            reason: "challenge_node_key_mismatch",
            owner_surface: "schema_service_client_contract",
            diagnostic: "The challenge response node key did not match the client request key.",
        };
    }
    if lower.contains("\"reason\":\"proof_of_work_invalid\"") {
        return RegistrationFailureDiagnostic {
            reason: "proof_of_work_invalid",
            owner_surface: "schema_service_pow_retry_contract",
            diagnostic: "The endpoint rejected the solved PoW retry; inspect challenge MAC, counter, and signature verification.",
        };
    }
    if lower.contains("\"reason\":\"node_signature_invalid\"")
        || lower.contains("\"reason\":\"node_key_invalid\"")
        || lower.contains("node publish signature")
    {
        return RegistrationFailureDiagnostic {
            reason: "node_signature_invalid",
            owner_surface: "schema_service_node_signature_contract",
            diagnostic:
                "The endpoint rejected the node key or schema-claim signature on the retry.",
        };
    }
    if lower.contains("\"reason\":\"cert_invalid\"")
        || lower.contains("\"reason\":\"app_not_registered\"")
        || lower.contains("\"reason\":\"app_owner_mismatch\"")
    {
        return RegistrationFailureDiagnostic {
            reason: "app_identity_claim_rejected",
            owner_surface: "schema_service_app_identity_config",
            diagnostic: "The endpoint rejected owner_app_id claim authorization before or during mutation admission.",
        };
    }
    if lower.contains("failed to parse schema response") {
        return RegistrationFailureDiagnostic {
            reason: "response_contract_invalid",
            owner_surface: "schema_service_response_contract",
            diagnostic: "The endpoint returned a 2xx add-schema response that the client could not deserialize.",
        };
    }
    if lower.contains("failed to submit schema")
        || lower.contains("error sending request")
        || lower.contains("is the schema service running")
    {
        return RegistrationFailureDiagnostic {
            reason: "endpoint_unreachable",
            owner_surface: "schema_service_deploy",
            diagnostic: "The probe could not reach the schema service add-schema endpoint.",
        };
    }
    RegistrationFailureDiagnostic {
        reason: "registration_failed",
        owner_surface: "schema_service_unknown",
        diagnostic: "The add-schema request failed without a recognized safe error shape; inspect client/server logs for the run id.",
    }
}

#[path = "schema_pow_live_probe/negative.rs"]
mod negative;
use negative::run_negative_probe;

#[path = "schema_pow_live_probe/phases.rs"]
mod phases;
use phases::{run_negative_mode, run_quota_probe, run_read_only_resolve, run_registration_proof};

#[tokio::main]
async fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("schema_pow_live_probe: {error}");
            std::process::exit(2);
        }
    };
    let environment_label = match args.environment {
        Env::Dev => "dev",
        Env::Prod => "prod",
    };
    let schema = probe_schema(args.environment, &args.run_id);
    if let Some(diagnostic) = endpoint_preflight_diagnostic(&args.url) {
        emit_preflight_failure(environment_label, &schema.name, &diagnostic);
    }
    let signing_key = match ephemeral_signing_key() {
        Ok(key) => key,
        Err(error) => {
            eprintln!("schema_pow_live_probe: {error}");
            std::process::exit(1);
        }
    };

    if let Some(mode) = args.negative_proof {
        run_negative_mode(&args, &schema, &signing_key, mode, environment_label).await;
        return;
    }
    let client = SchemaServiceClient::new_with_timeout(
        &args.url,
        Duration::from_secs(args.request_timeout_secs),
    )
    .with_node_identity(signing_key, args.environment);

    if let Some(max_attempts) = args.quota_attempts {
        run_quota_probe(&client, &args, environment_label, max_attempts).await;
        return;
    }

    let resolve_outcome = run_read_only_resolve(&client, &schema, environment_label).await;
    run_registration_proof(&client, &schema, environment_label, resolve_outcome).await;
}
