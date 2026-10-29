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

#[path = "schema_pow_live_probe/model.rs"]
mod model;
#[path = "schema_pow_live_probe/schema.rs"]
mod schema;
use model::*;
use schema::*;

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
