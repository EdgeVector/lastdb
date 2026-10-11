//! Probe phases driven by `main`: negative, quota, resolve, registration.

use super::*;
use std::time::Instant;

/// Print a FAIL record to stderr and exit 1.
#[allow(clippy::needless_pass_by_value)] // call sites build the record inline
pub(super) fn fail_exit(record: serde_json::Value) -> ! {
    eprintln!("{record}");
    std::process::exit(1);
}

/// FAIL record for the registration phase, with the shared fixed fields.
pub(super) fn registration_failure(
    environment: &str,
    schema_name: &str,
    reason: &str,
    owner_surface: &str,
    diagnostic: &str,
) -> serde_json::Value {
    serde_json::json!({
        "status": "FAIL",
        "environment": environment,
        "schema_name": schema_name,
        "reason": reason,
        "owner_surface": owner_surface,
        "diagnostic": diagnostic,
        "private_key_persisted": false
    })
}

pub(super) async fn run_negative_mode(
    args: &ProbeArgs,
    schema: &Schema,
    signing_key: &SigningKey,
    mode: NegativeProof,
    environment_label: &str,
) {
    if let Err(reason) =
        run_negative_probe(args, schema, signing_key, mode, environment_label).await
    {
        fail_exit(serde_json::json!({
            "status": "FAIL",
            "environment": environment_label,
            "negative_proof": mode.label(),
            "reason": reason
        }));
    }
    println!(
        "{}",
        serde_json::json!({
            "status": "PASS",
            "environment": environment_label,
            "negative_proof": mode.label(),
            "rejection": mode.expected_reason(),
            "private_key_persisted": false
        })
    );
}

pub(super) async fn run_quota_probe(
    client: &SchemaServiceClient,
    args: &ProbeArgs,
    environment_label: &str,
    max_attempts: u16,
) {
    for attempt in 1..=max_attempts {
        let quota_run_id = format!("{}-quota-{attempt}", args.run_id);
        let schema = probe_schema(args.environment, &quota_run_id);
        emit_progress(
            environment_label,
            &schema.name,
            "quota-registration-submit",
            None,
        );
        match client.add_schema(&schema, HashMap::new()).await {
            Ok(_) => {}
            Err(error) if error.to_string().contains("quota exceeded") => {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "PASS",
                        "environment": environment_label,
                        "quota_probe": true,
                        "attempts": attempt,
                        "accepted_before_reject": attempt - 1,
                        "rejection": "quota_exceeded",
                        "private_key_persisted": false
                    })
                );
                return;
            }
            Err(_) => fail_exit(serde_json::json!({
                "status": "FAIL",
                "environment": environment_label,
                "reason": "unexpected_registration_failure",
                "attempt": attempt
            })),
        }
    }
    fail_exit(serde_json::json!({
        "status": "FAIL",
        "environment": environment_label,
        "reason": "quota_not_rejected",
        "attempts": max_attempts
    }));
}

/// Prove the read-only resolve endpoint answers for the probe proposal and
/// return its outcome label.
pub(super) async fn run_read_only_resolve(
    client: &SchemaServiceClient,
    schema: &Schema,
    environment_label: &str,
) -> String {
    let schema_name = schema.name.clone();
    emit_progress(environment_label, &schema_name, "read-only-resolve", None);
    let resolve = match client
        // The live proof only needs to prove the read-only endpoint responds
        // before mutation; a stale version asks the service for its bounded
        // refresh verdict instead of running native component-cover.
        .resolve_schemas(
            Some(READ_ONLY_RESOLVE_PREFLIGHT_REGISTRY_VERSION),
            vec![resolve_proposal(schema)],
        )
        .await
    {
        Ok(response) => response,
        Err(error) => fail_exit(serde_json::json!({
            "status": "FAIL",
            "environment": environment_label,
            "schema_name": schema_name,
            "reason": "read_only_resolve_failed",
            "owner_surface": "schema_service_resolve_endpoint",
            "diagnostic": "The read-only /v1/schemas/resolve path failed before the mutation proof; inspect the dev schema service deployment and resolver logs.",
            "error": error.to_string(),
            "private_key_persisted": false
        })),
    };
    let proposal_name = schema
        .descriptive_name
        .as_deref()
        .unwrap_or(schema_name.as_str());
    match resolve
        .results
        .get(proposal_name)
        .or_else(|| resolve.results.get(&schema_name))
    {
        Some(result) => outcome_label(&result.outcome).to_string(),
        None => fail_exit(registration_failure(
            environment_label,
            &schema_name,
            "read_only_resolve_missing_result",
            "schema_service_resolve_endpoint",
            "The read-only /v1/schemas/resolve response did not include the probe proposal result.",
        )),
    }
}

/// Register the probe schema twice and require the same identity both times.
pub(super) async fn run_registration_proof(
    client: &SchemaServiceClient,
    schema: &Schema,
    environment_label: &'static str,
    resolve_outcome: String,
) {
    let schema_name = schema.name.clone();
    let first_started = Instant::now();
    emit_progress(
        environment_label,
        &schema_name,
        "valid-registration-first-submit",
        None,
    );
    let first = match client.add_schema(schema, HashMap::new()).await {
        Ok(response) => response,
        Err(error) => {
            let diagnostic = registration_failure_diagnostic(&error);
            fail_exit(registration_failure(
                environment_label,
                &schema_name,
                diagnostic.reason,
                diagnostic.owner_surface,
                diagnostic.diagnostic,
            ))
        }
    };
    let first_registration_ms = first_started.elapsed().as_millis();

    let repost_started = Instant::now();
    emit_progress(
        environment_label,
        &schema_name,
        "valid-registration-repost",
        None,
    );
    let repost = match client.add_schema(schema, HashMap::new()).await {
        Ok(response) => response,
        Err(error) => {
            let diagnostic = registration_failure_diagnostic(&error);
            fail_exit(registration_failure(
                environment_label,
                &schema_name,
                "idempotent_repost_failed",
                diagnostic.owner_surface,
                diagnostic.diagnostic,
            ))
        }
    };

    if let Err(reason) = validate_idempotent_repost_identity(
        first.schema.identity_hash.as_deref(),
        repost.schema.identity_hash.as_deref(),
    ) {
        fail_exit(registration_failure(
            environment_label,
            &schema_name,
            reason,
            "schema_service_idempotent_repost",
            "The idempotent repost did not return the same non-empty schema identity as the first registration.",
        ));
    }

    let report = ProbeReport {
        status: "PASS",
        environment: environment_label,
        schema_name,
        first_registration_ms,
        repost_ms: repost_started.elapsed().as_millis(),
        resolve_outcome,
        first_identity_hash: first.schema.identity_hash,
        repost_identity_hash: repost.schema.identity_hash,
        protocol_steps: ProtocolStepReport {
            challenge: "PASS",
            grind: "PASS",
            signed_retry: "PASS",
            idempotent_repost: "PASS",
        },
        private_key_persisted: false,
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("probe report is serializable")
    );
}
