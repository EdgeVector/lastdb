//! Live probe for the same-app distinct-name veto (fold #2161).
//!
//! Registers two schemas of ONE owner app with different descriptive names
//! but field descriptions that a similarity match pairs up (the shape that
//! merged lastgit's `LastgitRepoIndex` into `LastgitOpenCrIndex`, prod
//! identity b7c537cc). PASS requires:
//!
//! - the second schema registers as its own identity (no expansion into the
//!   first, no `replaced_schema`, its own descriptive name kept);
//! - the first schema's field list is unchanged afterwards;
//! - an identical re-POST of the second schema is accepted with the same
//!   identity (idempotent).
//!
//! Uses the production client (challenge, grind, signed retry) with an
//! ephemeral node key. The owner app id is a local namespace claim, so no
//! DevCert is needed. Prints one JSON report on stdout.
//!
//! Usage: same_app_veto_live_probe --url <schema-service-url> --run-id <id>

use app_identity_crypto::{Env, SigningKey};
use schema_service_client::SchemaServiceClient;
use schema_types::{Schema, SchemaType};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

const OWNER_APP_ID: &str = "schema-veto-live-proof";

struct ProbeArgs {
    url: String,
    run_id: String,
    environment: Env,
}

fn parse_args() -> Result<ProbeArgs, String> {
    let mut url = None;
    let mut run_id = None;
    let mut environment = Env::Dev;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = args.next(),
            "--run-id" => run_id = args.next(),
            "--prod" => environment = Env::Prod,
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let url = url.ok_or("--url is required")?;
    let run_id = run_id.ok_or("--run-id is required")?;
    if run_id.is_empty()
        || !run_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err("--run-id accepts only ASCII letters, digits, and '-'".to_string());
    }
    Ok(ProbeArgs {
        url,
        run_id,
        environment,
    })
}

fn ephemeral_signing_key() -> Result<SigningKey, String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|_| "failed to obtain operating-system randomness".to_string())?;
    Ok(SigningKey::from_bytes(&bytes))
}

/// One rollup schema. `rollup` names what the rollup lists; everything else
/// (field roles, the shared timestamp description) is the same, as in the
/// lastgit pair.
fn rollup_schema(run_id: &str, rollup: &str, display: &str, prefix: &str) -> Schema {
    let key = format!("{prefix}key");
    let payload = format!("{prefix}payload_json");
    let updated = format!("{prefix}updated_at");
    let mut schema = Schema::new(
        format!("veto_probe_{rollup}_index_{run_id}"),
        SchemaType::Single,
        None,
        Some(vec![key.clone(), payload.clone(), updated.clone()]),
        None,
        None,
    );
    schema.descriptive_name = Some(format!("VetoProbe{display}Index {run_id}"));
    schema.purpose_statement = Some(format!(
        "Rollup of all {rollup} rows so the list view never scans the full {rollup} schema."
    ));
    schema.owner_app_id = Some(OWNER_APP_ID.to_string());
    schema.field_descriptions.insert(
        key,
        format!("Constant partition key for the all_{rollup} rollup row."),
    );
    schema.field_descriptions.insert(
        payload,
        format!("JSON array of every {rollup} row in the rollup."),
    );
    schema.field_descriptions.insert(
        updated,
        "RFC 3339 timestamp of last index patch.".to_string(),
    );
    schema
}

fn fail(reason: &str, detail: &str) -> ! {
    eprintln!(
        "{}",
        serde_json::json!({"status": "FAIL", "reason": reason, "detail": detail})
    );
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let args = parse_args().unwrap_or_else(|e| {
        eprintln!("same_app_veto_live_probe: {e}");
        std::process::exit(2);
    });
    let key = ephemeral_signing_key().unwrap_or_else(|e| fail("no_randomness", &e));
    let client = SchemaServiceClient::new_with_timeout(&args.url, Duration::from_secs(30))
        .with_node_identity(key, args.environment);

    let first = rollup_schema(&args.run_id, "open_items", "OpenItems", "open_items_");
    let second = rollup_schema(&args.run_id, "repos", "Repos", "");

    let a = client
        .add_schema(&first, HashMap::new())
        .await
        .unwrap_or_else(|e| fail("first_registration_failed", &e.to_string()));
    let b = client
        .add_schema(&second, HashMap::new())
        .await
        .unwrap_or_else(|e| fail("second_registration_failed", &e.to_string()));
    let b_again = client
        .add_schema(&second, HashMap::new())
        .await
        .unwrap_or_else(|e| fail("idempotent_repost_failed", &e.to_string()));
    let a_after = client
        .get_schema(&a.schema.name)
        .await
        .unwrap_or_else(|e| fail("first_readback_failed", &e.to_string()));
    let a_fields = a.schema.fields.unwrap_or_default();
    let a_after_fields = a_after.schema.fields.unwrap_or_default();

    let checks = serde_json::json!({
        "distinct_identity": a.schema.name != b.schema.name,
        "second_not_replacing": b.replaced_schema.is_none(),
        "second_keeps_its_name": b.schema.descriptive_name == second.descriptive_name,
        "first_fields_unchanged": a_fields == a_after_fields,
        "repost_same_identity": b_again.schema.name == b.schema.name
            && b_again.schema.identity_hash == b.schema.identity_hash,
    });
    let pass = checks
        .as_object()
        .is_some_and(|m| m.values().all(|v| v.as_bool() == Some(true)));
    let report = serde_json::json!({
        "status": if pass { "PASS" } else { "FAIL" },
        "url": args.url,
        "owner_app_id": OWNER_APP_ID,
        "first": {"name": a.schema.name, "descriptive_name": a.schema.descriptive_name,
                  "fields": a_fields, "fields_after": a_after_fields},
        "second": {"name": b.schema.name, "descriptive_name": b.schema.descriptive_name,
                   "fields": b.schema.fields, "replaced_schema": b.replaced_schema,
                   "mutation_mappers": b.mutation_mappers},
        "repost": {"name": b_again.schema.name, "identity_hash": b_again.schema.identity_hash},
        "checks": checks,
        "private_key_persisted": false,
    });
    println!("{report}");
    if !pass {
        std::process::exit(1);
    }
}
