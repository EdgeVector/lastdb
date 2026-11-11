//! Schema Service advice, service URL / API key lookup, and manifest retention checks.

use super::*;
use serde::Deserialize;
use serde_json::{json, Value};

const ADVICE_TIMEOUT: Duration = Duration::from_secs(5);
const KNOWN_KINDS: [&str; 7] = [
    "canonical_match",
    "same_name_different_meaning",
    "near_match",
    "abbreviation",
    "missing_type",
    "suggested_classification",
    "similar_schema",
];

#[derive(Deserialize)]
struct AdviceReply {
    findings: Vec<Finding>,
}

#[derive(Deserialize)]
struct Finding {
    kind: String,
    #[serde(default)]
    field: Option<String>,
    message: String,
}

enum Unavailable {
    Unreachable(String),
    Schema(String),
}

async fn advise(
    client: &reqwest::Client,
    url: &str,
    app_id: &str,
    schema: &Value,
) -> Result<Vec<Finding>, Unavailable> {
    let response = client
        .post(url)
        .json(&json!({ "app_id": app_id, "schema": schema }))
        .send()
        .await
        .map_err(|e| {
            Unavailable::Unreachable(if e.is_timeout() {
                format!("no reply within {}s", ADVICE_TIMEOUT.as_secs())
            } else {
                e.to_string()
            })
        })?;
    if !response.status().is_success() {
        return Err(Unavailable::Schema(format!("HTTP {}", response.status())));
    }
    response
        .json::<AdviceReply>()
        .await
        .map(|reply| reply.findings)
        .map_err(|e| Unavailable::Schema(format!("reply does not parse: {e}")))
}

fn advice_lines(
    schema_service: &str,
    app_id: &str,
    schemas: &[Value],
) -> Result<Vec<String>, String> {
    let url = format!("{}/v1/advice/schema", schema_service.trim_end_matches('/'));
    // trace-egress: propagate (Schema Service advice; read-only, no credentials)
    let client = reqwest::Client::builder()
        .timeout(ADVICE_TIMEOUT)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())?;
    block_on_app(async {
        let mut lines = Vec::new();
        for schema in schemas {
            let label = schema
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>");
            match advise(&client, &url, app_id, schema).await {
                Ok(findings) => lines.extend(
                    findings
                        .iter()
                        .filter(|f| KNOWN_KINDS.contains(&f.kind.as_str()))
                        .map(|f| {
                            let target = f.field.as_deref().map_or_else(
                                || label.to_string(),
                                |field| format!("{label}.{field}"),
                            );
                            format!(
                                "advice  {target}  {}: {}",
                                f.kind,
                                f.message.replace(['\n', '\r'], " ")
                            )
                        }),
                ),
                Err(Unavailable::Schema(reason)) => {
                    lines.push(format!("advice unavailable: {label}: {reason}"));
                }
                Err(Unavailable::Unreachable(reason)) => {
                    lines.push(format!("advice unavailable: {reason}"));
                    break;
                }
            }
        }
        Ok(lines)
    })
}

pub(crate) fn print_schema_advice(
    schema_service: Result<String, String>,
    app_id: &str,
    schemas: &[Value],
) {
    if schemas.is_empty() {
        return;
    }
    for line in schema_service
        .and_then(|url| advice_lines(&url, app_id, schemas))
        .unwrap_or_else(|e| vec![format!("advice unavailable: {e}")])
    {
        eprintln!("{line}");
    }
}

pub(crate) fn resolve_schema_service_url(
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<String, String> {
    use lastdb_node::profile::endpoints::{schema_service_url_for, Environment};
    match (schema_url, env) {
        (Some(url), _) => Ok(url),
        (None, Some("dev") | None) => Ok(schema_service_url_for(Environment::Dev).to_string()),
        (None, Some("prod")) => Ok(schema_service_url_for(Environment::Prod).to_string()),
        (None, Some(other)) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
    }
}

pub(crate) fn require_dev_api_key(api_key: Option<String>) -> Result<String, String> {
    match api_key.or_else(|| std::env::var("EXEMEM_DEV_API_KEY").ok()) {
        Some(k) if !k.trim().is_empty() => Ok(k),
        _ => Err("no developer API key — pass --api-key or set EXEMEM_DEV_API_KEY".into()),
    }
}

fn policy_in_force(retention: &Value, key: &str) -> Option<String> {
    let policy = retention.get(key).filter(|p| !p.is_null())?;
    Some(format!(
        "{}s source={}",
        policy.get("ttl_seconds")?.as_u64()?,
        policy.get("source")?.as_str()?
    ))
}

fn summary_line(retention: &Value) -> String {
    let ttl = retention
        .get("ttl_seconds")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let status = retention
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("");
    let anchor = retention
        .get("anchor")
        .and_then(Value::as_str)
        .map(|a| format!(" anchor={a}"))
        .unwrap_or_default();
    let change = retention
        .get("change")
        .and_then(Value::as_str)
        .filter(|c| *c != "new" && *c != "unchanged")
        .map(|c| {
            let prev = retention
                .get("previous_ttl_seconds")
                .and_then(Value::as_u64)
                .map(|p| format!(" from {p}s"))
                .unwrap_or_default();
            format!(" ({c}{prev})")
        })
        .unwrap_or_default();
    if status == "operator_override" {
        let in_force = policy_in_force(retention, "effective").unwrap_or_default();
        return format!(
            "manifest ttl_seconds={ttl} is kept, but an operator policy ({in_force}) wins until `lastdb admin retention clear`{anchor}"
        );
    }
    if !status.is_empty() {
        return format!("{status} ttl_seconds={ttl} source=manifest{change}{anchor}");
    }
    if retention.get("blocked").is_some_and(|b| !b.is_null()) {
        return format!("would be REFUSED: ttl_seconds={ttl} source=manifest{change}");
    }
    if retention.get("would_apply").and_then(Value::as_bool) == Some(false) {
        let in_force = policy_in_force(retention, "current").unwrap_or_default();
        return format!(
            "would NOT take effect: an operator policy ({in_force}) wins; manifest ttl_seconds={ttl} would be kept beside it"
        );
    }
    let current = policy_in_force(retention, "current")
        .map(|p| format!(" (in force now: {p})"))
        .unwrap_or_default();
    format!("would apply ttl_seconds={ttl} source=manifest{change}{current}")
}

pub(crate) fn print_retention_text(retention: &Value) {
    if retention.get("declared").and_then(Value::as_bool) == Some(false) {
        println!("    retention: not declared in this manifest");
    } else {
        println!("    retention: {}", summary_line(retention));
        if let Some(rule) = retention.get("age_rule").and_then(Value::as_str) {
            println!("    retention: {rule}");
        }
    }
    let blocked = retention
        .get("blocked")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !blocked.is_empty() {
        println!("    retention: REFUSED: {blocked}");
    }
    for key in ["notes", "warnings"] {
        for line in retention
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|line| !blocked.contains(line))
        {
            let label = if key == "warnings" { "WARNING: " } else { "" };
            println!("    retention: {label}{line}");
        }
    }
}

pub(crate) fn collision_error(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Option<String> {
    let mut hazards = Vec::new();
    for (catalog, names) in lastdb_node::app_publish::catalog_collisions(outcomes) {
        let has_retention = names.iter().any(|name| {
            manifest.schemas.iter().any(|s| {
                s.get("name").and_then(Value::as_str) == Some(name.as_str())
                    && s.get("retention").is_some()
            })
        });
        if has_retention {
            hazards.push(format!(
                "[{}] share the catalog schema {catalog}",
                names.join(", ")
            ));
        }
    }
    for outcome in outcomes {
        if outcome
            .retention
            .as_ref()
            .and_then(|r| r.get("status"))
            .and_then(Value::as_str)
            == Some("shares_retained_schema")
        {
            hazards.push(format!(
                "{} is bound to a catalog schema that carries a TTL",
                outcome.schema_name
            ));
        }
    }
    (!hazards.is_empty()).then(|| {
        format!(
            "a retention would delete records it should not: {}. The TTL sweep deletes the records of every schema bound to the catalog schema. Give each schema its own shape, or remove the `retention`",
            hazards.join("; ")
        )
    })
}

pub(crate) fn retention_refusals(
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Option<String> {
    use lastdb_node::app_publish::SchemaCoverage;
    let names: Vec<&str> = outcomes
        .iter()
        .filter(|o| {
            matches!(&o.coverage, SchemaCoverage::Novel { reason }
                if reason.contains("TTL") || reason.contains("retention"))
        })
        .map(|o| o.schema_name.as_str())
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}
