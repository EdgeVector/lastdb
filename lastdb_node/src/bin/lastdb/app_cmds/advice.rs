//! Schema Service advice, service URL / API key lookup, and manifest retention
//! checks for `lastdb app check` and `lastdb app publish`.

use super::*;
use serde::Deserialize;
use serde_json::{json, Value};

/// Advice is a courtesy, so a slow service costs the developer little.
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

fn finding_line(schema: &str, finding: &Finding) -> String {
    let target = finding
        .field
        .as_deref()
        .map_or_else(|| schema.to_string(), |field| format!("{schema}.{field}"));
    let message = finding.message.replace(['\n', '\r'], " ");
    format!("advice  {target}  {}: {message}", finding.kind)
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
    let status = response.status();
    if !status.is_success() {
        return Err(Unavailable::Schema(format!("HTTP {status}")));
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
                        .filter(|finding| KNOWN_KINDS.contains(&finding.kind.as_str()))
                        .map(|finding| finding_line(label, finding)),
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

/// Print Schema Service advice for `schemas` on stderr, so the stdout of
/// `--json` and of the registry reply stays machine-readable.
pub(crate) fn print_schema_advice(
    schema_service: Result<String, String>,
    app_id: &str,
    schemas: &[Value],
) {
    if schemas.is_empty() {
        return;
    }
    let lines = schema_service
        .and_then(|url| advice_lines(&url, app_id, schemas))
        .unwrap_or_else(|e| vec![format!("advice unavailable: {e}")]);
    for line in lines {
        eprintln!("{line}");
    }
}

pub(crate) fn resolve_schema_service_url(
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<String, String> {
    match (schema_url, env) {
        (Some(url), _) => Ok(url),
        (None, Some("dev") | None) => Ok(lastdb_node::profile::endpoints::schema_service_url_for(
            lastdb_node::profile::endpoints::Environment::Dev,
        )
        .to_string()),
        (None, Some("prod")) => Ok(lastdb_node::profile::endpoints::schema_service_url_for(
            lastdb_node::profile::endpoints::Environment::Prod,
        )
        .to_string()),
        (None, Some(other)) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
    }
}

/// The developer API key from `--api-key` or `$EXEMEM_DEV_API_KEY`.
pub(crate) fn require_dev_api_key(api_key: Option<String>) -> Result<String, String> {
    match api_key.or_else(|| std::env::var("EXEMEM_DEV_API_KEY").ok()) {
        Some(k) if !k.trim().is_empty() => Ok(k),
        _ => Err("no developer API key — pass --api-key or set EXEMEM_DEV_API_KEY".into()),
    }
}

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn seconds(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn policy_in_force(retention: &Value, key: &str) -> Option<String> {
    let policy = retention.get(key).filter(|policy| !policy.is_null())?;
    let ttl = seconds(policy, "ttl_seconds")?;
    let source = text(policy, "source")?;
    Some(format!("{ttl}s source={source}"))
}

fn summary_line(retention: &Value) -> String {
    let ttl = seconds(retention, "ttl_seconds").unwrap_or(0);
    let status = text(retention, "status").unwrap_or("");
    let anchor = text(retention, "anchor")
        .map(|anchor| format!(" anchor={anchor}"))
        .unwrap_or_default();
    let change = text(retention, "change")
        .filter(|change| *change != "new" && *change != "unchanged")
        .map(|change| {
            let previous = seconds(retention, "previous_ttl_seconds")
                .map(|previous| format!(" from {previous}s"))
                .unwrap_or_default();
            format!(" ({change}{previous})")
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
    if retention
        .get("blocked")
        .is_some_and(|blocked| !blocked.is_null())
    {
        return format!("would be REFUSED: ttl_seconds={ttl} source=manifest{change}");
    }
    if retention.get("would_apply").and_then(Value::as_bool) == Some(false) {
        let in_force = policy_in_force(retention, "current").unwrap_or_default();
        return format!(
            "would NOT take effect: an operator policy ({in_force}) wins; manifest ttl_seconds={ttl} would be kept beside it"
        );
    }
    let current = policy_in_force(retention, "current")
        .map(|policy| format!(" (in force now: {policy})"))
        .unwrap_or_default();
    format!("would apply ttl_seconds={ttl} source=manifest{change}{current}")
}

pub(crate) fn print_retention_text(retention: &Value) {
    if retention.get("declared").and_then(Value::as_bool) == Some(false) {
        println!("    retention: not declared in this manifest");
    } else {
        println!("    retention: {}", summary_line(retention));
        if let Some(rule) = text(retention, "age_rule") {
            println!("    retention: {rule}");
        }
    }
    if let Some(blocked) = text(retention, "blocked") {
        println!("    retention: REFUSED: {blocked}");
    }
    let blocked = text(retention, "blocked").unwrap_or("");
    for key in ["notes", "warnings"] {
        let lines = retention
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|line| !blocked.contains(line));
        for line in lines {
            let label = if key == "warnings" { "WARNING: " } else { "" };
            println!("    retention: {label}{line}");
        }
    }
}

fn names_with_retention<'a>(
    manifest: &lastdb_node::app_publish::AppManifest,
    names: &'a [String],
) -> Vec<&'a str> {
    names
        .iter()
        .filter(|name| {
            manifest.schemas.iter().any(|schema| {
                schema.get("name").and_then(Value::as_str) == Some(name.as_str())
                    && schema.get("retention").is_some()
            })
        })
        .map(String::as_str)
        .collect()
}

pub(crate) fn collision_error(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Option<String> {
    let mut hazards = Vec::new();
    for (catalog, names) in lastdb_node::app_publish::catalog_collisions(outcomes) {
        if !names_with_retention(manifest, &names).is_empty() {
            hazards.push(format!(
                "[{}] share the catalog schema {catalog}",
                names.join(", ")
            ));
        }
    }
    for outcome in outcomes {
        let status = outcome
            .retention
            .as_ref()
            .and_then(|retention| text(retention, "status"));
        if status == Some("shares_retained_schema") {
            hazards.push(format!(
                "{} is bound to a catalog schema that carries a TTL",
                outcome.schema_name
            ));
        }
    }
    (!hazards.is_empty()).then(|| {
        format!(
            "a retention would delete records it should not: {}. The TTL sweep deletes the records \
             of every schema bound to the catalog schema. Give each schema its own shape, or \
             remove the `retention`",
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
        .filter(|outcome| {
            matches!(&outcome.coverage, SchemaCoverage::Novel { reason }
                if reason.contains("TTL") || reason.contains("retention"))
        })
        .map(|outcome| outcome.schema_name.as_str())
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}
