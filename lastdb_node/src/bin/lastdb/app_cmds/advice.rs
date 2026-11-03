//! Schema Service advice on manifest schemas (`design-lastdb-advisory-schema-service`,
//! section 9). `lastdb app check` and `lastdb app publish` print it. Advice
//! never stops either command: a timeout, a network error, a non-2xx reply,
//! or a reply that does not parse prints `advice unavailable: <reason>`.

use super::*;
use serde::Deserialize;
use serde_json::{json, Value};

/// Advice is a courtesy, so a slow service costs the developer little.
const ADVICE_TIMEOUT: Duration = Duration::from_secs(5);

/// The finding kinds this build prints. The route can add kinds; a kind not
/// listed here is skipped.
const KNOWN_KINDS: [&str; 7] = [
    "canonical_match",
    "same_name_different_meaning",
    "near_match",
    "abbreviation",
    "missing_type",
    "suggested_classification",
    "similar_schema",
];

/// `POST /v1/advice/schema` reply. Fields this build does not read, such as
/// `evidence`, are ignored so the route can grow.
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

/// Why one schema got no advice.
enum Unavailable {
    /// The service did not answer. The remaining schemas are skipped.
    Unreachable(String),
    /// The service answered this schema with no usable reply.
    Schema(String),
}

/// `advice  <schema>.<field>  <kind>: <message>`, or `<schema>` alone for a
/// finding about the whole schema.
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

/// Advice lines for each manifest schema, in manifest order, from the Schema
/// Service at `schema_service`. Each schema is sent as written in the
/// manifest, one request after another: there are few of them, and the
/// route is rate-limited per caller.
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
/// `--json` and of the registry reply stays machine-readable. `schema_service`
/// is the resolved base URL, or why it could not be resolved.
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
