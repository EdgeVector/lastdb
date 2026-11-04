use super::*;

/// Whether the daemon has registered `schema_name`.
///
/// `Ok(false)` means the schema is not registered — distinct from a transport
/// failure, because "never written" is a legitimate steady state for a plane
/// whose sink is off by default.
pub(crate) fn schema_registered(socket: &Path, schema_name: &str) -> Result<bool, String> {
    // Only `/` needs escaping here: every telemetry schema name is
    // `namespace/Type` in ASCII.
    let target = format!("/api/schema/{}", schema_name.replace('/', "%2F"));
    let req = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), Duration::from_secs(10))?;
    if response.starts_with("HTTP/1.1 404 ") {
        return Ok(false);
    }
    if !response.starts_with("HTTP/1.1 200 ") {
        return Err(format!(
            "schema read returned {}",
            http_status_and_body(&response)
        ));
    }
    Ok(true)
}

/// Render an HTTP failure as `<status line> — <body>`.
///
/// The node already names exactly what it objected to (`Invalid field: schema
/// '…' has no field(s): …`). Printing only the status line throws that away and
/// costs the reader a hand-replayed socket call to recover what they were
/// already told.
pub(crate) fn http_status_and_body(response: &str) -> String {
    let status = response.lines().next().unwrap_or("<empty response>");
    match response.split_once("\r\n\r\n") {
        Some((_, body)) if !body.trim().is_empty() => {
            format!("{status} — {}", body.trim())
        }
        _ => status.to_string(),
    }
}

/// Read every rollup record in `[since_ms, until_ms]`.
///
/// The query returns each stored record.
pub(crate) fn query_request_ops_rollup(
    socket: &Path,
    since_ms: u64,
    until_ms: u64,
) -> Result<RollupRead, String> {
    if !schema_registered(socket, lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SCHEMA)? {
        return Ok(RollupRead::SchemaAbsent);
    }
    let body = serde_json::json!({
        "schema_name": lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SCHEMA,
        "filter": {
            "HashRangeRange": {
                "hash": lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SERIES,
                "start": format!("{since_ms:020}"),
                "end": format!("{until_ms:020}~")
            }
        },
        "limit": 10000
    });
    let response = post_json(socket, "/api/query", &body)?;
    if !response.starts_with("HTTP/1.1 200 ") {
        return Err(format!(
            "rollup query returned {}",
            http_status_and_body(&response)
        ));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "rollup query response had no body".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid rollup JSON: {e}"))?;
    let rows = value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "rollup query response had no results array".to_string())?;
    // The request asks for 10000; the server caps the page and says so. This
    // merge has no cursor loop, so a capped page yields an aggregate that is
    // smaller than the window and looks exactly like a complete one. Carry the
    // flag out rather than rendering a total that cannot support itself.
    let truncated = value
        .get("has_more")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok(RollupRead::Rows {
        snapshot: merge_request_ops_rollup_rows(rows, since_ms, until_ms)?,
        truncated,
    })
}

/// Merge rollup records into one ranked snapshot. A malformed record is an
/// error that names its field and `sample_id`; nothing is skipped.
pub(crate) fn merge_request_ops_rollup_rows(
    rows: &[serde_json::Value],
    since_ms: u64,
    until_ms: u64,
) -> Result<lastdb_node::request_telemetry::RequestOpsRollupSnapshot, String> {
    let mut aggregates: HashMap<String, lastdb_node::request_telemetry::OpAggregate> =
        HashMap::new();
    for row in rows {
        let (key, new) = ops_rollup_merge::aggregate_from_row(row)?;
        match aggregates.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                ops_rollup_merge::merge_aggregate(e.get_mut(), &new);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(new);
            }
        }
    }
    Ok(ops_rollup_merge::rank_aggregates(
        aggregates,
        rows.len(),
        since_ms,
        until_ms,
    ))
}
