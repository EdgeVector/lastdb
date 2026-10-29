use super::*;

/// Read the field list the daemon has actually registered for `schema_name`.
///
/// `Ok(None)` means the schema is not registered — distinct from a transport
/// failure, because "never written" is a legitimate steady state for a plane
/// whose sink is off by default.
pub(crate) fn fetch_schema_fields(
    socket: &Path,
    schema_name: &str,
) -> Result<Option<Vec<String>>, String> {
    // Only `/` needs escaping here: every telemetry schema name is
    // `namespace/Type` in ASCII.
    let target = format!("/api/schema/{}", schema_name.replace('/', "%2F"));
    let req = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), Duration::from_secs(10))?;
    if response.starts_with("HTTP/1.1 404 ") {
        return Ok(None);
    }
    if !response.starts_with("HTTP/1.1 200 ") {
        return Err(format!(
            "schema read returned {}",
            http_status_and_body(&response)
        ));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "schema response had no body".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid schema JSON: {e}"))?;
    let Some(fields) = value
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(serde_json::Value::as_array)
    else {
        // Registered, but this daemon does not report its field list. Treat as
        // "cannot narrow" rather than "no fields", which would project nothing.
        return Ok(Some(Vec::new()));
    };
    Ok(Some(
        fields
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(ToString::to_string)
            .collect(),
    ))
}

/// Split `wanted` into (projected, omitted) against the schema's declared list.
///
/// An EMPTY `declared` means the daemon did not report a field list, not that it
/// declares nothing. Narrowing to the intersection there would project zero
/// columns and read the whole window as empty — a far worse answer than the 400
/// this function exists to avoid — so the projection is passed through untouched
/// and the query is allowed to fail loudly instead.
pub(crate) fn narrow_projection(
    wanted: Vec<String>,
    declared: &[String],
) -> (Vec<String>, Vec<String>) {
    if declared.is_empty() {
        return (wanted, Vec::new());
    }
    wanted
        .into_iter()
        .partition(|field| declared.iter().any(|d| d == field))
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

/// Every rollup column this build knows how to read.
///
/// `/api/query` `fields` is a real projection: a field absent from this list
/// never reaches the merge and silently reads as 0. Phase fields
/// derive from the model's PHASE_NAMES so a new phase cannot be persisted by
/// the sampler yet projected away by the CLI.
///
/// Single source of truth so the "N of M columns" degradation line cannot
/// disagree with the projection it is describing.
pub(crate) fn rollup_projection() -> Vec<String> {
    let mut projection: Vec<String> = [
        "sampled_at_ms",
        "client",
        "kind",
        "schema",
        "count",
        "sum_ms",
        "max_ms",
        "last_ts_ms",
        "error_count",
        "sum_body_bytes",
        "max_body_bytes",
        "phase_count",
        "phased_sum_ms",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    for name in lastdb_node::request_telemetry::PhaseTimings::PHASE_NAMES {
        projection.push(format!("sum_{name}_us"));
    }
    projection
}

pub(crate) fn query_request_ops_rollup(
    socket: &Path,
    since_ms: u64,
    until_ms: u64,
) -> Result<RollupRead, String> {
    let mut projection = rollup_projection();

    // Narrow the projection to what the daemon has actually registered.
    //
    // A field this build knows about but the registered schema does not declare
    // is a hard `400 Invalid field` for the WHOLE query — correct behaviour
    // (`lastdb-wide-projection-is-a-filter-not-a-superset-read`), and fatal
    // here, because the schema is upgraded only inside the write path and that
    // path is off by default. A newer CLI against a store whose sink has been
    // off therefore projects fields that will never exist, and `--since` dies
    // permanently rather than degrading.
    //
    // Narrowing also makes the merge's own back-compat tolerance reachable.
    // `merge_request_ops_rollup_rows` reads absent columns as 0 for rows
    // predating a field — but a row missing a *projected* field is dropped
    // whole, so that tolerance could never fire while the field was projected.
    // Ask only for declared columns and the old rows come back.
    let declared =
        fetch_schema_fields(socket, lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SCHEMA)?;
    let Some(declared) = declared else {
        return Ok(RollupRead::SchemaAbsent);
    };
    let omitted;
    (projection, omitted) = narrow_projection(projection, &declared);

    let body = serde_json::json!({
        "schema_name": lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SCHEMA,
        "fields": projection,
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
        snapshot: merge_request_ops_rollup_rows(rows, since_ms, until_ms),
        omitted,
        truncated,
    })
}

pub(crate) fn merge_request_ops_rollup_rows(
    rows: &[serde_json::Value],
    since_ms: u64,
    until_ms: u64,
) -> lastdb_node::request_telemetry::RequestOpsRollupSnapshot {
    let mut aggregates: HashMap<String, lastdb_node::request_telemetry::OpAggregate> =
        HashMap::new();
    for row in rows {
        let Some((key, new)) = ops_rollup_merge::aggregate_from_row(row) else {
            continue;
        };
        match aggregates.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                ops_rollup_merge::merge_aggregate(e.get_mut(), &new);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(new);
            }
        }
    }
    ops_rollup_merge::rank_aggregates(aggregates, rows.len(), since_ms, until_ms)
}
