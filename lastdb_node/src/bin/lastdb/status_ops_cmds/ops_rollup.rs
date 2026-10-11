// lint:file-size-ok moved verbatim out of a larger file; further splitting is follow-up work
use super::*;

/// Print request-ops offender tables from live status and optional durable rollups.
pub(crate) fn ops(data_dir: Option<PathBuf>, args: &OpsArgs) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let health = lastdb_node::health_alert::probe_health(&socket);
    if health.is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    // Forensic path: full recent ring + rankings (`?recent=1`). Cheap
    // `/api/status` deliberately omits those tables.
    let snapshot = probe_status_forensics(
        &socket,
        status_client_timeout(args.timeout, admin_scan_client_timeout()),
    )?;
    let schema_labels = load_schema_labels(&socket, args);
    print_ops_tables(&snapshot, &schema_labels, &socket, args);
    if let Some(raw_since) = args.since.as_deref() {
        print_since_rollup(&socket, raw_since, &snapshot, &schema_labels)?;
    }
    println!();
    println!(
        "Tip: clients should send header `X-LastDB-Client: <name>` (e.g. kanban, brain). \
         Unlabeled traffic is attributed to its peer process as `peer:<comm>`, with the \
         exact pid on Slowest recent."
    );
    Ok(())
}

/// Resolve runtime schema `name` values after the forensic snapshot. Failure is non-fatal.
fn load_schema_labels(
    socket: &Path,
    args: &OpsArgs,
) -> lastdb_node::request_telemetry::SchemaLabels {
    // Resolve runtime schema `name` values after the forensic snapshot. This
    // keeps the catalog request from perturbing the ring that we are about to
    // render. Failure is non-fatal: short runtime IDs remain unambiguous.
    if args.by_app && args.since.is_none() {
        lastdb_node::request_telemetry::SchemaLabels::new()
    } else {
        match probe_schema_labels(
            socket,
            status_client_timeout(args.timeout, Duration::from_secs(10)),
        ) {
            Ok(labels) => labels,
            Err(error) => {
                eprintln!("Schema labels unavailable ({error}); showing short runtime IDs.");
                lastdb_node::request_telemetry::SchemaLabels::new()
            }
        }
    }
}

/// Print the build identity, snapshot age, request-ops tables, and purge ledger.
fn print_ops_tables(
    snapshot: &lastdb_node::self_metrics::StatusSnapshot,
    schema_labels: &lastdb_node::request_telemetry::SchemaLabels,
    socket: &Path,
    args: &OpsArgs,
) {
    // Before any number is printed: name the build these numbers came from.
    // Every perf checkpoint in this workspace cites a version, and until this
    // line existed that version came from the CLI — which is the installed
    // binary, not necessarily the running one.
    for line in lastdb_node::self_metrics::build_identity_lines(&snapshot.build) {
        println!("{line}");
    }
    // Likewise: this output is rendered through the CLI's own compiled phase
    // vocabulary, so a newer daemon's phases would be dropped without a trace.
    // Say so first — a warning under the table is read after the wrong
    // conclusion has already formed.
    for line in lastdb_node::self_metrics::phase_vocabulary_skew_lines(&snapshot.build) {
        println!("{line}");
    }
    // Name the snapshot age and the socket before the numbers: a minute-old
    // snapshot read as current made a live workload look like zero traffic.
    let now_secs = fold_db::clock::unix_secs();
    let age_line = sampler_snapshot_age_line(snapshot.sampler.last_sample_at, now_secs, socket);
    if args.by_app {
        // `--by-app` stdout is a key-value contract for parsers; keep it there.
        eprintln!("{age_line}");
    } else {
        println!("{age_line}");
    }
    if args.by_app {
        for line in lastdb_node::self_metrics::request_ops_app_verb_lines(snapshot) {
            println!("{line}");
        }
    } else {
        for line in lastdb_node::request_telemetry::snapshot_lines_with_schema_labels(
            &snapshot.request_ops,
            schema_labels,
        ) {
            println!("{line}");
        }
    }
    // The purge ledger sits next to the phase tables. It shows acquisition
    // count and critical-section cost without requiring source inspection.
    println!();
    let purge_lines = if args.by_app {
        lastdb_node::self_metrics::purge_stats_lines(snapshot)
    } else {
        lastdb_node::self_metrics::purge_stats_table_lines(snapshot, schema_labels)
    };
    for line in purge_lines {
        println!("{line}");
    }
}

/// Print the durable `--since` rollup section, or the reason it is empty.
fn print_since_rollup(
    socket: &Path,
    raw_since: &str,
    snapshot: &lastdb_node::self_metrics::StatusSnapshot,
    schema_labels: &lastdb_node::request_telemetry::SchemaLabels,
) -> Result<(), String> {
    let duration_ms = parse_since_duration_ms(raw_since)?;
    let until_ms = fold_db::clock::unix_millis();
    let since_ms = until_ms.saturating_sub(duration_ms);
    println!();
    match query_request_ops_rollup(socket, since_ms, until_ms) {
        // The plane has never been registered, so it has certainly never
        // been written. That is the "sink was never switched on" state, and
        // the ladder below already words it correctly — reuse it instead of
        // inventing a second phrasing that could drift out of agreement.
        Ok(RollupRead::SchemaAbsent) => {
            println!(
                "Request ops rollup: rows=0 since={since_ms} until={until_ms} \
                     (schema {} is not registered on this daemon)",
                lastdb_node::self_metrics::REQUEST_OPS_ROLLUP_SCHEMA
            );
            for line in rollup_empty_reason(&snapshot.sampler) {
                println!("{line}");
            }
        }
        Ok(RollupRead::Rows {
            snapshot: rollup,
            omitted,
            truncated,
        }) => {
            for line in lastdb_node::request_telemetry::rollup_lines_with_schema_labels(
                &rollup,
                schema_labels,
            ) {
                println!("{line}");
            }
            // Say what the numbers above cannot account for. A narrowed
            // projection still produces a well-formed table, so without
            // this line a partial aggregate is indistinguishable from a
            // complete one — the same unearned-confidence failure the
            // empty-window ladder below exists to prevent.
            if !omitted.is_empty() {
                let width = rollup_projection().len();
                println!(
                    "  ^ PARTIAL: the registered schema declares {} of the {} columns this \
                         build reads.",
                    width.saturating_sub(omitted.len()),
                    width
                );
                println!(
                    "    Not projected, so reported as zero: {}",
                    omitted.join(", ")
                );
                println!(
                    "    The rollup schema is upgraded only by the durable writer, so a store \
                         whose sink has been off lags the binary. Run with \
                         LASTDB_SELF_METRICS_TO_DB=1 to let the daemon add them."
                );
            }
            if truncated {
                println!(
                    "  ^ TRUNCATED: the server capped the page and reported more rows in \
                         range. The totals above are a lower bound, not the window."
                );
            }
            // An empty window and a broken writer look identical from the
            // read side. The primary spent 12 days reporting "no durable
            // rollups in that window yet" while the sampler's retention
            // read was failing on a dangling atom every tick, so say which
            // one this is rather than leaving the reader to assume idle.
            //
            // There are three ways to get zero rows, not two. The third —
            // the durable sink is simply switched off — is the DEFAULT
            // (`LASTDB_SELF_METRICS_TO_DB` unset), and it was reported as
            // "this window really had no rollups" on a primary that had
            // just served ~45k requests in the window. Silence about a
            // disabled sink reads as evidence of an idle node, which is
            // the opposite of the truth.
            if rollup.is_empty() {
                for line in rollup_empty_reason(&snapshot.sampler) {
                    println!("{line}");
                }
            }
        }
        Err(e) => println!("Request ops rollup: unavailable ({e})"),
    }
    Ok(())
}

/// Explain an empty `--since` window in terms of the sampler's actual state.
///
/// Zero durable rollup rows has four causes, and only one of them means the
/// node was quiet:
///
/// 1. the sink is on but **failing** — writes are attempted and lost;
/// 2. the durable sink is **off** (the default) — nothing was ever written;
/// 3. the daemon did not report the sink at all (predates the field) —
///    unknown, and it must say so rather than guess;
/// 4. the sink is on and healthy — the window really was idle.
///
/// Ordered so the strongest claim ("really had no rollups") is made only when
/// every weaker explanation has been ruled out.
pub(crate) fn rollup_empty_reason(
    sampler: &lastdb_node::self_metrics::SamplerStatus,
) -> Vec<String> {
    if let Some(err) = sampler.last_error.as_deref() {
        return vec![
            "  ^ NOT an idle window: durable telemetry writes are FAILING.".to_string(),
            format!("    sampler last_error={err}"),
        ];
    }
    match sampler.db_write_enabled {
        Some(false) => vec![
            "  ^ NOT an idle window: durable telemetry persistence is OFF.".to_string(),
            "    LASTDB_SELF_METRICS_TO_DB is unset on the daemon, so request-ops".to_string(),
            "    rollups are never written and `--since` has nothing to read. Live".to_string(),
            "    counters above are in-memory only and reset on restart.".to_string(),
        ],
        // The daemon predates the field. "It did not tell us" is not evidence
        // of an idle window, so do not claim one — and do not claim the sink
        // is off either, which would be just as unearned.
        None => vec![
            "  ^ UNKNOWN: this daemon does not report whether durable telemetry".to_string(),
            "    persistence is enabled, so an empty window cannot be told apart".to_string(),
            "    from a sink that was never switched on. Upgrade the daemon, or".to_string(),
            "    check LASTDB_SELF_METRICS_TO_DB on it directly.".to_string(),
        ],
        Some(true) => {
            vec!["    (sampler reports healthy — this window really had no rollups)".to_string()]
        }
    }
}

/// What a `--since` rollup read found, including how much of it the registered
/// schema was able to answer.
///
/// The read has three outcomes, and collapsing them loses the one the operator
/// needs. `SchemaAbsent` is not an error and not an idle window — it is the
/// signature of a daemon whose durable sink has never run, which the empty-window
/// ladder already knows how to explain.
pub(crate) enum RollupRead {
    /// `lastdb_telemetry/RequestOpsRollup` is not registered on this daemon.
    SchemaAbsent,
    Rows {
        snapshot: lastdb_node::request_telemetry::RequestOpsRollupSnapshot,
        /// Fields this build wants that the registered schema does not declare.
        /// Projected away so the query can run at all — and reported, because
        /// the resulting aggregate is narrower than the one the code describes.
        omitted: Vec<String>,
        /// The server capped the page and said there was more.
        truncated: bool,
    },
}

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
/// never reaches the merge and silently reads as 0 (which is exactly how
/// `sum_cold_shard_loads` was dropped before it was listed here). Phase fields
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
        "sum_cold_shard_loads",
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

pub(crate) fn field_str(fields: &serde_json::Value, name: &str) -> Option<String> {
    fields.get(name).and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => other.as_str().map(str::to_string),
    })
}

pub(crate) fn field_u64(fields: &serde_json::Value, name: &str) -> Option<u64> {
    fields.get(name).and_then(serde_json::Value::as_u64)
}

pub(crate) fn op_kind_from_str(raw: &str) -> Option<lastdb_node::request_telemetry::OpKind> {
    match raw {
        "query" => Some(lastdb_node::request_telemetry::OpKind::Query),
        "query_batch" => Some(lastdb_node::request_telemetry::OpKind::QueryBatch),
        "mutation" => Some(lastdb_node::request_telemetry::OpKind::Mutation),
        "mutation_batch" => Some(lastdb_node::request_telemetry::OpKind::MutationBatch),
        "status" => Some(lastdb_node::request_telemetry::OpKind::Status),
        "schema" => Some(lastdb_node::request_telemetry::OpKind::Schema),
        "search" => Some(lastdb_node::request_telemetry::OpKind::Search),
        "history" => Some(lastdb_node::request_telemetry::OpKind::History),
        "atom" => Some(lastdb_node::request_telemetry::OpKind::Atom),
        "deliver" => Some(lastdb_node::request_telemetry::OpKind::Deliver),
        "local_watch" => Some(lastdb_node::request_telemetry::OpKind::LocalWatch),
        "file_blob" => Some(lastdb_node::request_telemetry::OpKind::FileBlob),
        "other" => Some(lastdb_node::request_telemetry::OpKind::Other),
        _ => None,
    }
}

pub(crate) fn parse_since_duration_ms(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("since must not be empty".into());
    }
    let (digits, unit) = trimmed.split_at(trimmed.len().saturating_sub(1));
    let value = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid since duration '{raw}'"))?;
    let seconds = match unit {
        "s" => value,
        "m" => value.saturating_mul(60),
        "h" => value.saturating_mul(60 * 60),
        "d" => value.saturating_mul(24 * 60 * 60),
        _ => return Err(format!("invalid since duration unit in '{raw}'")),
    };
    seconds
        .checked_mul(1000)
        .ok_or_else(|| format!("since duration '{raw}' is too large"))
}
