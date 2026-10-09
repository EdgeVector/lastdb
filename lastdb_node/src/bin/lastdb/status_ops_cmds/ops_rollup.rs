// lint:file-size-ok moved verbatim out of a larger file; further splitting is follow-up work
use super::*;

/// Print request-ops offender tables from live status and optional durable rollups.
pub(crate) fn ops(data_dir: Option<PathBuf>, args: &OpsArgs) -> Result<(), String> {
    // lint:fn-size-ok moved verbatim from status_ops_cmds.rs; splitting it is a separate change
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
    // Resolve runtime schema `name` values after the forensic snapshot. This
    // keeps the catalog request from perturbing the ring that we are about to
    // render. Failure is non-fatal: short runtime IDs remain unambiguous.
    let schema_labels = if args.by_app && args.since.is_none() {
        lastdb_node::request_telemetry::SchemaLabels::new()
    } else {
        match probe_schema_labels(
            &socket,
            status_client_timeout(args.timeout, Duration::from_secs(10)),
        ) {
            Ok(labels) => labels,
            Err(error) => {
                eprintln!("Schema labels unavailable ({error}); showing short runtime IDs.");
                lastdb_node::request_telemetry::SchemaLabels::new()
            }
        }
    };
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
    let age_line = sampler_snapshot_age_line(snapshot.sampler.last_sample_at, now_secs, &socket);
    if args.by_app {
        // `--by-app` stdout is a key-value contract for parsers; keep it there.
        eprintln!("{age_line}");
    } else {
        println!("{age_line}");
    }
    if args.by_app {
        for line in lastdb_node::self_metrics::request_ops_app_verb_lines(&snapshot) {
            println!("{line}");
        }
    } else {
        for line in lastdb_node::request_telemetry::snapshot_lines_with_schema_labels(
            &snapshot.request_ops,
            &schema_labels,
        ) {
            println!("{line}");
        }
    }
    // The purge ledger sits next to the phase tables. It shows acquisition
    // count and critical-section cost without requiring source inspection.
    println!();
    let purge_lines = if args.by_app {
        lastdb_node::self_metrics::purge_stats_lines(&snapshot)
    } else {
        lastdb_node::self_metrics::purge_stats_table_lines(&snapshot, &schema_labels)
    };
    for line in purge_lines {
        println!("{line}");
    }
    if let Some(raw_since) = args.since.as_deref() {
        let duration_ms = parse_since_duration_ms(raw_since)?;
        let until_ms = fold_db::clock::unix_millis();
        let since_ms = until_ms.saturating_sub(duration_ms);
        println!();
        match query_request_ops_rollup(&socket, since_ms, until_ms) {
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
                    &schema_labels,
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
    }
    println!();
    println!(
        "Tip: clients should send header `X-LastDB-Client: <name>` (e.g. kanban, brain). \
         Unlabeled traffic is attributed to its peer process as `peer:<comm>`, with the \
         exact pid on Slowest recent."
    );
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
    // lint:fn-size-ok moved verbatim from status_ops_cmds.rs; splitting it is a separate change
    let mut aggregates: HashMap<String, lastdb_node::request_telemetry::OpAggregate> =
        HashMap::new();
    for row in rows {
        let Some(fields) = row.get("fields") else {
            continue;
        };
        let Some(client) = field_str(fields, "client") else {
            continue;
        };
        let Some(kind) = field_str(fields, "kind")
            .as_deref()
            .and_then(op_kind_from_str)
        else {
            continue;
        };
        let schema = field_str(fields, "schema").filter(|s| !s.is_empty());
        // Absent on rows written before route identity shipped, and on every
        // row whose schema already identifies it. Absent reads as "no route
        // attribution", so those rows merge and render exactly as before.
        let route = field_str(fields, "route").filter(|r| !r.is_empty());
        // Must agree with `OpAggregate::key` and `request_ops_rollup_key` on
        // row identity: a merge that collapses two routes the writer kept
        // apart hands the operator back the single unidentifiable row this
        // field exists to split.
        let key = format!(
            "{}\0{}\0{}\0{}",
            client,
            kind.as_str(),
            schema.as_deref().unwrap_or(""),
            route.as_deref().unwrap_or("")
        );
        let count = field_u64(fields, "count").unwrap_or(0);
        let sum_ms = field_u64(fields, "sum_ms").unwrap_or(0);
        let max_ms = field_u64(fields, "max_ms").unwrap_or(0);
        let last_ts_ms = field_u64(fields, "last_ts_ms").unwrap_or(0);
        let error_count = field_u64(fields, "error_count").unwrap_or(0);
        // Absent on rollup rows written before read-cost attribution shipped.
        let sum_cold_shard_loads = field_u64(fields, "sum_cold_shard_loads").unwrap_or(0);
        // Absent on rows written before write-volume attribution shipped.
        let sum_body_bytes = field_u64(fields, "sum_body_bytes").unwrap_or(0);
        let max_body_bytes = field_u64(fields, "max_body_bytes").unwrap_or(0);
        // Rows written before phase persistence shipped lack these fields
        // entirely; `unwrap_or(0)` reads them as an absent phase set — the
        // "no phases reported" state, not a measurement of zero. The struct
        // literal is deliberate: adding another phase to the model breaks
        // this build until the merge learns to read it.
        let phase_sums = lastdb_node::request_telemetry::PhaseTimings {
            queue_wait_us: field_u64(fields, "sum_queue_wait_us").unwrap_or(0),
            admission_wait_us: field_u64(fields, "sum_admission_wait_us").unwrap_or(0),
            parse_us: field_u64(fields, "sum_parse_us").unwrap_or(0),
            schema_resolve_us: field_u64(fields, "sum_schema_resolve_us").unwrap_or(0),
            validate_us: field_u64(fields, "sum_validate_us").unwrap_or(0),
            lock_wait_us: field_u64(fields, "sum_lock_wait_us").unwrap_or(0),
            purge_barrier_us: field_u64(fields, "sum_purge_barrier_us").unwrap_or(0),
            purge_plan_us: field_u64(fields, "sum_purge_plan_us").unwrap_or(0),
            purge_commit_us: field_u64(fields, "sum_purge_commit_us").unwrap_or(0),
            purge_materialize_us: field_u64(fields, "sum_purge_materialize_us").unwrap_or(0),
            purge_trace_us: field_u64(fields, "sum_purge_trace_us").unwrap_or(0),
            purge_retention_guard_us: field_u64(fields, "sum_purge_retention_guard_us")
                .unwrap_or(0),
            purge_delete_us: field_u64(fields, "sum_purge_delete_us").unwrap_or(0),
            purge_finalize_us: field_u64(fields, "sum_purge_finalize_us").unwrap_or(0),
            molecule_gate_us: field_u64(fields, "sum_molecule_gate_us").unwrap_or(0),
            cas_precondition_us: field_u64(fields, "sum_cas_precondition_us").unwrap_or(0),
            count_us: field_u64(fields, "sum_count_us").unwrap_or(0),
            hydrate_us: field_u64(fields, "sum_hydrate_us").unwrap_or(0),
            hydrate_atoms_us: field_u64(fields, "sum_hydrate_atoms_us").unwrap_or(0),
            hydrate_format_us: field_u64(fields, "sum_hydrate_format_us").unwrap_or(0),
            hydrate_sort_us: field_u64(fields, "sum_hydrate_sort_us").unwrap_or(0),
            hydrate_filter_us: field_u64(fields, "sum_hydrate_filter_us").unwrap_or(0),
            annotate_us: field_u64(fields, "sum_annotate_us").unwrap_or(0),
            apply_us: field_u64(fields, "sum_apply_us").unwrap_or(0),
            // Rows written before the apply residual was carved carry the
            // whole bucket in `sum_apply_us` and nothing in these three, so
            // `apply` appears to drop at the upgrade boundary rather than
            // the work having moved. Same additive-migration shape as the
            // persist and hydrate splits.
            apply_memory_us: field_u64(fields, "sum_apply_memory_us").unwrap_or(0),
            schema_load_us: field_u64(fields, "sum_schema_load_us").unwrap_or(0),
            dedupe_scan_us: field_u64(fields, "sum_dedupe_scan_us").unwrap_or(0),
            idempotency_check_us: field_u64(fields, "sum_idempotency_check_us").unwrap_or(0),
            grouping_us: field_u64(fields, "sum_grouping_us").unwrap_or(0),
            restore_molecules_us: field_u64(fields, "sum_restore_molecules_us").unwrap_or(0),
            protein_sibling_fold_us: field_u64(fields, "sum_protein_sibling_fold_us").unwrap_or(0),
            spawn_indexing_us: field_u64(fields, "sum_spawn_indexing_us").unwrap_or(0),
            sync_uuids_us: field_u64(fields, "sum_sync_uuids_us").unwrap_or(0),
            schema_reload_us: field_u64(fields, "sum_schema_reload_us").unwrap_or(0),
            persist_us: field_u64(fields, "sum_persist_us").unwrap_or(0),
            // Rows written before the durable-store bucket was split carry
            // the whole bucket in `sum_persist_us` and nothing in these
            // three, so `persist` appears to drop at the upgrade boundary
            // while the sub-steps appear from nothing. That is the split
            // showing up in history, not a regression cured — the same
            // reading the change-feed split needs on `sum_change_record_us`.
            persist_molecules_us: field_u64(fields, "sum_persist_molecules_us").unwrap_or(0),
            persist_schema_us: field_u64(fields, "sum_persist_schema_us").unwrap_or(0),
            persist_idempotency_us: field_u64(fields, "sum_persist_idempotency_us").unwrap_or(0),
            flush_us: field_u64(fields, "sum_flush_us").unwrap_or(0),
            sync_capture_us: field_u64(fields, "sum_sync_capture_us").unwrap_or(0),
            index_wait_us: field_u64(fields, "sum_index_wait_us").unwrap_or(0),
            change_record_us: field_u64(fields, "sum_change_record_us").unwrap_or(0),
            change_record_lock_wait_us: field_u64(fields, "sum_change_record_lock_wait_us")
                .unwrap_or(0),
            change_record_write_us: field_u64(fields, "sum_change_record_write_us").unwrap_or(0),
            response_envelope_us: field_u64(fields, "sum_response_envelope_us").unwrap_or(0),
            status_sync_us: field_u64(fields, "sum_status_sync_us").unwrap_or(0),
            status_backup_us: field_u64(fields, "sum_status_backup_us").unwrap_or(0),
            status_durability_us: field_u64(fields, "sum_status_durability_us").unwrap_or(0),
            status_data_dir_us: field_u64(fields, "sum_status_data_dir_us").unwrap_or(0),
            status_request_ops_us: field_u64(fields, "sum_status_request_ops_us").unwrap_or(0),
        };
        let phase_count = field_u64(fields, "phase_count").unwrap_or(0);
        // Absent on rows written before the residual shipped. Reading 0 makes
        // `unphased_us` render nothing for those intervals, which is right —
        // the wall clock for their phased population was never recorded, and
        // a residual derived from `sum_ms` would be wrong, not approximate.
        let phased_sum_ms = field_u64(fields, "phased_sum_ms").unwrap_or(0);
        // Absent on rows written before the molecule work counters shipped.
        // Both read 0 there, which `molecule_work_detail` renders as nothing —
        // the honest answer, since the ratio's denominator was never recorded.
        let sum_molecules_persisted = field_u64(fields, "sum_molecules_persisted").unwrap_or(0);
        let sum_molecule_store_commits =
            field_u64(fields, "sum_molecule_store_commits").unwrap_or(0);
        // Same additive migration: rows written before the resident commit
        // counters shipped read 0 here, which renders no ratio rather than a
        // wrong one.
        let sum_resident_commits = field_u64(fields, "sum_resident_commits").unwrap_or(0);
        let sum_resident_operations = field_u64(fields, "sum_resident_operations").unwrap_or(0);
        aggregates
            .entry(key)
            .and_modify(|agg| {
                agg.count = agg.count.saturating_add(count);
                agg.sum_ms = agg.sum_ms.saturating_add(sum_ms);
                agg.max_ms = agg.max_ms.max(max_ms);
                agg.last_ts_ms = agg.last_ts_ms.max(last_ts_ms);
                agg.error_count = agg.error_count.saturating_add(error_count);
                agg.sum_cold_shard_loads = agg
                    .sum_cold_shard_loads
                    .saturating_add(sum_cold_shard_loads);
                agg.sum_body_bytes = agg.sum_body_bytes.saturating_add(sum_body_bytes);
                agg.max_body_bytes = agg.max_body_bytes.max(max_body_bytes);
                // Interval SUMS merge by addition — same invariant the
                // sampler's delta writer relies on.
                agg.phase_sums.accumulate(phase_sums);
                agg.phase_count = agg.phase_count.saturating_add(phase_count);
                agg.phased_sum_ms = agg.phased_sum_ms.saturating_add(phased_sum_ms);
                agg.sum_molecules_persisted = agg
                    .sum_molecules_persisted
                    .saturating_add(sum_molecules_persisted);
                agg.sum_molecule_store_commits = agg
                    .sum_molecule_store_commits
                    .saturating_add(sum_molecule_store_commits);
                agg.sum_resident_commits = agg
                    .sum_resident_commits
                    .saturating_add(sum_resident_commits);
                agg.sum_resident_operations = agg
                    .sum_resident_operations
                    .saturating_add(sum_resident_operations);
            })
            .or_insert(lastdb_node::request_telemetry::OpAggregate {
                client,
                kind,
                schema,
                route,
                count,
                sum_ms,
                max_ms,
                last_ts_ms,
                error_count,
                sum_cold_shard_loads,
                sum_body_bytes,
                max_body_bytes,
                // Durable rollup rows carry `error_count` without a status
                // breakdown; `error_detail` renders nothing for an empty one,
                // so `--since` output is unchanged.
                error_statuses: std::collections::BTreeMap::new(),
                error_statuses_overflow: 0,
                last_error_status: None,
                last_error_ts_ms: None,
                phase_sums,
                phase_count,
                phased_sum_ms,
                sum_molecules_persisted,
                sum_molecule_store_commits,
                sum_resident_commits,
                sum_resident_operations,
            });
    }

    // Same segregation the live snapshot applies: idle long-poll wait must not
    // outrank real work in a "top by total time" table an operator reads as
    // "biggest consumer".
    let (mut idle_wait, mut top_by_total_ms): (Vec<_>, Vec<_>) = aggregates
        .values()
        .cloned()
        .partition(|a| a.kind.is_idle_wait());
    top_by_total_ms.sort_by(|a, b| {
        b.sum_ms
            .cmp(&a.sum_ms)
            .then_with(|| b.max_ms.cmp(&a.max_ms))
            .then_with(|| b.count.cmp(&a.count))
    });
    top_by_total_ms.truncate(32);
    idle_wait.sort_by(|a, b| b.sum_ms.cmp(&a.sum_ms).then_with(|| b.count.cmp(&a.count)));
    idle_wait.truncate(32);

    let mut top_by_body_bytes: Vec<_> = aggregates
        .values()
        .filter(|a| !a.kind.is_idle_wait())
        .cloned()
        .collect();
    top_by_body_bytes.sort_by(|a, b| {
        b.sum_body_bytes
            .cmp(&a.sum_body_bytes)
            .then_with(|| b.max_body_bytes.cmp(&a.max_body_bytes))
            .then_with(|| b.count.cmp(&a.count))
    });
    top_by_body_bytes.truncate(32);
    if top_by_body_bytes
        .first()
        .is_none_or(|a| a.sum_body_bytes == 0)
    {
        top_by_body_bytes.clear();
    }

    let mut top_by_count: Vec<_> = aggregates.into_values().collect();
    top_by_count.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| b.sum_ms.cmp(&a.sum_ms))
            .then_with(|| b.max_ms.cmp(&a.max_ms))
    });
    top_by_count.truncate(32);

    lastdb_node::request_telemetry::RequestOpsRollupSnapshot {
        top_by_total_ms,
        top_by_count,
        top_by_body_bytes,
        idle_wait,
        row_count: rows.len(),
        since_ms,
        until_ms,
    }
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
