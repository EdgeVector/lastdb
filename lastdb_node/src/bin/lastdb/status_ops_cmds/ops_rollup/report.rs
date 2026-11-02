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
        "Note: clients should send header `X-LastDB-Client: <name>` (e.g. kanban, brain). \
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

/// Print the build identity, snapshot age, and request-ops tables.
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
            // read was failing on a dangling record every tick, so say which
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
/// Zero durable rollup rows has three causes, and only one of them means the
/// node was quiet:
///
/// 1. the sink is on but **failing** — writes are attempted and lost;
/// 2. the durable sink is **off** (the default) — nothing was ever written;
/// 3. the sink is on and healthy — the window really was idle.
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
    if sampler.db_write_enabled {
        vec!["    (sampler reports healthy — this window really had no rollups)".to_string()]
    } else {
        vec![
            "  ^ NOT an idle window: durable telemetry persistence is OFF.".to_string(),
            "    LASTDB_SELF_METRICS_TO_DB is unset on the daemon, so request-ops".to_string(),
            "    rollups are never written and `--since` has nothing to read. Live".to_string(),
            "    counters above are in-memory only and reset on restart.".to_string(),
        ]
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
