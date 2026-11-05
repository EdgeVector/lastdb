use super::*;

/// Print request-ops offender tables from the live in-memory ring.
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
    if args.by_app {
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
