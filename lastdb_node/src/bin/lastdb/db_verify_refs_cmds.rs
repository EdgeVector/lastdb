//! `lastdb db verify-refs`: report faults in the atom counters. Read-only.

use super::*;
use serde_json::{json, Value};

/// What the operator asked for.
pub(crate) struct VerifyRefsOpts {
    pub json: bool,
    /// Show the last result of the daemon and run nothing.
    pub last: bool,
    /// Stop after this many steps (0: run to the end).
    pub steps: u32,
    pub cursor: Option<String>,
    pub max_pages: Option<u32>,
    pub max_ms: Option<u64>,
    pub pause_ms: Option<u64>,
    pub page_rows: Option<usize>,
    pub scope: Option<String>,
    /// Check the personal scope and each scope that the database catalog names.
    pub all_scopes: bool,
    pub max_atoms: Option<usize>,
}

fn u64_of(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// The request body of one step. The run settings count on the first step only.
fn step_body(opts: &VerifyRefsOpts, scope: Option<&str>, cursor: Option<&str>) -> Value {
    let mut body = json!({});
    let fields = [
        ("max_pages", opts.max_pages.map(Value::from)),
        ("max_ms", opts.max_ms.map(Value::from)),
        ("pause_ms", opts.pause_ms.map(Value::from)),
    ];
    let first_only = [
        ("page_rows", opts.page_rows.map(Value::from)),
        ("scope", scope.map(Value::from)),
        ("max_atoms", opts.max_atoms.map(Value::from)),
    ];
    let kept = fields
        .into_iter()
        .chain(first_only.into_iter().filter(|_| cursor.is_none()));
    for (name, value) in kept {
        if let Some(value) = value {
            body[name] = value;
        }
    }
    if let Some(cursor) = cursor {
        body["cursor"] = Value::from(cursor);
    }
    body
}

pub(crate) fn db_verify_refs(socket: &Path, opts: &VerifyRefsOpts) -> Result<(), String> {
    if opts.last {
        return show_last(socket, opts.json);
    }
    if opts.all_scopes {
        if opts.cursor.is_some() || opts.scope.is_some() || opts.steps > 0 {
            return Err("--all-scopes cannot be combined with --cursor, --scope or --steps".into());
        }
        return verify_all_scopes(socket, opts);
    }
    let report = run_scope(socket, opts, opts.scope.as_deref(), opts.cursor.clone())?;
    show_report(&report, opts.json)?;
    verdict(&report).map_or(Ok(()), Err)
}

/// Follow the cursor of one scope until the run is complete, or until the
/// step limit. Returns the last report.
fn run_scope(
    socket: &Path,
    opts: &VerifyRefsOpts,
    scope: Option<&str>,
    mut cursor: Option<String>,
) -> Result<Value, String> {
    let mut steps = 0;
    loop {
        let body = step_body(opts, scope, cursor.as_deref());
        let value = db_post_json(socket, "/api/db/verify-refs", &body)?;
        let report = value
            .get("verify_refs")
            .cloned()
            .ok_or_else(|| "response missing verify_refs".to_string())?;
        steps += 1;
        let done = report.get("done").and_then(Value::as_bool).unwrap_or(false);
        let next = report
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        if !opts.json && !done {
            eprintln!(
                "verify-refs: phase {} pages {} faults {}",
                report.get("phase").and_then(Value::as_str).unwrap_or("?"),
                u64_of(&report, "pages"),
                u64_of(&report, "mismatches_total")
            );
        }
        if done || next.is_none() || (opts.steps > 0 && steps >= opts.steps) {
            return Ok(report);
        }
        cursor = next;
    }
}

/// Check the personal scope and each scope that the catalog names.
fn verify_all_scopes(socket: &Path, opts: &VerifyRefsOpts) -> Result<(), String> {
    let req = format!(
        "GET /api/db/verify-refs/scopes HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "verify-refs scopes")?;
    let named: Vec<String> = value
        .get("scopes")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let mut scopes: Vec<Option<String>> = vec![None];
    scopes.extend(named.into_iter().map(Some));
    let mut reports = Vec::new();
    let mut problems = Vec::new();
    for scope in &scopes {
        let report = run_scope(socket, opts, scope.as_deref(), None)?;
        warn_empty_scope(&report);
        problems.extend(verdict(&report));
        reports.push(report);
    }
    if opts.json {
        let text = serde_json::to_string_pretty(&reports).map_err(|e| format!("serialize: {e}"))?;
        println!("{text}");
    } else {
        for report in &reports {
            print_report(report, true);
        }
        println!(
            "Checked {} scope(s): the personal scope and {} that the database catalog names. A scope with data and no catalog entry needs --scope.",
            scopes.len(),
            scopes.len() - 1
        );
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}

/// Print one report, with the loud line for a scope that held no tip.
fn show_report(report: &Value, json_only: bool) -> Result<(), String> {
    let done = report.get("done").and_then(Value::as_bool).unwrap_or(false);
    warn_empty_scope(report);
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_report(report, done);
    if let Some(cursor) = report.get("next_cursor").and_then(Value::as_str) {
        println!("Not complete. Continue with: lastdb db verify-refs --cursor {cursor}");
    }
    Ok(())
}

/// A run that read no tip in its scope checked nothing. A wrong `--scope` gives
/// this result, so say it on stderr whatever the output mode.
fn warn_empty_scope(report: &Value) {
    let done = report.get("done").and_then(Value::as_bool).unwrap_or(false);
    if done && u64_of(report, "tips_read") == 0 {
        eprintln!(
            "WARNING: 0 tips read in scope {}: check the scope. A scope other than the personal one needs --scope PREFIX or --all-scopes.",
            report.get("scope").and_then(Value::as_str).unwrap_or("personal")
        );
    }
}

/// The reason to exit non-zero, when a complete run found a fault or could not
/// compare the counters with the totals.
fn verdict(report: &Value) -> Option<String> {
    if !report.get("done").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let faults = u64_of(report, "mismatches_total");
    if faults > 0 {
        return Some(format!("verify-refs found {faults} fault(s)"));
    }
    if !report
        .get("tally_complete")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Some(format!(
            "verify-refs is incomplete: more than {} atoms are referenced, so a counter above its holders was not checked. Run again with a larger --max-atoms",
            u64_of(report, "tally_atoms")
        ));
    }
    None
}

fn show_last(socket: &Path, json_only: bool) -> Result<(), String> {
    let req = format!(
        "GET /api/db/verify-refs HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "verify-refs")?;
    let summary = value
        .get("verify_refs")
        .cloned()
        .ok_or_else(|| "response missing verify_refs".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    match summary.get("last").filter(|last| !last.is_null()) {
        Some(last) => print_report(last, true),
        None => println!("verify-refs: no run finished since the daemon started"),
    }
    if let Some(running) = summary.get("running").filter(|r| !r.is_null()) {
        println!(
            "verify-refs: a run is in progress (phase {}, {} pages)",
            running.get("phase").and_then(Value::as_str).unwrap_or("?"),
            u64_of(running, "pages")
        );
    }
    Ok(())
}

/// The fault kinds, in the order the report prints them.
const KINDS: [&str; 34] = [
    "counter_below_tips",
    "counter_above_tips",
    "body_without_counter",
    "counter_without_body",
    "zero_counter",
    "suspect_counter",
    "tip_without_body",
    "record_without_body",
    "unreadable_tip",
    "unreadable_record",
    "unreadable_counter",
    "unsealed_rows",
    "blob_counter_missing",
    "blob_counter_below_refs",
    "blob_zero_without_dwell",
    "blob_dwell_with_refs",
    "blob_unqueued",
    "blob_stale_queue_entry",
    "blob_suspect",
    "unreadable_blob_counter",
    "blob_counter_without_blob",
    "molecule_slots_above_tips",
    "molecule_slots_below_tips",
    "molecule_records_above_records",
    "molecule_records_below_records",
    "tips_without_header",
    "molecule_counters_unset",
    "suspect_molecule",
    "unreadable_header",
    "bindings_counter_mismatch",
    "binding_record_without_counter",
    "zero_bindings_counter",
    "suspect_bindings",
    "unreadable_bindings",
];

fn print_report(report: &Value, complete: bool) {
    let scope = report.get("scope").and_then(Value::as_str);
    println!(
        "Verify refs ({}, scope {}): {}",
        report.get("run_id").and_then(Value::as_str).unwrap_or("?"),
        scope.unwrap_or("personal"),
        if complete { "complete" } else { "in progress" }
    );
    println!(
        "  pages {}  steps {}  time {} ms  page retries {}",
        u64_of(report, "pages"),
        u64_of(report, "steps"),
        u64_of(report, "elapsed_ms"),
        u64_of(report, "page_retries")
    );
    if report.get("tips_read").is_some() {
        println!(
            "  read: {} tips, {} records, {} counters, {} bodies",
            u64_of(report, "tips_read"),
            u64_of(report, "records_read"),
            u64_of(report, "counters_read"),
            u64_of(report, "bodies_read")
        );
        println!(
            "  molecules: {} headers, {} bindings counters, {} binding records",
            u64_of(report, "headers_read"),
            u64_of(report, "binding_counters_read"),
            u64_of(report, "binding_records_read")
        );
        println!(
            "  blobs: {} counters, {} queue entries",
            u64_of(report, "blob_counters_read"),
            u64_of(report, "blob_queue_read")
        );
        println!(
            "  totals: {} atoms, complete: {}, touched by writers: {}",
            u64_of(report, "tally_atoms"),
            report
                .get("tally_complete")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            u64_of(report, "touched_atoms")
        );
    }
    if !report
        .get("tally_complete")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        println!("  INCOMPLETE: counters were not compared with the totals (more atoms than --max-atoms)");
    }
    println!(
        "  faults: {}   suspect atoms: {}",
        u64_of(report, "mismatches_total"),
        u64_of(report, "suspect_atoms")
    );
    if let Some(kinds) = report.get("mismatches") {
        for kind in KINDS {
            let count = u64_of(kinds, kind);
            if count > 0 {
                println!("    {kind}: {count}");
            }
        }
        let candidates = u64_of(kinds, "unconfirmed_count_mismatch");
        if candidates > 0 {
            println!(
                "  candidates (a writer changed the atom during the pass): {candidates}; run again when writers are quiet"
            );
        }
    }
    print_samples(report);
}

fn print_samples(report: &Value) {
    let Some(samples) = report.get("samples").and_then(Value::as_array) else {
        return;
    };
    for sample in samples.iter().take(20) {
        let text = |key: &str| sample.get(key).and_then(Value::as_str).unwrap_or("?");
        println!(
            "  e.g. {} {} — {}",
            text("kind"),
            text("atom"),
            text("detail")
        );
    }
    if let Some(notes) = report.get("notes").and_then(Value::as_array) {
        for note in notes.iter().filter_map(Value::as_str) {
            println!("  note: {note}");
        }
    }
}
