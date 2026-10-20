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
    pub max_atoms: Option<usize>,
}

fn u64_of(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// The request body of one step. The run settings count on the first step only.
fn step_body(opts: &VerifyRefsOpts, cursor: Option<&str>) -> Value {
    let mut body = json!({});
    let fields = [
        ("max_pages", opts.max_pages.map(Value::from)),
        ("max_ms", opts.max_ms.map(Value::from)),
        ("pause_ms", opts.pause_ms.map(Value::from)),
    ];
    let first_only = [
        ("page_rows", opts.page_rows.map(Value::from)),
        ("scope", opts.scope.clone().map(Value::from)),
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
    let mut cursor = opts.cursor.clone();
    let mut steps = 0;
    loop {
        let body = step_body(opts, cursor.as_deref());
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
        let stop = done || next.is_none() || (opts.steps > 0 && steps >= opts.steps);
        if stop {
            return finish(&report, opts.json, done);
        }
        cursor = next;
    }
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

fn finish(report: &Value, json_only: bool, done: bool) -> Result<(), String> {
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?
        );
    } else {
        print_report(report, done);
        if let Some(cursor) = report.get("next_cursor").and_then(Value::as_str) {
            println!("Not complete. Continue with: lastdb db verify-refs --cursor {cursor}");
        }
    }
    let faults = u64_of(report, "mismatches_total");
    if done && faults > 0 {
        return Err(format!("verify-refs found {faults} fault(s)"));
    }
    Ok(())
}

/// The fault kinds, in the order the report prints them.
const KINDS: [&str; 11] = [
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
            "  totals: {} atoms, complete: {}, touched by writers: {}",
            u64_of(report, "tally_atoms"),
            report
                .get("tally_complete")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            u64_of(report, "touched_atoms")
        );
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
