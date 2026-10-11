use super::*;

use serde_json::Value;

fn count_of(report: &Value, key: &str) -> u64 {
    report.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn flag_of(report: &Value, key: &str) -> bool {
    report.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn validate_rekey_opts(opts: &RekeyCliOpts) -> Result<(), String> {
    if opts.progress && opts.until_complete {
        return Err("--progress and --until-complete are mutually exclusive".to_string());
    }
    if opts.until_complete && !opts.execute {
        return Err(
            "--until-complete requires --execute (a dry run completes nothing)".to_string(),
        );
    }
    if opts.compact_after && !(opts.execute && opts.remove_flat && opts.until_complete) {
        return Err(
            "--compact-after requires --execute --remove-flat --until-complete".to_string(),
        );
    }
    if opts.compact_after && opts.json_only {
        return Err("--compact-after cannot be combined with --json".to_string());
    }
    if let Some(n) = opts.tip_page {
        // The daemon raises anything below its floor rather than spinning, so a
        // `0` or `1` here would silently run at a different page size than the
        // operator asked for — and page size is the number a recorded timing is
        // compared on. Say why instead.
        if n < 2 {
            return Err(format!(
                "--tip-page {n} is below the minimum of 2: a resuming page re-reads \
                 its inclusive start (the cursor row) and drops it, so a one-row \
                 page carries no new work"
            ));
        }
    }
    Ok(())
}

fn rekey_request_body(opts: &RekeyCliOpts) -> Value {
    let mut body = serde_json::json!({
        "dry_run": !opts.execute,
        "remove_flat": opts.remove_flat,
    });
    if opts.progress {
        body["progress_only"] = serde_json::json!(true);
    }
    if let Some(n) = opts.max_ops {
        body["max_ops"] = serde_json::json!(n);
    }
    if let Some(n) = opts.tip_page {
        body["tip_page"] = serde_json::json!(n);
    }
    if let Some(n) = opts.audit_unresolved {
        body["audit_unresolved"] = serde_json::json!(n);
    }
    body
}

pub(crate) fn db_rekey_atom_partition_prefix(
    socket: &Path,
    opts: RekeyCliOpts,
) -> Result<(), String> {
    validate_rekey_opts(&opts)?;
    if opts.until_complete {
        db_rekey_until_complete(
            socket,
            opts.remove_flat,
            opts.max_ops.unwrap_or(REKEY_UNTIL_COMPLETE_OPS),
            opts.tip_page,
            opts.json_only,
        )?;
        if opts.compact_after {
            println!();
            println!("Flat-key retirement completed; compacting atoms to return bytes.");
            db_compact(socket, Some("atoms"), false, true, false)?;
        }
        return Ok(());
    }
    let mut report = db_rekey_post(socket, &rekey_request_body(&opts))?;
    if opts.progress {
        add_rekey_progress_derived_fields(&mut report);
    }
    if opts.json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    if opts.progress {
        print_rekey_progress(&report);
        return Ok(());
    }
    print_rekey_counters(&report, opts.execute);
    if !opts.execute {
        println!();
        println!(
            "This is a PLAN. `would_dual_write` above is work still to do, and `completed` is\n\
             always false for a dry run. Re-run with --execute to migrate, or use --progress to\n\
             read the durable checkpoint."
        );
    }
    if opts.audit_unresolved.is_some() {
        print_unresolved_audit(&report);
        print_flat_only_rollup(&report);
    }
    if !opts.execute {
        println!();
        println!("Re-run with --execute to dual-write prefixed bodies + locators.");
        println!("After dual-write: set LASTDB_ATOM_KEY_ENCODING=partition_prefix and restart.");
        println!(
            "Then retire and return bytes with: --execute --remove-flat \\
             --until-complete --compact-after"
        );
    }
    Ok(())
}

fn print_rekey_counters(report: &Value, execute: bool) {
    let mode = if execute {
        "EXECUTED"
    } else {
        "DRY RUN — plan only, nothing was written"
    };
    println!("Atom partition-prefix rekey — {mode}");
    // Page size governs how a timing from this pass compares to the next one, so
    // print it beside the counters rather than leaving it implicit in the build.
    println!("  tip_page: {}", rekey_u64(report, "tip_page"));
    // `would_dual_write` is REMAINING work. Naming it `dual_written` in a
    // dry-run report is what got a 8.8%-done migration filed as finished.
    let written_key = if execute {
        "dual_written"
    } else {
        "would_dual_write"
    };
    let counters = [
        "tips_scanned",
        "slots_considered",
        written_key,
        "already_prefixed",
        "missing_body",
        "flat_removed",
    ];
    for key in counters {
        println!("  {key}: {}", count_of(report, key));
    }
    println!(
        "  scan_reached_end: {}",
        flag_of(report, "scan_reached_end")
    );
    println!("  completed: {}", flag_of(report, "completed"));
}

fn print_unresolved_audit(report: &Value) {
    let mis_derived = count_of(report, "unresolved_mis_derived");
    println!();
    println!("Unresolved-tip classification");
    println!("  mis_derived_partition:  {mis_derived}");
    println!(
        "  orphan_locator:         {}",
        count_of(report, "unresolved_orphan_locator")
    );
    println!(
        "  undecodable_locator:    {}",
        count_of(report, "unresolved_undecodable_locator")
    );
    println!(
        "  no_body_anywhere:       {}",
        count_of(report, "unresolved_no_body")
    );
    if mis_derived > 0 {
        println!();
        println!(
            "REMOVE-FLAT UNSAFE: {mis_derived} live bodies sit under a partition no \
             tip-driven read derives."
        );
    }
    if let Some(rows) = report
        .get("unresolved")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
    {
        println!();
        println!("  class / atom_uuid / derived_partition / locator_partition");
        for row in rows {
            let s = |key: &str| {
                row.get(key)
                    .and_then(Value::as_str)
                    .unwrap_or("-")
                    .to_string()
            };
            // Partitions carry a NUL separator — escape so the line stays
            // one line in a terminal and in a redirected log.
            let esc = |v: String| v.escape_default().to_string();
            println!(
                "  {} {} {} {}",
                s("class"),
                s("atom_uuid"),
                esc(s("derived_partition")),
                esc(s("locator_partition")),
            );
        }
    }
    if flag_of(report, "unresolved_truncated") {
        println!();
        println!("Detail rows truncated by --audit-limit; counters above are complete.");
    }
}

/// Roll flat-only tips up by molecule: after a completed migration this
/// population growing means a writer still places bodies flat, and the
/// molecules it touches are what identify it.
fn print_flat_only_rollup(report: &Value) {
    let Some(rows) = report
        .get("would_dual_write_tips")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
    else {
        return;
    };
    let mut by_molecule: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    for row in rows {
        let molecule = row
            .get("tip_key")
            .and_then(Value::as_str)
            .and_then(|k| k.split(':').nth(1))
            .unwrap_or("-")
            .to_string();
        *by_molecule.entry(molecule).or_default() += 1;
    }
    println!();
    println!("Flat-only bodies (would_dual_write) by molecule — after a completed");
    println!("migration these name the writer still placing bodies flat:");
    let mut rollup: Vec<(String, u64)> = by_molecule.into_iter().collect();
    rollup.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (molecule, tips) in rollup {
        println!("  {molecule} x{tips}");
    }
    if flag_of(report, "would_dual_write_truncated") {
        println!("  (detail capped by --audit-limit; the counter is complete)");
    }
}

/// `mk:` records one daemon call decides by default. Every one costs an atom
/// body read, and a stamping pass adds a write, so an unbounded call runs for
/// minutes and trips the control socket's read deadline — the operator then sees
/// a failure for work that is still running, and after a `--execute` cannot say
/// whether anything was written. This CLI walks in bounded passes instead.
///
/// 200,000 was the first guess and it was too coarse: a `--schema Card`
/// stamping pass selects ~148,000 keys, stays under the cap, and still took
/// longer than the deadline. Sized so a stamping pass finishes well inside it;
/// override with `--max-keys` when the shape of a store says otherwise.
pub(crate) const TOMBSTONE_AUDIT_KEYS_PER_CALL: usize = 50_000;
