use super::*;

/// Fold one bounded pass into the running total, so a walk over several calls
/// prints (and emits as JSON) the same shape a single call would.
// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn merge_tombstone_audit_passes(
    acc: serde_json::Value,
    pass: serde_json::Value,
) -> serde_json::Value {
    let serde_json::Value::Object(mut acc) = acc else {
        return pass;
    };
    let serde_json::Value::Object(pass) = pass else {
        return serde_json::Value::Object(acc);
    };
    let sum = |a: &serde_json::Value, b: &serde_json::Value| -> serde_json::Value {
        serde_json::json!(a
            .as_u64()
            .unwrap_or(0)
            .saturating_add(b.as_u64().unwrap_or(0)))
    };

    let stamped = sum(
        acc.get("keys_stamped").unwrap_or(&serde_json::Value::Null),
        pass.get("keys_stamped").unwrap_or(&serde_json::Value::Null),
    );
    acc.insert("keys_stamped".to_string(), stamped);
    // The cursor and the "is there more" answer are the latest pass's, not a sum.
    for key in ["more_remaining", "next_after_key"] {
        match pass.get(key) {
            Some(v) => {
                acc.insert(key.to_string(), v.clone());
            }
            None => {
                acc.remove(key);
            }
        }
    }

    let (Some(serde_json::Value::Object(acc_audit)), Some(serde_json::Value::Object(pass_audit))) =
        (acc.get("audit").cloned(), pass.get("audit"))
    else {
        return serde_json::Value::Object(acc);
    };
    let mut audit = acc_audit;
    for key in [
        "keys_scanned",
        "keys_unreadable",
        "meta_tombstoned",
        "live",
        "content_tombstoned_meta_false",
        "atoms_missing",
        "atoms_fetched",
    ] {
        let merged = sum(
            audit.get(key).unwrap_or(&serde_json::Value::Null),
            pass_audit.get(key).unwrap_or(&serde_json::Value::Null),
        );
        audit.insert(key.to_string(), merged);
    }

    // A molecule can straddle a pass boundary, so per-molecule rows add up too.
    let mut rows: Vec<serde_json::Value> = audit
        .get("per_molecule")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for row in pass_audit
        .get("per_molecule")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let molecule = row.get("molecule").and_then(serde_json::Value::as_str);
        let existing = rows
            .iter_mut()
            .find(|r| r.get("molecule").and_then(serde_json::Value::as_str) == molecule);
        match existing {
            None => rows.push(row),
            Some(target) => {
                for key in [
                    "keys",
                    "meta_tombstoned",
                    "content_tombstoned_meta_false",
                    "atoms_missing",
                ] {
                    let merged = sum(
                        target.get(key).unwrap_or(&serde_json::Value::Null),
                        row.get(key).unwrap_or(&serde_json::Value::Null),
                    );
                    target[key] = merged;
                }
            }
        }
    }
    rows.sort_by_key(|r| {
        std::cmp::Reverse(
            r.get("content_tombstoned_meta_false")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        )
    });
    audit.insert("per_molecule".to_string(), serde_json::Value::Array(rows));
    acc.insert("audit".to_string(), serde_json::Value::Object(audit));
    serde_json::Value::Object(acc)
}

pub(crate) struct DrainPlaneResidueCliOpts {
    pub(crate) family: String,
    pub(crate) source: String,
    pub(crate) target: Option<String>,
    pub(crate) execute: bool,
    pub(crate) after: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) drop_empty_source: bool,
    pub(crate) until_complete: bool,
    pub(crate) json_only: bool,
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_drain_plane_residue(
    socket: &Path,
    opts: &DrainPlaneResidueCliOpts,
) -> Result<(), String> {
    // Accept both CLI spellings; the wire format is snake_case serde.
    let family = opts.family.trim().to_lowercase().replace('-', "_");
    let target = opts.target.clone().unwrap_or_else(|| {
        match family.as_str() {
            "protein" => "proteins",
            "index" => "indexes",
            // tip / conflict / order_log all live on tips canonically.
            _ => "tips",
        }
        .to_string()
    });

    let mut after = opts.after.clone();
    let mut passes = 0u32;
    let mut totals = serde_json::json!({
        "keys_scanned": 0u64,
        "copied_to_target": 0u64,
        "target_already_won": 0u64,
        "deleted_from_source": 0u64,
        "skipped": 0u64,
    });
    let mut report = loop {
        let mut body = serde_json::json!({
            "family": family,
            "source_collection": opts.source,
            "target_collection": target,
            "execute": opts.execute,
            "drop_empty_source": opts.drop_empty_source,
        });
        if let Some(limit) = opts.limit {
            body["limit"] = serde_json::json!(limit);
        }
        if let Some(cursor) = &after {
            body["after"] = serde_json::Value::String(cursor.clone());
        }
        let value = db_post_json(socket, "/api/db/drain-plane-residue", &body)?;
        let pass = value
            .get("plane_residue_drain")
            .cloned()
            .ok_or_else(|| "response missing plane_residue_drain".to_string())?;
        passes += 1;
        for counter in [
            "keys_scanned",
            "copied_to_target",
            "target_already_won",
            "deleted_from_source",
            "skipped",
        ] {
            let add = pass
                .get(counter)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let sum = totals
                .get(counter)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                .saturating_add(add);
            totals[counter] = serde_json::json!(sum);
        }
        let done = pass
            .get("done")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        after = pass
            .get("after")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if done || !opts.until_complete || after.is_none() {
            break pass;
        }
        if !opts.json_only {
            let scanned = totals["keys_scanned"].as_u64().unwrap_or(0);
            println!("  … pass {passes}: {scanned} rows decided so far");
        }
    };

    report["passes"] = serde_json::json!(passes);
    report["totals"] = totals.clone();
    if opts.json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let n = |key: &str| {
        totals
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    println!(
        "plane-residue drain {} {} -> {} ({}):",
        family,
        opts.source,
        target,
        if opts.execute { "execute" } else { "DRY RUN" },
    );
    println!("  passes:              {passes}");
    println!("  keys_scanned:        {}", n("keys_scanned"));
    println!("  copied_to_target:    {}", n("copied_to_target"));
    println!("  target_already_won:  {}", n("target_already_won"));
    println!("  deleted_from_source: {}", n("deleted_from_source"));
    println!("  skipped:             {}", n("skipped"));
    let done = report
        .get("done")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let dropped = report
        .get("source_dropped")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("  done: {done}  source_dropped: {dropped}");
    if !done {
        if let Some(cursor) = report.get("after").and_then(serde_json::Value::as_str) {
            println!("  resume with: --after {}", cursor.escape_default());
        }
    }
    if !opts.execute {
        println!();
        println!("This is a DRY RUN — nothing was copied or deleted. Re-run with --execute.");
    }
    Ok(())
}
