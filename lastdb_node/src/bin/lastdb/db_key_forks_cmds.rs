use super::*;

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_legacy_key_forks(
    socket: &Path,
    execute: bool,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    use fold_db::db_operations::{LegacyKeyForkAudit, LegacyKeyForkMoleculeStat};

    let max_keys = max_keys.unwrap_or(LEGACY_KEY_FORK_KEYS_PER_CALL);
    let mut total = LegacyKeyForkAudit {
        dry_run: !execute,
        ..Default::default()
    };
    let mut by_molecule: HashMap<String, LegacyKeyForkMoleculeStat> = HashMap::new();
    let mut after_key: Option<String> = None;
    let mut passes = 0u32;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "max_keys": max_keys,
        });
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/legacy-key-fork-audit", &body)?;
        let pass: LegacyKeyForkAudit = serde_json::from_value(
            value
                .get("legacy_key_fork_audit")
                .cloned()
                .ok_or_else(|| "response missing legacy_key_fork_audit".to_string())?,
        )
        .map_err(|e| format!("decode legacy-key-fork audit: {e}"))?;
        passes += 1;
        // Captured before `pass.per_molecule` is moved below, and kept as this
        // pass's own delta: `total.twin_atoms_live` is the running sum.
        let pass_twin_atoms_live = pass.twin_atoms_live;
        total.keys_scanned += pass.keys_scanned;
        total.keys_unreadable += pass.keys_unreadable;
        total.current_form += pass.current_form;
        total.legacy_form += pass.legacy_form;
        total.forked += pass.forked;
        total.legacy_only += pass.legacy_only;
        total.twin_atoms_live += pass.twin_atoms_live;
        total.twin_atoms_missing += pass.twin_atoms_missing;
        total.twin_atoms_tombstoned += pass.twin_atoms_tombstoned;
        total.legacy_tips_deleted += pass.legacy_tips_deleted;
        total.bytes_freed_approx += pass.bytes_freed_approx;
        for row in pass.per_molecule {
            let aggregate = by_molecule.entry(row.molecule.clone()).or_insert_with(|| {
                LegacyKeyForkMoleculeStat {
                    molecule: row.molecule.clone(),
                    ..Default::default()
                }
            });
            aggregate.keys += row.keys;
            aggregate.current_form += row.current_form;
            aggregate.legacy_form += row.legacy_form;
            aggregate.forked += row.forked;
            aggregate.legacy_only += row.legacy_only;
            aggregate.twin_atoms_live += row.twin_atoms_live;
            aggregate.twin_atoms_missing += row.twin_atoms_missing;
            aggregate.twin_atoms_tombstoned += row.twin_atoms_tombstoned;
            aggregate.legacy_tips_deleted += row.legacy_tips_deleted;
        }
        after_key = pass.next_after_key;
        if !pass.more_remaining || after_key.is_none() {
            break;
        }
        if !json_only {
            println!(
                "{}",
                pass_progress_line(
                    u64::from(passes),
                    total.keys_scanned,
                    &[("safe forks", pass_twin_atoms_live)],
                )
            );
        }
    }
    total.per_molecule = by_molecule.into_values().collect();
    total.per_molecule.sort_by(|a, b| {
        b.forked
            .cmp(&a.forked)
            .then_with(|| a.molecule.cmp(&b.molecule))
    });
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&total).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    println!("Legacy HashKey-encoding fork audit ({passes} pass(es))");
    println!("  keys scanned:          {}", total.keys_scanned);
    println!("  current-form tips:     {}", total.current_form);
    println!("  legacy-form tips:      {}", total.legacy_form);
    println!("  forked (twin exists):  {}", total.forked);
    println!("  legacy-only protected: {}", total.legacy_only);
    println!("  live twin atoms:       {}", total.twin_atoms_live);
    println!("  missing twin atoms:    {}", total.twin_atoms_missing);
    println!("  tombstoned twin atoms: {}", total.twin_atoms_tombstoned);
    println!("  legacy tips deleted:   {}", total.legacy_tips_deleted);
    println!("  bytes freed (approx):  {}", total.bytes_freed_approx);
    if !execute {
        println!("\nDry run only. Prove on a CoW clone before re-running `drain-legacy-key-forks --execute`.");
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_order_log_audit(
    socket: &Path,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(ORDER_LOG_AUDIT_KEYS_PER_CALL);
    let field = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(serde_json::Value::as_u64).unwrap_or(0)
    };

    // Totals accumulate across passes; `short` collects every finding so the
    // summary describes the whole store, not the last page of it.
    let mut totals: HashMap<String, u64> = HashMap::new();
    let mut short: Vec<serde_json::Value> = Vec::new();
    let mut after_key: Option<String> = None;
    let mut passes = 0u32;
    loop {
        let mut body = serde_json::json!({ "max_keys": max_keys });
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/order-log-audit", &body)?;
        let pass = value
            .get("order_log_audit")
            .cloned()
            .ok_or_else(|| "response missing order_log_audit".to_string())?;
        passes += 1;
        for key in [
            "keys_scanned",
            "molecules_decided",
            "molecules_ok",
            "molecules_without_order_count",
            "molecules_short",
            "entries_missing",
            "logical_entries_missing",
            "molecules_with_duplicate_storage_rows",
            "order_counts_without_keys",
            "short_candidates_rechecked",
            "short_candidates_cleared_by_recheck",
        ] {
            *totals.entry(key.to_string()).or_default() += field(&pass, key);
        }
        // `moc:` rows are re-read whole on every pass, so this one is a
        // property of the store, not a sum over passes.
        totals.insert(
            "order_counts_read".to_string(),
            field(&pass, "order_counts_read"),
        );
        totals.insert(
            "order_counts_unreadable".to_string(),
            field(&pass, "order_counts_unreadable"),
        );
        if let Some(rows) = pass
            .get("short_molecules")
            .and_then(serde_json::Value::as_array)
        {
            short.extend(rows.iter().cloned());
        }
        let more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        after_key = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if !more || after_key.is_none() {
            break;
        }
        if !json_only {
            println!(
                "{}",
                pass_progress_line(
                    u64::from(passes),
                    totals.get("keys_scanned").copied().unwrap_or(0),
                    &[],
                )
            );
        }
    }

    short.sort_by_key(|row| std::cmp::Reverse(field(row, "shortfall")));
    let molecules_short = totals.get("molecules_short").copied().unwrap_or(0);
    let entries_missing = totals.get("entries_missing").copied().unwrap_or(0);

    if json_only {
        let mut out = serde_json::Map::new();
        for (key, value) in &totals {
            out.insert(key.clone(), serde_json::json!(value));
        }
        out.insert("short_molecules".into(), serde_json::json!(short));
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(out))
                .map_err(|e| format!("serialize: {e}"))?
        );
    } else {
        println!("Order-log audit — invariant: moc:{{M}} >= count(mk:{{M}}:…)");
        for key in [
            "order_counts_read",
            "order_counts_unreadable",
            "keys_scanned",
            "molecules_decided",
            "molecules_ok",
            "molecules_without_order_count",
            "order_counts_without_keys",
            "short_candidates_rechecked",
            "short_candidates_cleared_by_recheck",
            "molecules_short",
            "entries_missing",
            "logical_entries_missing",
            "molecules_with_duplicate_storage_rows",
        ] {
            println!("  {key}: {}", totals.get(key).copied().unwrap_or(0));
        }
        // A pass that raced heavy writes clears candidates it would once have
        // reported. Say so, so a lower count than a previous run reads as this
        // and not as damage that repaired itself.
        let cleared = totals
            .get("short_candidates_cleared_by_recheck")
            .copied()
            .unwrap_or(0);
        if cleared > 0 {
            println!(
                "  note: {cleared} candidate(s) cleared on re-read — their log had \
                 caught up by the time their keys were counted (concurrent writes, \
                 not damage)."
            );
        }
        if !short.is_empty() {
            println!();
            println!("Molecules whose order log is short (worst first):");
            for row in short.iter().take(50) {
                // An unattributed molecule is a real finding (schema dropped, or
                // not loaded on this node), so say so rather than leaving a gap.
                let schema = row
                    .get("schema")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unattributed>");
                let unique = field(row, "unique_keys");
                let logical = field(row, "logical_shortfall");
                let dups = field(row, "duplicate_storage_rows");
                println!(
                    "  {} [{schema}]: moc:={} raw_keys={} unique_keys={} shortfall={} logical_shortfall={} dups={}",
                    row.get("molecule")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?"),
                    field(row, "order_count"),
                    field(row, "keys"),
                    unique,
                    field(row, "shortfall"),
                    logical,
                    dups,
                );
            }
        }
        println!();
        let unreadable = totals.get("order_counts_unreadable").copied().unwrap_or(0);
        if unreadable > 0 {
            println!(
                "WARNING: {unreadable} `moc:` row(s) did not decode. Those molecules are \
                 indistinguishable from ones with no order log and were NOT checked — \
                 the verdict below covers only the rest of the store."
            );
        }
        if molecules_short == 0 {
            println!(
                "No molecule holds an order log shorter than its key set. \
                 No truncated `update_order` is on this store."
            );
        } else {
            println!(
                "{molecules_short} molecule(s) are missing {entries_missing} order-log entrie(s). \
                 SampleN under-reports on these; point reads and HashKey lookups are unaffected. \
                 An order log records the sequence in which values changed and is NOT derivable \
                 from the current per-key records — do not attempt a reconstruction."
            );
        }
    }

    if molecules_short > 0 {
        return Err(format!(
            "order-log audit found {molecules_short} short molecule(s)"
        ));
    }
    Ok(())
}

pub(crate) type OrderLogBloatSchemaTotals =
    std::collections::BTreeMap<String, serde_json::Map<String, serde_json::Value>>;
