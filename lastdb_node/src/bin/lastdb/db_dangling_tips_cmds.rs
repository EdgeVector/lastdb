use super::*;

pub(crate) fn db_unresolved_atoms(socket: &Path, json_only: bool) -> Result<(), String> {
    let req = format!(
        "GET /api/db/unresolved-atoms HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Connection: close\r\n\
         \r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), Duration::from_secs(30))?;
    let value = parse_json_response(&response, "/api/db/unresolved-atoms")?;
    let report = value
        .get("unresolved_atoms")
        .ok_or_else(|| "response missing unresolved_atoms".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let edges = report
        .get("edges")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "response missing unresolved atom edge count".to_string())?;
    let rows = report
        .get("rows")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "response missing unresolved atom record key count".to_string())?;
    let capped = report
        .get("capped")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "response missing unresolved atom cap flag".to_string())?;
    println!("Unresolved atoms: {rows} record key(s), {edges} atom edge(s)");
    if capped {
        println!("Identity cap reached; these counts are lower bounds.");
    }
    for identity in report
        .get("identities")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "response missing unresolved atom identities".to_string())?
    {
        let key: fold_db::schema::types::key_value::KeyValue = serde_json::from_value(
            identity
                .get("key")
                .cloned()
                .ok_or_else(|| "unresolved atom identity lacks key".to_string())?,
        )
        .map_err(|error| format!("invalid unresolved atom key: {error}"))?;
        let atom_uuid = identity
            .get("atom_uuid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "unresolved atom identity lacks atom_uuid".to_string())?;
        let molecule_uuid = identity
            .get("molecule_uuid")
            .and_then(serde_json::Value::as_str);
        let schema = identity.get("schema").and_then(serde_json::Value::as_str);
        let field = identity.get("field").and_then(serde_json::Value::as_str);
        let storage_namespace = identity
            .get("storage_namespace")
            .and_then(serde_json::Value::as_str);
        let tip_storage_key = identity
            .get("tip_storage_key")
            .and_then(serde_json::Value::as_str);
        let atom_storage_key = identity
            .get("atom_storage_key")
            .and_then(serde_json::Value::as_str);
        println!(
            "  key={} atom_uuid={atom_uuid} molecule_uuid={molecule_uuid:?} schema={schema:?} field={field:?} namespace={storage_namespace:?} tip_storage_key={tip_storage_key:?} atom_storage_key={atom_storage_key:?}",
            key.to_storage_key()
        );
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_repair_dangling_tips(
    socket: &Path,
    execute: bool,
    args: &RepairDanglingTipsArgs,
    json_only: bool,
) -> Result<(), String> {
    // A scoped pass exists to name the keys it would touch, so it reports
    // per-key detail by default; an unscoped pass keeps its old default (none).
    let audit_limit = args
        .audit_limit
        .or(args.schema.as_ref().map(|_| SCOPED_REPAIR_AUDIT_LIMIT));
    let mut request = serde_json::json!({
        "dry_run": !execute,
        "max_ops": args.max_ops,
        "tip_page": args.tip_page,
        "audit_unresolved": audit_limit,
    });
    // Sent only when set, so an unscoped call is byte-identical to the
    // request an older daemon already accepts.
    if let Some(schema) = &args.schema {
        request["schema"] = serde_json::json!(schema);
    }
    if let Some(hash_key) = &args.hash_key {
        request["hash_key"] = serde_json::json!(hash_key);
    }
    let value = db_post_json(socket, "/api/db/repair-dangling-tips", &request)?;
    let report = value
        .get("repair_dangling_tips")
        .cloned()
        .ok_or_else(|| "response missing repair_dangling_tips".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Dangling tip repair — {mode}");
    let scope = report.get("scope").filter(|s| !s.is_null());
    if let Some(scope) = scope {
        let list = |key: &str| {
            scope
                .get(key)
                .and_then(serde_json::Value::as_array)
                .map(|v| {
                    v.iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        };
        println!("  scope: schema(s) {}", list("schemas"));
        if let Some(hash) = scope.get("hash_key").and_then(serde_json::Value::as_str) {
            println!("  scope: hash key {hash}");
        }
        println!(
            "  scope: {} molecule(s), {} key range(s); `completed` covers this scope only",
            scope
                .get("molecule_uuids")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len),
            scope
                .get("key_ranges")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    for key in [
        "tips_scanned",
        "repairable_tips",
        "tips_repaired",
        "skipped_changed",
        "skipped_molecule_missing",
        "skipped_atom_not_in_molecule",
        "skipped_body_restored",
        "skipped_mis_derived",
        "skipped_unparseable_key",
        "skipped_unrepairable",
        "rescued_by_live_probe",
        "failed_repairs",
        "storage_keys_deleted",
    ] {
        println!(
            "  {key:24} {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    println!(
        "  completed                {}",
        report
            .get("completed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    // `skipped_unrepairable` is a size; this is what it sits on. Without it the
    // only record of which molecules were refused is a daemon-log warn line, so
    // an operator reading the report cannot tell bounded, explained residue from
    // an unexplained one.
    if let Some(refused) = report
        .get("refused_molecules")
        .and_then(serde_json::Value::as_array)
        .filter(|refused| !refused.is_empty())
    {
        println!();
        println!("  Refused molecules (left completely untouched):");
        for entry in refused {
            let field = |key: &str| {
                entry
                    .get(key)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            println!(
                "    {} — {} key(s) over the {}-byte limit (longest {}), {} tip(s) left in place",
                entry
                    .get("molecule_uuid")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("?"),
                field("keys_over_limit"),
                field("key_limit_bytes"),
                field("longest_key_bytes"),
                field("tips_left_in_place"),
            );
        }
        println!(
            "  These are correct refusals: rewriting would emit a key the store \
             rejects, and the repair is delete-then-store."
        );
    }
    // `unresolved[].schema` joins each dangling molecule against the live
    // schema catalog. Grouped here so "which schema is producing these" is a
    // line count on the report itself, not a separate molecule-keys probe
    // per molecule uuid.
    if let Some(rows) = report
        .get("unresolved")
        .and_then(serde_json::Value::as_array)
        .filter(|rows| !rows.is_empty())
    {
        use std::collections::BTreeMap;
        let mut by_schema: BTreeMap<String, u64> = BTreeMap::new();
        for row in rows {
            let label = row
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("(unattributed)")
                .to_string();
            *by_schema.entry(label).or_insert(0) += 1;
        }
        println!();
        println!(
            "  Unresolved tips by schema (of {} detail row(s) captured, use --audit-limit to widen):",
            rows.len()
        );
        for (schema, count) in by_schema {
            println!("    {count:6}  {schema}");
        }
        // A scoped pass is small by construction, so name each key: that is
        // the list an operator checks before re-running with --execute.
        if scope.is_some() {
            println!();
            println!("  Repairable tips (tip key -> missing atom):");
            for row in rows {
                let field = |key: &str| {
                    row.get(key)
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?")
                        .to_string()
                };
                println!(
                    "    {} -> {}",
                    field("tip_key").escape_debug(),
                    field("atom_uuid")
                );
            }
        }
    }
    if report
        .get("aborted_on_repeated_failures")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        println!();
        println!(
            "  WARNING: stopped early on an unbroken run of repair failures — \
             treat this as a store fault, not a few bad molecules."
        );
    }
    if !execute {
        println!();
        println!("Re-run with --execute to remove repairable dangling tips.");
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_probe_locator_only(
    socket: &Path,
    max_tips: Option<usize>,
    tip_page: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/probe-locator-only",
        &serde_json::json!({
            "max_tips": max_tips,
            "tip_page": tip_page,
        }),
    )?;
    let report = value
        .get("probe_locator_only")
        .cloned()
        .ok_or_else(|| "response missing probe_locator_only".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    println!("Locator-only tip population probe (read-only stratified sample)");
    for key in [
        "tips_sampled",
        "max_tips",
        "strata",
        "strata_exhausted",
        "locator_only",
        "body_at_derived_or_flat",
        "dangling",
        "other_unresolved",
        "tip_page",
        "probed_at_unix",
    ] {
        println!(
            "  {key:24} {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    if let Some(rate) = report
        .get("locator_only_per_mille")
        .and_then(serde_json::Value::as_u64)
    {
        println!("  locator_only_per_mille  {rate}  (‰ of sample)");
    }
    if let Some(rate) = report
        .get("dangling_per_mille")
        .and_then(serde_json::Value::as_u64)
    {
        println!("  dangling_per_mille      {rate}  (‰ of sample — no reader route)");
    }
    // Printed only when measured. These are absent on the first probe of a
    // process, and printing a default 0 there would read as "no recurrence"
    // when it means "no earlier sample to compare".
    if let Some(prev) = report
        .get("prev_dangling")
        .and_then(serde_json::Value::as_u64)
    {
        let at = report
            .get("prev_probed_at_unix")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!("  prev_dangling           {prev}  (previous probe, at {at})");
    }
    if let Some(rate) = report
        .get("dangling_recurrence_per_hour")
        .and_then(serde_json::Value::as_u64)
    {
        println!("  dangling_recurrence     {rate}  (new dangling tips per hour)");
    }
    println!(
        "  completed                {}",
        report
            .get("completed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    if !report
        .get("completed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let exhausted = report
            .get("strata_exhausted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let strata = report
            .get("strata")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!();
        println!(
            "Sample hit max_tips in {} of {strata} windows — rates are estimates.",
            strata.saturating_sub(exhausted)
        );
        println!(
            "The budget is spread across {strata} windows partitioning `mk:`, not spent on the"
        );
        println!(
            "first max_tips keys, so the estimate is not biased toward the head of the keyspace."
        );
        println!("Raise --max-tips to refine it.");
    }
    println!();
    println!("Cached for `lastdb status` until the next probe or process restart.");
    Ok(())
}
