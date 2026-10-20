use super::*;

/// Per-key detail rows a `--schema` repair reports when `--audit-limit` is
/// not given.
pub(crate) const SCOPED_REPAIR_AUDIT_LIMIT: usize = 1000;

/// Bounds and key scope for `lastdb db repair-dangling-tips`.
pub(crate) struct RepairDanglingTipsArgs {
    pub(crate) max_ops: Option<usize>,
    pub(crate) tip_page: Option<usize>,
    pub(crate) audit_limit: Option<usize>,
    pub(crate) schema: Option<String>,
    pub(crate) hash_key: Option<String>,
}

#[path = "db_dangling_tips_report.rs"]
mod db_dangling_tips_report;
use db_dangling_tips_report::*;

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

pub(crate) fn db_repair_dangling_tips(
    socket: &Path,
    execute: bool,
    args: &RepairDanglingTipsArgs,
    json_only: bool,
) -> Result<(), String> {
    let request = repair_dangling_tips_request(execute, args);
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
    print_repair_scope(scope);
    print_repair_counters(&report);
    print_refused_molecules(&report);
    print_unresolved_tips(&report, scope.is_some());
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

fn repair_dangling_tips_request(execute: bool, args: &RepairDanglingTipsArgs) -> serde_json::Value {
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
    request
}

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
    print_probe_counts(&report);
    print_probe_rates(&report);
    print_probe_completion(&report);
    println!();
    println!("Cached for `lastdb status` until the next probe or process restart.");
    Ok(())
}

fn print_probe_counts(report: &serde_json::Value) {
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
}

fn print_probe_rates(report: &serde_json::Value) {
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
}

fn print_probe_completion(report: &serde_json::Value) {
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
}
