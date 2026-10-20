use super::*;

pub(crate) fn db_gc_atoms(
    socket: &Path,
    schema: Option<&str>,
    execute: bool,
    allow_after_peer_apply: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/gc-atoms",
        &serde_json::json!({
            "dry_run": !execute,
            "schema": schema,
            "allow_after_peer_apply": allow_after_peer_apply,
        }),
    )?;
    let report = value
        .get("gc_atoms")
        .cloned()
        .ok_or_else(|| "response missing gc_atoms".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    let scope = schema.map_or(String::new(), |name| format!(" — schema {name}"));
    println!("Orphan atom GC — {mode}{scope}");
    println!(
        "  atoms_scanned:          {}",
        report
            .get("atoms_scanned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  atoms_referenced:       {}",
        report
            .get("atoms_referenced")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  atoms_deleted:          {}",
        report
            .get("atoms_deleted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    let bytes = report
        .get("bytes_freed_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  atom_bytes_approx:      {}", format_bytes(bytes));
    if !execute {
        println!();
        println!("Re-run with --execute to delete orphan atoms.");
    }
    Ok(())
}
