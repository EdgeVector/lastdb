use super::*;

pub(crate) fn db_gc_proteins(socket: &Path, execute: bool, json_only: bool) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/gc-proteins",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let report = value
        .get("gc_proteins")
        .cloned()
        .ok_or_else(|| "response missing gc_proteins".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Orphan protein GC — {mode}");
    println!(
        "  proteins_scanned:       {}",
        report
            .get("proteins_scanned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  proteins_with_members:  {}",
        report
            .get("proteins_with_members")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  molprot_backrefs:       {}",
        report
            .get("molprot_backrefs_scanned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  referenced_empty:       {}",
        report
            .get("proteins_referenced_by_backref")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  orphan_proteins:        {}",
        report
            .get("orphan_proteins")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  proteins_deleted:       {}",
        report
            .get("proteins_deleted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    let bytes = report
        .get("bytes_freed_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  protein_bytes_approx:  {}", format_bytes(bytes));
    if !execute {
        println!();
        println!("Re-run with --execute to delete empty, unbound protein rows.");
    }
    Ok(())
}

pub(crate) fn db_reclaim_keep_small_snapshot(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/reclaim-keep-small-snapshot",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let report = value
        .get("reclaim_keep_small_snapshot")
        .cloned()
        .ok_or_else(|| "response missing reclaim_keep_small_snapshot".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    let str_of = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?")
    };
    let u64_of = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    println!("Keep-small snapshot group reclaim — {mode}");
    println!("  collection:    {}", str_of("collection"));
    println!("  key:           {}", str_of("expected_only_id"));
    println!("  group dir:     {}", str_of("dir"));
    println!("  segments:      {}", u64_of("segments"));
    println!(
        "  on_disk_bytes: {} ({})",
        u64_of("on_disk_bytes"),
        format_bytes(u64_of("on_disk_bytes"))
    );
    println!(
        "  dropped:       {}",
        report
            .get("dropped")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    if !execute {
        println!("Re-run with --execute to remove the group directory and return the bytes.");
    }
    Ok(())
}
