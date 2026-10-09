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

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_reclaim_keep_small_legacy(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/reclaim-keep-small-legacy",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let mut report = value
        .get("reclaim_keep_small_legacy")
        .cloned()
        .ok_or_else(|| "response missing reclaim_keep_small_legacy".to_string())?;
    // The retired `metadata` group is usually gone after the first reclaim,
    // while `lastdb status` still shows keep_small bytes. Those live in the
    // CURRENT keep_small group, which a different verb reclaims. Measure it
    // with that verb's dry run (no load, no delete) and name it, so the
    // operator sees which group the residue is in and which command returns it.
    let current = match db_post_json(
        socket,
        "/api/db/reclaim-keep-small-snapshot",
        &serde_json::json!({ "dry_run": true }),
    ) {
        Ok(v) => v
            .get("reclaim_keep_small_snapshot")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        Err(e) => serde_json::json!({ "error": e }),
    };
    if let Some(obj) = report.as_object_mut() {
        obj.insert("current_keep_small_group".to_string(), current.clone());
        obj.insert(
            "current_keep_small_reclaim_command".to_string(),
            serde_json::Value::String("lastdb db reclaim-keep-small-snapshot".to_string()),
        );
    }
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
            .to_string()
    };
    let u64_of = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    println!("Legacy keep-small group reclaim (metadata) — {mode}");
    println!("  collection:    {}", str_of("collection"));
    println!("  key:           {}", str_of("expected_only_id"));
    println!("  group dir:     {}", str_of("dir"));
    println!("  segments:      {}", u64_of("segments"));
    println!(
        "  on_disk_bytes: {} ({})",
        u64_of("on_disk_bytes"),
        format_bytes(u64_of("on_disk_bytes"))
    );
    if let Some(ids) = report.get("ids").and_then(|v| v.as_array()) {
        let ids: Vec<&str> = ids.iter().filter_map(|v| v.as_str()).collect();
        println!("  ids recorded:  {ids:?}");
    }
    let dropped = report
        .get("dropped")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("  dropped:       {dropped}");
    let already_absent = report
        .get("already_absent")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("  already_absent: {already_absent}");
    if already_absent {
        println!("The retired group is already gone; there is nothing to reclaim here.");
    } else if !execute {
        println!("Re-run with --execute to remove the group directory and return the bytes.");
    }
    println!();
    println!("Current keep_small group (measured by a dry run, nothing removed):");
    if let Some(err) = current.get("error").and_then(serde_json::Value::as_str) {
        println!("  could not measure: {err}");
    } else {
        let cur_str = |key: &str| {
            current
                .get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?")
                .to_string()
        };
        let cur_u64 = |key: &str| {
            current
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        println!("  group dir:     {}", cur_str("dir"));
        println!("  segments:      {}", cur_u64("segments"));
        println!(
            "  on_disk_bytes: {} ({})",
            cur_u64("on_disk_bytes"),
            format_bytes(cur_u64("on_disk_bytes"))
        );
    }
    println!("Reclaim live keep_small residue with `lastdb db reclaim-keep-small-snapshot`.");
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
