use super::*;

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn print_inventory_human(inventory: &serde_json::Value) {
    let main_total = inventory
        .get("main_total_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let main_keys = inventory
        .get("main_total_keys")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("Live DB inventory (decrypted key+value sizes on running node)");
    println!(
        "main tree: {} keys, {}",
        main_keys,
        format_bytes(main_total)
    );
    if let Some(planes) = inventory.get("planes") {
        println!();
        let collection_count = planes
            .get("collection_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let total_bytes = planes
            .get("total_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!(
            "Collection planes: {} collections, {}",
            collection_count,
            format_bytes(total_bytes)
        );
        if let Some(rows) = planes.get("by_role").and_then(|v| v.as_array()) {
            for row in rows {
                let label = row.get("label").and_then(|v| v.as_str()).unwrap_or("?");
                let bytes = row
                    .get("bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let names = row
                    .get("collections")
                    .and_then(|v| v.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str())
                            .take(4)
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                println!("  {:>10}  {:<16} [{}]", format_bytes(bytes), label, names);
            }
        }
        if let Some(rows) = planes
            .get("legacy_split_named")
            .and_then(|v| v.as_array())
            .filter(|rows| !rows.is_empty())
        {
            let names = rows
                .iter()
                .filter_map(|row| row.get("name").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  legacy splits: {names}");
        }
        if let Some(rows) = planes
            .get("unknown_active_collections")
            .and_then(|v| v.as_array())
            .filter(|rows| !rows.is_empty())
        {
            let names = rows
                .iter()
                .filter_map(|row| row.get("name").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  UNKNOWN active planes: {names}");
        }
    }
    println!();
    println!("Main key classes (largest first):");
    if let Some(classes) = inventory.get("main_classes").and_then(|v| v.as_array()) {
        for c in classes {
            let class = c.get("class").and_then(|v| v.as_str()).unwrap_or("?");
            let keys = c
                .get("keys")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let bytes = c
                .get("bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let prefix = c.get("prefix").and_then(|v| v.as_str()).unwrap_or("");
            println!(
                "  {:>10}  {:>8} keys  {}  — {}",
                format_bytes(bytes),
                keys,
                class,
                prefix
            );
        }
    }
    println!();
    println!("Per-schema atoms (logical JSON size):");
    if let Some(rows) = inventory.get("per_schema_atoms").and_then(|v| v.as_array()) {
        for (i, row) in rows.iter().enumerate() {
            if i >= 25 {
                println!("  … {} more schemas", rows.len().saturating_sub(25));
                break;
            }
            let name = row
                .get("schema_name")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let bytes = row
                .get("bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let count = row
                .get("atom_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            println!(
                "  {:>10}  {:>8} atoms  {}",
                format_bytes(bytes),
                count,
                name
            );
        }
        let total = inventory
            .get("per_schema_atoms_total_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!("  total logical atom bytes: {}", format_bytes(total));
    }
    println!();
    println!("Per-schema mutation history:");
    if let Some(rows) = inventory
        .get("per_schema_history")
        .and_then(|v| v.as_array())
    {
        if rows.is_empty() {
            println!("  (none)");
        } else {
            for (i, row) in rows.iter().enumerate() {
                if i >= 25 {
                    println!("  … {} more schemas", rows.len().saturating_sub(25));
                    break;
                }
                let name = row
                    .get("schema_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let events = row
                    .get("history_events")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let bytes = row
                    .get("history_bytes_approx")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                println!(
                    "  {:>10}  {:>8} events  {}",
                    format_bytes(bytes),
                    events,
                    name
                );
            }
        }
    }
    if let Some(tf) = inventory.get("tip_format") {
        println!();
        println!("Tip format (mk: values):");
        let scanned = tf
            .get("tips_scanned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let thin = tf
            .get("tips_thin")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let fat = tf
            .get("tips_fat")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let unreadable = tf
            .get("tips_unreadable")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let fat_bytes = tf
            .get("fat_bytes_approx")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let thin_bytes = tf
            .get("thin_bytes_approx")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!("  scanned:     {scanned}");
        println!("  thin:        {thin}  ({})", format_bytes(thin_bytes));
        println!("  fat (legacy):{fat}  ({})", format_bytes(fat_bytes));
        if unreadable > 0 {
            println!("  unreadable:  {unreadable}");
        }
        if fat > 0 {
            println!();
            println!("  → migrate: lastdb db migrate-thin-tips            # dry-run");
            println!("             lastdb db migrate-thin-tips --execute  # rewrite in place");
            println!("  → rekey:   lastdb db rekey-atom-partition-prefix  # dual-write dry-run");
            println!("             lastdb db rekey-atom-partition-prefix --execute");
        }
    }
    if let Some(notes) = inventory.get("notes").and_then(|v| v.as_array()) {
        println!();
        println!("Notes:");
        for n in notes {
            if let Some(s) = n.as_str() {
                println!("  • {s}");
            }
        }
    }
    println!();
    println!("Tip: clear long history with:");
    println!("  lastdb db clear-history                         # dry-run keep_last=1");
    println!("  lastdb db clear-history --execute               # keep newest 1 per field key");
    println!("  lastdb db clear-history --keep-last 0           # dry-run full history purge");
    println!("  lastdb db clear-history --keep-last 0 --execute # purge all history (tip = only version)");
    println!("  lastdb db compact --collection schemas          # dry-run reclaim preview");
    println!("  lastdb db compact --collection schemas --execute # rewrite live + drop dead segs");
    println!("  lastdb db compact --collection tips             # dry-run purged-tip reclaim");
    println!("  lastdb db compact --collection tips --execute    # reclaim purged tip bytes");
    println!("  lastdb db stamp-purged-atom-retirements         # dry-run; keep-set copy of disk");
    println!(
        "  lastdb db stamp-purged-atom-retirements --execute # stamp extras; not compact --execute"
    );
}

/// One line after a `compact --all` walk, so a plane left out of it (the
/// owner must name `cas_blobs`) is not read as a gap in the report.
pub(crate) fn print_named_only_note(report: &serde_json::Value) {
    let Some(names) = report
        .get("named_only_not_walked")
        .and_then(|v| v.as_array())
    else {
        return;
    };
    let names: Vec<&str> = names.iter().filter_map(|n| n.as_str()).collect();
    println!("Not walked (name with --collection): {}", names.join(", "));
}
