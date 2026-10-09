//! Database inventory and schema storage reports.

use super::*;

pub(crate) fn db_inventory(
    home: &Path,
    socket: &Path,
    json_only: bool,
    out: Option<&Path>,
    timeout_secs: Option<u64>,
) -> Result<(), String> {
    // Full main-tree prefix scans on multi-GiB stores can take minutes and
    // burn cold-shard budget — treat as a heavy op, not a free status call.
    let deadline = inventory_client_timeout(timeout_secs);
    if !json_only {
        eprintln!(
            "lastdb db inventory: heavy full-store walk (client deadline {}s; \
             raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS)",
            deadline.as_secs().max(1)
        );
    }
    let req = format!(
        "GET /api/db/inventory HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), deadline)?;
    let value = parse_json_response(&response, "inventory")?;
    let mut inventory = value
        .get("inventory")
        .cloned()
        .ok_or_else(|| "response missing inventory field".to_string())?;
    let breakdown = fold_db::mini_cutover::plane_breakdown_for_store_root(&home.join("data"));
    let planes = breakdown.to_plane_map_report();
    if let Some(obj) = inventory.as_object_mut() {
        obj.insert(
            "planes".to_string(),
            serde_json::to_value(&planes).map_err(|e| format!("serialize planes: {e}"))?,
        );
    }

    let pretty = serde_json::to_string_pretty(&inventory)
        .map_err(|e| format!("serialize inventory: {e}"))?;

    if let Some(path) = out.as_ref() {
        // Atomic write: only rename into place after a full serialize so a
        // failed run never leaves a 0-byte product that looks like an empty store.
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !json_only {
            eprintln!("Wrote {}", path.display());
        }
    }

    if json_only {
        // Only emit complete JSON on success (exit 0). Prefer --out over shell
        // redirect so a client timeout cannot create a deceptive 0-byte file.
        println!("{pretty}");
        return Ok(());
    }

    print_inventory_human(&inventory);
    Ok(())
}

pub(crate) fn db_schemas(
    socket: &Path,
    json_only: bool,
    out: Option<&Path>,
    timeout_secs: Option<u64>,
    limit: usize,
) -> Result<(), String> {
    let deadline = inventory_client_timeout(timeout_secs);
    if !json_only {
        eprintln!(
            "lastdb db schemas: heavy atom walk (client deadline {}s; \
             raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS). \
             This is not a cheap status gauge.",
            deadline.as_secs().max(1)
        );
    }
    let req = format!(
        "GET /api/db/schemas HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), deadline)?;
    let value = parse_json_response(&response, "schema storage")?;
    let report = value
        .get("schema_storage")
        .cloned()
        .ok_or_else(|| "response missing schema_storage field".to_string())?;

    let pretty = serde_json::to_string_pretty(&report)
        .map_err(|e| format!("serialize schema storage: {e}"))?;

    if let Some(path) = out.as_ref() {
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !json_only {
            eprintln!("Wrote {}", path.display());
        }
    }

    if json_only {
        println!("{pretty}");
        return Ok(());
    }

    print_schema_storage_human(&report, limit);
    Ok(())
}

pub(crate) fn print_schema_storage_human(report: &serde_json::Value, limit: usize) {
    let measured = report
        .get("measured_at")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let total_bytes = report
        .get("total_logical_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let total_atoms = report
        .get("total_atoms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let schema_count = report
        .get("schema_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("Per-schema logical atom storage  measured_at={measured}  heavy=true");
    println!(
        "  {:>10}  {:>8} atoms  {} schemas",
        format_bytes(total_bytes),
        total_atoms,
        schema_count
    );
    println!();
    println!("  {:>10}  {:>8}  {:<32}  SCHEMA", "SIZE", "ATOMS", "NAME");
    let rows = report
        .get("per_schema")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let shown = if limit == 0 { rows.len() } else { limit };
    for (i, row) in rows.iter().enumerate() {
        if i >= shown {
            println!(
                "  … {} more schemas (pass --limit 0 for all)",
                rows.len() - shown
            );
            break;
        }
        let identity = row
            .get("schema_name")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let name = row
            .get("display_name")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let bytes = row
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let atoms = row
            .get("atom_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!(
            "  {:>10}  {:>8}  {:<32}  {}",
            format_bytes(bytes),
            atoms,
            truncate_label(name, 32),
            identity
        );
    }
}
