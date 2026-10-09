//! Cloud prefix inventory, backup concurrency and LastStore snapshot commands. Moved verbatim from `cloud_cmds.rs`.

use super::*;

/// Live, read-only R2 prefix/category size inventory over the owner socket.
///
/// Uses the admin-scan client deadline because a large inventory can exceed
/// the generic `post_json` 10s budget. GC admission and status remain short.
pub(crate) fn cloud_prefix_inventory(home: &Path, json_only: bool) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first",
            socket.display()
        ));
    }
    let response = post_json_admin(
        &socket,
        "/api/sync/prefix-inventory",
        &serde_json::json!({}),
    )?;
    let value = parse_json_response(&response, "prefix-inventory")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }
    let report = value
        .get("report")
        .or_else(|| value.pointer("/data/report"))
        .unwrap_or(&value);
    println!("Cloud prefix inventory:");
    if let Some(entries) = report.get("entries").and_then(|e| e.as_array()) {
        for entry in entries {
            let category = entry
                .get("category")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let count = entry
                .get("object_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let bytes = entry
                .get("total_bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            println!("  {category:<18} objects={count:<10} bytes={bytes}");
        }
    }
    println!(
        "  {:<18} objects={:<10} bytes={}",
        "TOTAL",
        report
            .get("total_objects")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        report
            .get("total_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    if let Some(sample) = report
        .get("unclassified_sample")
        .and_then(|v| v.as_array())
        .filter(|s| !s.is_empty())
    {
        println!("  unclassified sample keys (not attributed to a known category):");
        for key in sample {
            if let Some(key) = key.as_str() {
                println!("    {key}");
            }
        }
    }
    // State the coverage. The storage Lambda routes a list to R2 or B2 by the
    // prefix it is handed, so a report that lists one prefix is a subtotal,
    // not a scope. Print what was listed so a reading cannot be mistaken for
    // more than it covers.
    if let Some(prefixes) = report
        .get("listed_prefixes")
        .and_then(|v| v.as_array())
        .filter(|p| !p.is_empty())
    {
        let names: Vec<String> = prefixes
            .iter()
            .filter_map(|p| p.as_str())
            .map(|p| {
                if p.is_empty() {
                    "<scope root>".to_string()
                } else {
                    p.to_string()
                }
            })
            .collect();
        println!("  listed prefixes ({}): {}", names.len(), names.join(" "));
    }
    Ok(())
}

/// Read, set, or clear live sealed-home backup PUT concurrency over the owner
/// socket. The daemon keeps its held cut and applies the value to subsequent
/// scheduling decisions.
pub(crate) fn cloud_backup_concurrency(
    home: &Path,
    value: Option<usize>,
    clear: bool,
    json_only: bool,
) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first",
            socket.display()
        ));
    }
    let (action, value) = if clear {
        ("clear", None)
    } else if let Some(value) = value {
        ("set", Some(value))
    } else {
        ("get", None)
    };
    let response = post_json(
        &socket,
        "/api/sync/backup-concurrency",
        &serde_json::json!({ "action": action, "value": value }),
    )?;
    let response = parse_json_response(&response, "backup-concurrency")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response).unwrap_or_else(|_| response.to_string())
        );
        return Ok(());
    }
    let status = response
        .pointer("/data/backup_upload_concurrency")
        .or_else(|| response.get("backup_upload_concurrency"))
        .unwrap_or(&response);
    println!("Backup upload concurrency:");
    println!(
        "  effective_override: {}",
        status
            .get("effective_override")
            .map_or_else(|| "null".to_string(), serde_json::Value::to_string)
    );
    println!(
        "  effective_source:   {}",
        status
            .get("effective_source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
    );
    println!(
        "  runtime_override:   {}",
        status
            .get("runtime_override")
            .map_or_else(|| "null".to_string(), serde_json::Value::to_string)
    );
    Ok(())
}

/// Live LastStore cloud backup snapshot over the owner socket.
pub(crate) fn cloud_laststore_snapshot(home: &Path, json_only: bool) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first",
            socket.display()
        ));
    }
    let response = post_json_admin(
        &socket,
        "/api/sync/laststore-snapshot",
        &serde_json::json!({}),
    )?;
    let value = parse_json_response(&response, "laststore-snapshot")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }

    let report = value
        .get("report")
        .or_else(|| value.pointer("/data/report"))
        .ok_or_else(|| "laststore-snapshot response missing report".to_string())?;
    println!("LastStore cloud snapshot complete:");
    println!(
        "  manifest_sha256: {}",
        report
            .get("manifest_sha256")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
    );
    println!(
        "  counter: {}",
        report
            .get("counter")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  cut_csn: {}",
        report
            .get("cut_csn")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  chunks: {} referenced, {} uploaded, {} already present",
        report
            .get("chunks_referenced")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        report
            .get("chunks_uploaded")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        report
            .get("chunks_already_present")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    Ok(())
}
