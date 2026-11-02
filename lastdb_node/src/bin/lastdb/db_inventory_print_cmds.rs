use super::*;

use serde_json::Value;

/// Rows shown for the per-schema tables before the "… N more" line.
const SCHEMA_ROW_LIMIT: usize = 25;

fn u64_of(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn str_of<'a>(value: &'a Value, key: &str, default: &'a str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or(default)
}

fn array_of<'a>(value: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    value.get(key).and_then(Value::as_array)
}

pub(crate) fn print_inventory_human(inventory: &Value) {
    println!("Live DB inventory (decrypted key+value sizes on running node)");
    println!(
        "main tree: {} keys, {}",
        u64_of(inventory, "main_total_keys"),
        format_bytes(u64_of(inventory, "main_total_bytes"))
    );
    println!();
    print_main_classes(inventory);
    println!();
    print_per_schema_records(inventory);
    if let Some(scan) = inventory.get("scan") {
        println!();
        println!(
            "Range reads: {} calls, {} went on above a run of deleted keys, {} retried with fewer rows, {} narrowed",
            u64_of(scan, "calls"),
            u64_of(scan, "skips"),
            u64_of(scan, "shrinks"),
            u64_of(scan, "splits")
        );
    }
    if let Some(notes) = array_of(inventory, "notes") {
        println!();
        println!("Notes:");
        for n in notes {
            if let Some(s) = n.as_str() {
                println!("  • {s}");
            }
        }
    }
}

fn print_main_classes(inventory: &Value) {
    println!("Main key classes (largest first):");
    if let Some(classes) = array_of(inventory, "main_classes") {
        for c in classes {
            println!(
                "  {:>10}  {:>8} keys  {}  — {}",
                format_bytes(u64_of(c, "bytes")),
                u64_of(c, "keys"),
                str_of(c, "class", "?"),
                str_of(c, "prefix", "")
            );
        }
    }
}

/// `per_schema_records` holds the live records of each schema.
fn print_per_schema_records(inventory: &Value) {
    println!("Per-schema records (stored value size):");
    if let Some(rows) = array_of(inventory, "per_schema_records") {
        for (i, row) in rows.iter().enumerate() {
            if i >= SCHEMA_ROW_LIMIT {
                println!(
                    "  … {} more schemas",
                    rows.len().saturating_sub(SCHEMA_ROW_LIMIT)
                );
                break;
            }
            println!(
                "  {:>10}  {:>8} records  {}",
                format_bytes(u64_of(row, "bytes")),
                u64_of(row, "record_count"),
                str_of(row, "schema_name", "?")
            );
        }
        println!(
            "  total record bytes: {}",
            format_bytes(u64_of(inventory, "per_schema_records_total_bytes"))
        );
    }
}
