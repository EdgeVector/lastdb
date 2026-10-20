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

/// Comma-joined `name` fields of a non-empty row list, or `None` when empty.
fn joined_names(planes: &Value, key: &str) -> Option<String> {
    let rows = array_of(planes, key).filter(|rows| !rows.is_empty())?;
    Some(
        rows.iter()
            .filter_map(|row| row.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

pub(crate) fn print_inventory_human(inventory: &Value) {
    println!("Live DB inventory (decrypted key+value sizes on running node)");
    println!(
        "main tree: {} keys, {}",
        u64_of(inventory, "main_total_keys"),
        format_bytes(u64_of(inventory, "main_total_bytes"))
    );
    if let Some(planes) = inventory.get("planes") {
        println!();
        print_collection_planes(planes);
    }
    println!();
    print_main_classes(inventory);
    println!();
    print_per_schema_atoms(inventory);
    println!();
    if let Some(counters) = inventory.get("atom_counters") {
        print_atom_counters(counters);
        println!();
    }
    print_per_schema_history(inventory);
    if let Some(scan) = inventory.get("scan") {
        println!();
        println!(
            "Range reads: {} calls, {} retried with fewer rows, {} narrowed (many deleted keys in a range)",
            u64_of(scan, "calls"),
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
    println!();
    print_history_tips();
}

fn print_collection_planes(planes: &Value) {
    println!(
        "Collection planes: {} collections, {}",
        u64_of(planes, "collection_count"),
        format_bytes(u64_of(planes, "total_bytes"))
    );
    if let Some(rows) = array_of(planes, "by_role") {
        for row in rows {
            let names = array_of(row, "collections")
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str())
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            println!(
                "  {:>10}  {:<16} [{}]",
                format_bytes(u64_of(row, "bytes")),
                str_of(row, "label", "?"),
                names
            );
        }
    }
    if let Some(names) = joined_names(planes, "unknown_active_collections") {
        println!("  UNKNOWN active planes: {names}");
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

fn print_per_schema_atoms(inventory: &Value) {
    println!("Per-schema atoms (logical JSON size):");
    if let Some(rows) = array_of(inventory, "per_schema_atoms") {
        for (i, row) in rows.iter().enumerate() {
            if i >= SCHEMA_ROW_LIMIT {
                println!(
                    "  … {} more schemas",
                    rows.len().saturating_sub(SCHEMA_ROW_LIMIT)
                );
                break;
            }
            println!(
                "  {:>10}  {:>8} atoms  {}",
                format_bytes(u64_of(row, "bytes")),
                u64_of(row, "atom_count"),
                str_of(row, "schema_name", "?")
            );
        }
        println!(
            "  total logical atom bytes: {}",
            format_bytes(u64_of(inventory, "per_schema_atoms_total_bytes"))
        );
    }
}

fn print_per_schema_history(inventory: &Value) {
    println!("Per-schema mutation history:");
    let Some(rows) = array_of(inventory, "per_schema_history") else {
        return;
    };
    if rows.is_empty() {
        println!("  (none)");
        return;
    }
    for (i, row) in rows.iter().enumerate() {
        if i >= SCHEMA_ROW_LIMIT {
            println!(
                "  … {} more schemas",
                rows.len().saturating_sub(SCHEMA_ROW_LIMIT)
            );
            break;
        }
        println!(
            "  {:>10}  {:>8} events  {}",
            format_bytes(u64_of(row, "history_bytes_approx")),
            u64_of(row, "history_events"),
            str_of(row, "schema_name", "?")
        );
    }
}

fn print_atom_counters(counters: &Value) {
    println!("Atom counters (atom:{{uuid}}\\0c):");
    println!("  counters:               {}", u64_of(counters, "counters"));
    println!(
        "  refs total:             {}",
        u64_of(counters, "refs_total")
    );
    println!("  largest count:          {}", u64_of(counters, "max_refs"));
    println!("  suspect:                {}", u64_of(counters, "suspect"));
    println!(
        "  zero, not suspect:      {}",
        u64_of(counters, "zero_counters")
    );
    println!(
        "  bodies without counter: {}",
        u64_of(counters, "bodies_without_counter")
    );
    println!(
        "  counters without body:  {}",
        u64_of(counters, "counters_without_body")
    );
    if let Some(histogram) = counters.get("refs_histogram") {
        println!(
            "  counters by count:      1: {}  2: {}  3-9: {}  10-99: {}  100+: {}",
            u64_of(histogram, "one"),
            u64_of(histogram, "two"),
            u64_of(histogram, "three_to_nine"),
            u64_of(histogram, "ten_to_ninety_nine"),
            u64_of(histogram, "hundred_or_more")
        );
    }
    if let Some(hottest) = array_of(counters, "hottest").filter(|rows| !rows.is_empty()) {
        println!("  most referenced atoms:");
        for hot in hottest {
            println!(
                "    {:>8} refs  {}",
                u64_of(hot, "refs"),
                str_of(hot, "atom", "?")
            );
        }
    }
}

fn print_history_tips() {
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
