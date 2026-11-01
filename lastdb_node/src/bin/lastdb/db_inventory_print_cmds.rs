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
