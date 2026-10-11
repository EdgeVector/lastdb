//! Human-readable report printers for `db repair-dangling-tips`.

pub(super) fn print_repair_scope(scope: Option<&serde_json::Value>) {
    if let Some(scope) = scope {
        let list = |key: &str| {
            scope
                .get(key)
                .and_then(serde_json::Value::as_array)
                .map(|v| {
                    v.iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        };
        println!("  scope: schema(s) {}", list("schemas"));
        if let Some(hash) = scope.get("hash_key").and_then(serde_json::Value::as_str) {
            println!("  scope: hash key {hash}");
        }
        println!(
            "  scope: {} molecule(s), {} key range(s); `completed` covers this scope only",
            scope
                .get("molecule_uuids")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len),
            scope
                .get("key_ranges")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
}

pub(super) fn print_repair_counters(report: &serde_json::Value) {
    for key in [
        "tips_scanned",
        "repairable_tips",
        "tips_repaired",
        "skipped_changed",
        "skipped_molecule_missing",
        "skipped_atom_not_in_molecule",
        "skipped_body_restored",
        "skipped_mis_derived",
        "skipped_unparseable_key",
        "skipped_unrepairable",
        "rescued_by_live_probe",
        "failed_repairs",
        "storage_keys_deleted",
    ] {
        println!(
            "  {key:24} {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    println!(
        "  completed                {}",
        report
            .get("completed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
}

pub(super) fn print_refused_molecules(report: &serde_json::Value) {
    // `skipped_unrepairable` is a size; this is what it sits on. Without it the
    // only record of which molecules were refused is a daemon-log warn line, so
    // an operator reading the report cannot tell bounded, explained residue from
    // an unexplained one.
    if let Some(refused) = report
        .get("refused_molecules")
        .and_then(serde_json::Value::as_array)
        .filter(|refused| !refused.is_empty())
    {
        println!();
        println!("  Refused molecules (left completely untouched):");
        for entry in refused {
            let field = |key: &str| {
                entry
                    .get(key)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            println!(
                "    {} — {} key(s) over the {}-byte limit (longest {}), {} tip(s) left in place",
                entry
                    .get("molecule_uuid")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("?"),
                field("keys_over_limit"),
                field("key_limit_bytes"),
                field("longest_key_bytes"),
                field("tips_left_in_place"),
            );
        }
        println!(
            "  These are correct refusals: rewriting would emit a key the store \
             rejects, and the repair is delete-then-store."
        );
    }
}

pub(super) fn print_unresolved_tips(report: &serde_json::Value, scoped: bool) {
    // `unresolved[].schema` joins each dangling molecule against the live
    // schema catalog. Grouped here so "which schema is producing these" is a
    // line count on the report itself, not a separate molecule-keys probe
    // per molecule uuid.
    if let Some(rows) = report
        .get("unresolved")
        .and_then(serde_json::Value::as_array)
        .filter(|rows| !rows.is_empty())
    {
        use std::collections::BTreeMap;
        let mut by_schema: BTreeMap<String, u64> = BTreeMap::new();
        for row in rows {
            let label = row
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("(unattributed)")
                .to_string();
            *by_schema.entry(label).or_insert(0) += 1;
        }
        println!();
        println!(
            "  Unresolved tips by schema (of {} detail row(s) captured, use --audit-limit to widen):",
            rows.len()
        );
        for (schema, count) in by_schema {
            println!("    {count:6}  {schema}");
        }
        // A scoped pass is small by construction, so name each key: that is
        // the list an operator checks before re-running with --execute.
        if scoped {
            println!();
            println!("  Repairable tips (tip key -> missing atom):");
            for row in rows {
                let field = |key: &str| {
                    row.get(key)
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?")
                        .to_string()
                };
                println!(
                    "    {} -> {}",
                    field("tip_key").escape_debug(),
                    field("atom_uuid")
                );
            }
        }
    }
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
