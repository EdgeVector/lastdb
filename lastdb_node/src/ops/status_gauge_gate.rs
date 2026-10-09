//! Durable gate for the status gauge contract (design PR-4).
//!
//! ## What this gate enforces
//!
//! 1. **Must-be-Gauge inventory** — every field listed in [`MUST_BE_GAUGE`] is
//!    declared as `Gauge` in `self_metrics.rs`. Those entries lock in the
//!    PR-2 / PR-3 conversions so a regression cannot silently re-bare them.
//! 2. **Hard-coded unit nouns** — `*_status_line` renderers must not embed
//!    closed-set unit nouns (`row(s)`, `edge(s)`, …) as string literals. Nouns
//!    come from [`crate::ops::gauge::Unit::noun`] / `Gauge` `Display`.
//!
//! ## BOUNDARY (won't-undo — operator-visible gauges only)
//!
//! This gate is **not** "every integer on `/api/status` must be a `Gauge`".
//! Out of scope by design:
//!
//! - timestamps / ages (`sampled_at`, `*_unix`, `*_secs` clocks)
//! - path disclosure flags and string paths
//! - config ceilings and sample-retention knobs still bare by product choice
//! - not-yet-converted operator counters (e.g. many `ResidentHealth` hit
//!   totals, Sync/BackupProgress bookkeeping still on bare integers)
//! - internal counters outside the status Health surface (`request_ops`,
//!   purge maps, ring buffers)
//!
//! When a bare field is converted to `Gauge`, **add it to [`MUST_BE_GAUGE`]**
//! in the same PR. Expanding the gate to "all integers" without an explicit
//! allowlist is how this lint gets ignored — do not do that here.
//!
//! Fault injection tests prove the auditors fail closed on planted violations
//! (vacuous-green control; brain checkpoint on vacuous guards).
//!
//! Ground truth: brain `design-lastdb-status-gauge-contract` (PR-4).

/// Operator-visible fields that **must** remain typed as [`crate::ops::gauge::Gauge`].
///
/// Format: `(StructName, field_name)`. Generated from the converted Health
/// surface on main after PR-2/PR-3; keep in lockstep with those structs.
const MUST_BE_GAUGE: &[(&str, &str)] = &[
    // QosHealth
    ("QosHealth", "total_permits"),
    ("QosHealth", "bulk_permits"),
    ("QosHealth", "total_in_use"),
    ("QosHealth", "bulk_in_use"),
    ("QosHealth", "interactive_sheds"),
    ("QosHealth", "bulk_sheds"),
    // UdsPoolHealth
    ("UdsPoolHealth", "workers"),
    ("UdsPoolHealth", "queue_capacity"),
    ("UdsPoolHealth", "in_flight"),
    ("UdsPoolHealth", "submitted"),
    ("UdsPoolHealth", "queue_full_rejects"),
    // WatchersHealth
    ("WatchersHealth", "max"),
    ("WatchersHealth", "active"),
    ("WatchersHealth", "peak"),
    ("WatchersHealth", "sheds"),
    // MoleculeGateHealth (PR-2)
    ("MoleculeGateHealth", "hold_total_us"),
    ("MoleculeGateHealth", "hold_count"),
    ("MoleculeGateHealth", "hold_max_us"),
    // ResidentHealth (partial — deferred-persist plus logical-set key gauges)
    ("ResidentHealth", "deferred_persist_completed"),
    ("ResidentHealth", "deferred_persist_us"),
    ("ResidentHealth", "resident_key_count"),
    ("ResidentHealth", "resident_held_keys"),
    ("ResidentHealth", "resident_dirty_keys"),
    ("ResidentHealth", "resident_key_budget"),
    ("ResidentHealth", "resident_purged_keys"),
    ("ResidentHealth", "loader_pin_bytes"),
    ("ResidentHealth", "loader_groups_open_now"),
    ("ResidentHealth", "resident_point_hits"),
    ("ResidentHealth", "resident_point_misses"),
    ("ResidentHealth", "resident_purge_runs"),
    ("ResidentHealth", "resident_over_cap_stalls"),
    ("ResidentHealth", "resident_over_cap_keys"),
    ("ResidentHealth", "loader_loads"),
    ("ResidentHealth", "loader_load_us"),
    // IntegrityHealth (PR-2)
    ("IntegrityHealth", "unresolved_atom_skips"),
    ("IntegrityHealth", "unresolved_atom_distinct"),
    ("IntegrityHealth", "unresolved_atom_rows"),
    // AtomRefEdgeHealth — v1/v2 cutover and audit gauges
    ("AtomRefEdgeHealth", "v1_bytes"),
    ("AtomRefEdgeHealth", "v2_bytes"),
    ("AtomRefEdgeHealth", "active_edges"),
    ("AtomRefEdgeHealth", "inactive_keys"),
    ("AtomRefEdgeHealth", "bytes_per_edge"),
    ("AtomRefEdgeHealth", "projected_final_bytes"),
    // FileBlobHealth — the file-blob plane's counterpart to IntegrityHealth
    ("FileBlobHealth", "absent_with_memo"),
    ("FileBlobHealth", "absent_with_memo_distinct"),
    ("FileBlobHealth", "absent_without_memo"),
    // LocatorOnlyHealth
    ("LocatorOnlyHealth", "tips_sampled"),
    ("LocatorOnlyHealth", "max_tips"),
    ("LocatorOnlyHealth", "locator_only"),
    ("LocatorOnlyHealth", "body_at_derived_or_flat"),
    ("LocatorOnlyHealth", "dangling"),
    ("LocatorOnlyHealth", "other_unresolved"),
    ("LocatorOnlyHealth", "locator_only_per_mille"),
    ("LocatorOnlyHealth", "dangling_per_mille"),
    ("LocatorOnlyHealth", "prev_dangling"),
    ("LocatorOnlyHealth", "dangling_recurrence_per_hour"),
    // DualReadHealth
    ("DualReadHealth", "gets"),
    ("DualReadHealth", "target_hits"),
    ("DualReadHealth", "legacy_hits"),
    ("DualReadHealth", "by_design_hits"),
    ("DualReadHealth", "misses"),
    ("DualReadPlaneHitHealth", "legacy_hits"),
    // ReadCostHealth
    ("ReadCostHealth", "cold_shard_loads"),
    ("ReadCostHealth", "warm_resident_groups"),
    ("ReadCostHealth", "warm_resident_bytes"),
    ("ReadCostHealth", "warm_budget_bytes"),
    ("ReadCostHealth", "warm_budget_handles"),
    ("ReadCostHealth", "open_append_handles"),
    ("ReadCostHealth", "torn_transaction_rollbacks"),
    ("ReadCostHealth", "torn_transaction_rollback_failures"),
    ("ReadCostHealth", "transaction_residency_refresh_failures"),
    // LimitsHealth
    ("LimitsHealth", "max_request_body_bytes"),
    ("LimitsHealth", "max_atom_content_bytes"),
    ("LimitsHealth", "max_atom_content_bytes_default"),
    ("LimitsHealth", "max_atom_content_bytes_absolute_max"),
    // BackupStorageHealth
    ("BackupStorageHealth", "referenced_bytes"),
    ("BackupStorageHealth", "billed_bytes"),
    ("BackupStorageHealth", "reclaimable_bytes"),
    ("BackupStorageHealth", "referenced_chunks"),
    ("BackupStorageHealth", "billed_chunks"),
    ("BackupStorageHealth", "reclaimable_chunks"),
];

/// Closed-set unit nouns that must come from `Unit::noun()`, never string
/// literals in `*_status_line` bodies.
const FORBIDDEN_HARDCODED_UNIT_NOUNS: &[&str] = &[
    "row(s)", "edge(s)", "key(s)", "chunk(s)",
    "event(s)",
    // bare "byte(s)" / "µs" / "hold(s)" appear in Named units and prose; we
    // only lock the historical mislabel class that PR-2 proved.
];

/// Parse `pub struct Name { ... }` field declarations from a Rust source
/// fragment. Best-effort line scan (not a full rustc parser): good enough for
/// the health-surface inventory and deliberately simple so the gate itself
/// stays reviewable.
fn parse_pub_struct_fields(src: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    let mut brace_depth: i32 = 0;
    let mut in_struct = false;

    for line in src.lines() {
        let trimmed = line.trim();
        // Skip pure comments.
        if trimmed.starts_with("//") {
            continue;
        }

        if !in_struct {
            if let Some(rest) = trimmed.strip_prefix("pub struct ") {
                let name = rest.split([' ', '<', '{']).next().unwrap_or("").trim();
                if name.is_empty() {
                    continue;
                }
                current = Some(name.to_string());
                in_struct = true;
                brace_depth = 0;
                // Count braces on the same line.
                for c in line.chars() {
                    if c == '{' {
                        brace_depth += 1;
                    } else if c == '}' {
                        brace_depth -= 1;
                    }
                }
                if brace_depth == 0 && line.contains('{') && line.contains('}') {
                    // empty one-liner struct
                    in_struct = false;
                    current = None;
                }
                continue;
            }
            continue;
        }

        for c in line.chars() {
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
            }
        }

        if let Some(struct_name) = current.as_ref() {
            // `pub field: Type,` — tolerate trailing attributes on prior lines.
            if let Some(after_pub) = trimmed.strip_prefix("pub ") {
                if let Some((name, ty)) = after_pub.split_once(':') {
                    let name = name.trim();
                    // Skip methods (`pub fn`) and visibility-only noise.
                    if !name.is_empty()
                        && !name.starts_with("fn ")
                        && !name.starts_with("const ")
                        && !name.starts_with("async ")
                        && !name.contains('(')
                    {
                        let ty = ty.trim().trim_end_matches(',').trim().to_string();
                        if !ty.is_empty() {
                            out.push((struct_name.clone(), name.to_string(), ty));
                        }
                    }
                }
            }
        }

        if in_struct && brace_depth <= 0 {
            in_struct = false;
            current = None;
            brace_depth = 0;
        }
    }
    out
}

/// Fail when a [`MUST_BE_GAUGE`] field is missing or not typed as `Gauge`.
pub fn audit_must_be_gauge(src: &str) -> Result<(), Vec<String>> {
    let fields = parse_pub_struct_fields(src);
    let mut index = std::collections::HashMap::<(String, String), String>::new();
    for (s, f, t) in fields {
        index.insert((s, f), t);
    }

    let mut errors = Vec::new();
    for &(struct_name, field_name) in MUST_BE_GAUGE {
        let key = (struct_name.to_string(), field_name.to_string());
        match index.get(&key) {
            None => errors.push(format!(
                "MUST_BE_GAUGE {struct_name}.{field_name}: field not found in source \
                 (renamed? remove from inventory only after the conversion is gone)"
            )),
            Some(ty) if is_gauge_type(ty) => {}
            Some(ty) => errors.push(format!(
                "MUST_BE_GAUGE {struct_name}.{field_name}: expected Gauge, found `{ty}` \
                 (operator-visible status field re-bared — convert back to Gauge or \
                 deliberately drop from MUST_BE_GAUGE with a design note)"
            )),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn is_gauge_type(ty: &str) -> bool {
    // Accept `Gauge`, `crate::ops::gauge::Gauge`, `super::gauge::Gauge`, etc.
    let core = ty.split("::").last().unwrap_or(ty).trim();
    core == "Gauge"
}

/// Extract approximate bodies of `fn …_status_line…` for unit-noun scanning.
fn status_line_fn_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let lines: Vec<&str> = src.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        // Match `fn foo_status_line` / `pub fn …` / `fn foo_status_line_with_…`
        let is_status_line = trimmed.contains("status_line")
            && (trimmed.starts_with("fn ")
                || trimmed.starts_with("pub fn ")
                || trimmed.starts_with("pub(crate) fn "));
        if !is_status_line {
            i += 1;
            continue;
        }
        // Skip tests of status lines (fn names often end with `_status_line` too).
        // We still want production renderers; test helpers live under `mod tests`.
        let name = trimmed
            .trim_start_matches("pub(crate) ")
            .trim_start_matches("pub ")
            .trim_start_matches("fn ")
            .split('(')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        if name.is_empty() {
            i += 1;
            continue;
        }

        // Collect until brace balance returns to zero after the opening `{`.
        let mut body = String::new();
        let mut depth = 0i32;
        let mut started = false;
        let mut j = i;
        while j < lines.len() {
            let line = lines[j];
            for c in line.chars() {
                if c == '{' {
                    depth += 1;
                    started = true;
                } else if c == '}' {
                    depth -= 1;
                }
            }
            body.push_str(line);
            body.push('\n');
            j += 1;
            if started && depth <= 0 {
                break;
            }
        }
        out.push((name, body));
        i = j;
    }
    out
}

/// Fail when a `*_status_line` function hard-codes a closed-set unit noun.
pub fn audit_status_line_hardcoded_units(src: &str) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    for (name, body) in status_line_fn_bodies(src) {
        // Only production renderers — skip pure test/helper names.
        if name.contains("pre_contract")
            || name.starts_with("test_")
            || name.ends_with("_reports_")
            || name.contains("_keeps_")
            || name.contains("_surfaces_")
            || name.contains("_uses_")
            || name.contains("_names_")
            || name.contains("_is_still_")
            || name.starts_with("regression_")
            || name.contains("_accepts_")
            || name.contains("_warns_")
        {
            continue;
        }
        // Only functions whose name is exactly `*_status_line` (or with a short
        // suffix like `_status_line_for_…` if added later). Require the stem.
        if !name.contains("status_line") {
            continue;
        }
        // Prefer production path: functions defined outside `mod tests` — we
        // approximate by requiring they take a health/`StatusSnapshot` arg or
        // are free of assert!/expect (tests usually have those).
        if body.contains("assert!") || body.contains("assert_eq!") || body.contains("assert_ne!") {
            continue;
        }

        for noun in FORBIDDEN_HARDCODED_UNIT_NOUNS {
            // Look for the noun inside a string literal.
            let patterns = [
                format!("\"{noun}\""),
                format!("\"{noun} "),
                format!(" {noun}\""),
                format!(" {noun} "),
                format!("{{{noun}}}"), // unlikely
            ];
            // Also catch format fragments like `{edges} distinct row(s)`.
            let embedded = noun.to_string();
            let has_literal = body.lines().any(|line| {
                let t = line.trim();
                if t.starts_with("//") {
                    return false;
                }
                // String-ish line containing the noun between quotes somewhere.
                if !line.contains(noun) {
                    return false;
                }
                // Must appear inside a double-quoted segment on the line.
                quoted_contains(line, noun)
                    || patterns.iter().any(|p| line.contains(p.as_str()))
                    || (line.contains('"') && line.contains(&embedded))
            });
            if has_literal {
                // Allow if the line also derives the noun via `.unit.noun()` —
                // but a hard-coded string is still a hard-code. Always fail.
                errors.push(format!(
                    "{name}: hard-coded unit noun `{noun}` — use Gauge/Unit::noun() \
                     (typed unit) instead of a string literal"
                ));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// True when `needle` appears inside at least one `"…"` segment of `line`.
fn quoted_contains(line: &str, needle: &str) -> bool {
    let mut in_str = false;
    let mut escaped = false;
    let mut cur = String::new();
    for c in line.chars() {
        if !in_str {
            if c == '"' {
                in_str = true;
                cur.clear();
            }
            continue;
        }
        if escaped {
            cur.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '"' => {
                if cur.contains(needle) {
                    return true;
                }
                in_str = false;
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    false
}

/// Full contract audit used by the CI test.
pub fn audit_status_gauge_contract(src: &str) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    if let Err(mut e) = audit_must_be_gauge(src) {
        errors.append(&mut e);
    }
    if let Err(mut e) = audit_status_line_hardcoded_units(src) {
        errors.append(&mut e);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

// lint:file-size-ok moved verbatim from the self_metrics.rs include list; cohesive unit, split further in a later pass
