//! Printing the retention the node reports for `lastdb app check`.

use serde_json::Value;

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn seconds(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn policy_in_force(retention: &Value, key: &str) -> Option<String> {
    let policy = retention.get(key).filter(|policy| !policy.is_null())?;
    let ttl = seconds(policy, "ttl_seconds")?;
    let source = text(policy, "source")?;
    Some(format!("{ttl}s source={source}"))
}

/// One line for a manifest TTL the manifest declares: what a read-only check
/// would apply, or what a sync did.
fn summary_line(retention: &Value) -> String {
    let ttl = seconds(retention, "ttl_seconds").unwrap_or(0);
    let status = text(retention, "status").unwrap_or("");
    let anchor = text(retention, "anchor")
        .map(|anchor| format!(" anchor={anchor}"))
        .unwrap_or_default();
    let change = text(retention, "change")
        .filter(|change| *change != "new" && *change != "unchanged")
        .map(|change| {
            let previous = seconds(retention, "previous_ttl_seconds")
                .map(|previous| format!(" from {previous}s"))
                .unwrap_or_default();
            format!(" ({change}{previous})")
        })
        .unwrap_or_default();
    if status == "operator_override" {
        let in_force = policy_in_force(retention, "effective").unwrap_or_default();
        return format!(
            "manifest ttl_seconds={ttl} is kept, but an operator policy ({in_force}) wins until `lastdb schema-retention clear`{anchor}"
        );
    }
    if !status.is_empty() {
        return format!("{status} ttl_seconds={ttl} source=manifest{change}{anchor}");
    }
    if retention
        .get("blocked")
        .is_some_and(|blocked| !blocked.is_null())
    {
        return format!("would be REFUSED: ttl_seconds={ttl} source=manifest{change}");
    }
    if retention.get("would_apply").and_then(Value::as_bool) == Some(false) {
        let in_force = policy_in_force(retention, "current").unwrap_or_default();
        return format!(
            "would NOT take effect: an operator policy ({in_force}) wins; manifest ttl_seconds={ttl} would be kept beside it"
        );
    }
    let current = policy_in_force(retention, "current")
        .map(|policy| format!(" (in force now: {policy})"))
        .unwrap_or_default();
    format!("would apply ttl_seconds={ttl} source=manifest{change}{current}")
}

/// Print the retention the node reported for one schema. The node sends the
/// numbers and the sentences; this prints them.
pub(crate) fn print_retention_text(retention: &Value) {
    if retention.get("declared").and_then(Value::as_bool) == Some(false) {
        println!("    retention: not declared in this manifest");
    } else {
        println!("    retention: {}", summary_line(retention));
        if let Some(rule) = text(retention, "age_rule") {
            println!("    retention: {rule}");
        }
    }
    if let Some(blocked) = text(retention, "blocked") {
        println!("    retention: REFUSED: {blocked}");
    }
    let shared = retention
        .get("shared_with")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    if !shared.is_empty() {
        println!("    retention: shared_with=[{}]", shared.join(", "));
    }
    let blocked = text(retention, "blocked").unwrap_or("");
    for key in ["notes", "warnings"] {
        let lines = retention
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            // The refusal above already says it.
            .filter(|line| !blocked.contains(line));
        for line in lines {
            let label = if key == "warnings" { "WARNING: " } else { "" };
            println!("    retention: {label}{line}");
        }
    }
}

/// Print the retention of every schema that reported one: what a promote or a
/// register-schemas run changed, which it would otherwise do in silence.
pub(crate) fn print_retention_changes(outcomes: &[lastdb_node::app_publish::CheckOutcome]) {
    for outcome in outcomes {
        if let Some(retention) = &outcome.retention {
            println!("  {}", outcome.schema_name);
            print_retention_text(retention);
        }
    }
}

/// The names in `names` whose manifest entry declares a `retention`.
fn names_with_retention<'a>(
    manifest: &lastdb_node::app_publish::AppManifest,
    names: &'a [String],
) -> Vec<&'a str> {
    names
        .iter()
        .filter(|name| {
            manifest.schemas.iter().any(|schema| {
                schema.get("name").and_then(Value::as_str) == Some(name.as_str())
                    && schema.get("retention").is_some()
            })
        })
        .map(String::as_str)
        .collect()
}

/// Print the manifest schemas that resolved to one catalog schema, and fail
/// when one of them declares a retention (or a schema is bound to a catalog
/// schema that carries a TTL): the TTL sweep deletes the records of all of
/// them.
pub(crate) fn collision_error(
    manifest: &lastdb_node::app_publish::AppManifest,
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Option<String> {
    let mut hazards = Vec::new();
    for (catalog, names) in lastdb_node::app_publish::catalog_collisions(outcomes) {
        if !names_with_retention(manifest, &names).is_empty() {
            hazards.push(format!(
                "[{}] share the catalog schema {catalog}",
                names.join(", ")
            ));
        }
    }
    for outcome in outcomes {
        let status = outcome
            .retention
            .as_ref()
            .and_then(|retention| text(retention, "status"));
        if status == Some("shares_retained_schema") {
            hazards.push(format!(
                "{} is bound to a catalog schema that carries a TTL",
                outcome.schema_name
            ));
        }
    }
    (!hazards.is_empty()).then(|| {
        format!(
            "a retention would delete records it should not: {}. The TTL sweep deletes the records \
             of every schema bound to the catalog schema. Give each schema its own shape, or \
             remove the `retention`",
            hazards.join("; ")
        )
    })
}

/// The schemas whose coverage failed because the node refused their manifest
/// retention (a shared catalog schema, a confirmation that is missing, a TTL
/// below the floor), or `None` when no schema failed for that reason.
pub(crate) fn retention_refusals(
    outcomes: &[lastdb_node::app_publish::CheckOutcome],
) -> Option<String> {
    use lastdb_node::app_publish::SchemaCoverage;
    let names: Vec<&str> = outcomes
        .iter()
        .filter(|outcome| {
            matches!(&outcome.coverage, SchemaCoverage::Novel { reason }
                if reason.contains("TTL") || reason.contains("retention"))
        })
        .map(|outcome| outcome.schema_name.as_str())
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}
