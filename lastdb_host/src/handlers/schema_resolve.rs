//! Schema-name resolution shared by the query and mutation handlers.

use super::*;

// ---------------------------------------------------------------------------
// Schema resolution (shared)
// ---------------------------------------------------------------------------

pub(super) fn collapse_redundant_schema_matches(matches: Vec<Schema>) -> Vec<Schema> {
    let mut seen = HashSet::new();
    let unique: Vec<Schema> = matches
        .into_iter()
        .filter(|schema| seen.insert(schema.name.clone()))
        .collect();

    let names: HashSet<&str> = unique.iter().map(|schema| schema.name.as_str()).collect();
    let superseded_bases: HashSet<String> = unique
        .iter()
        .filter_map(|schema| {
            let (_prefix, base) = schema.name.split_once(':')?;
            names.contains(base).then(|| base.to_string())
        })
        .collect();

    unique
        .into_iter()
        .filter(|schema| !superseded_bases.contains(&schema.name))
        .collect()
}

pub(super) fn candidate_summary(schema: &Schema) -> String {
    format!(
        "canonical={} descriptive_name={} owner_app_id={} source={:?}",
        schema.name,
        schema.descriptive_name.as_deref().unwrap_or("<none>"),
        schema.owner_app_id.as_deref().unwrap_or("<none>"),
        schema.source
    )
}

pub(super) fn resolve_descriptive_matches(
    requested: &str,
    matches: &[fold_db::schema::SchemaWithState],
) -> Result<Option<String>, HostError> {
    let active: Vec<Schema> = matches
        .iter()
        .filter(|entry| entry.state != SchemaState::Blocked)
        .map(|entry| entry.schema.clone())
        .collect();
    let mut candidates = collapse_redundant_schema_matches(active);

    match candidates.len() {
        0 => {
            if matches
                .iter()
                .any(|entry| entry.state == SchemaState::Blocked)
            {
                return Err(HostError::new(
                    400,
                    format!("Schema '{requested}' is blocked and cannot be queried or mutated"),
                ));
            }
            Ok(None)
        }
        1 => Ok(Some(candidates.pop().unwrap().name)),
        _ => {
            candidates.sort_by(|a, b| a.name.cmp(&b.name));
            let details = candidates
                .iter()
                .map(candidate_summary)
                .collect::<Vec<_>>()
                .join("; ");
            Err(HostError::new(
                409,
                format!(
                    "descriptive_name '{requested}' is ambiguous; pin the schema by canonical hash. Candidates: {details}. Example: lastdb query <canonical-hash> --fields <fields>"
                ),
            ))
        }
    }
}

/// Resolve a caller-supplied schema name to its canonical runtime name:
/// exact canonical/runtime name, then exact `owner_app_id/name` or
/// `owner_app_id/descriptive_name`, then exact `descriptive_name`, then a
/// trimmed/case-folded lenient pass. `Ok(None)` ⇒ no such schema (the caller
/// renders a `404`). This is the one resolution order both socket surfaces use.
///
/// # Errors
/// Returns a [`HostError`] `500` when the schema manager read fails.
pub fn resolve_schema_name<H: HostNode>(host: &H, name: &str) -> Result<Option<String>, HostError> {
    let mgr = host.fold_db().schema_manager();

    // Canonical name / identity hash resolves FIRST and unconditionally. A
    // retired name claim must never make a by-hash pin unresolvable — that is
    // the whole difference between retiring the claim and blocking the schema.
    if mgr
        .get_schema_metadata(name)
        .map_err(|e| HostError::internal(e.to_string()))?
        .is_some()
    {
        return Ok(Some(name.to_string()));
    }

    let retired_claims = mgr
        .retired_name_claims()
        .map_err(|e| HostError::internal(e.to_string()))?;
    let schemas: Vec<_> = mgr
        .get_schemas_with_states()
        .map_err(|e| HostError::internal(e.to_string()))?
        .into_iter()
        // A retired claimant stays installed, Available and readable by hash;
        // it just stops competing for the readable name. Dropping it here is
        // what lets `kanban doctor` raise a duplicate-name advisory to a red:
        // after the rekey path retires the predecessor, one name addresses one
        // Available schema again.
        .filter(|entry| !retired_claims.contains(&entry.schema.name))
        .collect();

    if let Some((owner_app_id, local_name)) = name.split_once('/') {
        if !owner_app_id.is_empty() && !local_name.is_empty() {
            let namespaced_matches: Vec<_> = schemas
                .iter()
                .filter(|entry| entry.schema.owner_app_id.as_deref() == Some(owner_app_id))
                .filter(|entry| {
                    entry.schema.name == local_name
                        || entry.schema.descriptive_name.as_deref() == Some(local_name)
                })
                .cloned()
                .collect();
            if let Some(canonical) = resolve_descriptive_matches(name, &namespaced_matches)? {
                return Ok(Some(canonical));
            }
        }
    }

    let exact_matches: Vec<_> = schemas
        .iter()
        .filter(|entry| entry.schema.descriptive_name.as_deref() == Some(name))
        .cloned()
        .collect();
    if let Some(canonical) = resolve_descriptive_matches(name, &exact_matches)? {
        return Ok(Some(canonical));
    }

    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed != name
        && mgr
            .get_schema_metadata(trimmed)
            .map_err(|e| HostError::internal(e.to_string()))?
            .is_some()
    {
        return Ok(Some(trimmed.to_string()));
    }
    let folded = trimmed.to_lowercase();
    let folded_matches: Vec<_> = schemas
        .iter()
        .filter(|entry| {
            entry
                .schema
                .descriptive_name
                .as_deref()
                .is_some_and(|d| d.to_lowercase() == folded)
        })
        .cloned()
        .collect();
    if let Some(canonical) = resolve_descriptive_matches(trimmed, &folded_matches)? {
        return Ok(Some(canonical));
    }
    Ok(None)
}
