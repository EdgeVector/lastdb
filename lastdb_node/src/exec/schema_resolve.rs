//! Schema-name and alias resolution for owner-socket routes.

use std::collections::HashSet;

use fold_db::schema::types::Schema;
use fold_db::schema::{SchemaState, SchemaWithState};

use super::reply::HostError;
use crate::host::Host;

/// Resolve a caller-supplied schema name to its canonical runtime name.
pub(in crate::exec) fn resolve_schema_name(
    host: &Host,
    name: &str,
) -> Result<Option<String>, HostError> {
    let mgr = host.db.schema_manager();

    if mgr
        .get_schema_metadata(name)
        .map_err(|e| HostError::internal(e.to_string()))?
        .is_some()
    {
        return Ok(Some(name.to_string()));
    }

    let schemas = mgr
        .get_schemas_with_states()
        .map_err(|e| HostError::internal(e.to_string()))?;
    if let Some(canonical) = resolve_exact_alias(name, &schemas)? {
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
    resolve_folded_alias(trimmed, &schemas)
}

/// Resolve aliases against an already captured schema directory.
pub(in crate::exec) fn resolve_captured_schema_alias(
    name: &str,
    entries: &[SchemaWithState],
) -> Result<Option<String>, HostError> {
    if let Some(canonical) = resolve_exact_alias(name, entries)? {
        return Ok(Some(canonical));
    }
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    resolve_folded_alias(trimmed, entries)
}

fn resolve_exact_alias(
    requested: &str,
    schemas: &[SchemaWithState],
) -> Result<Option<String>, HostError> {
    if let Some(canonical) = resolve_matches(
        requested,
        schemas
            .iter()
            .filter(|entry| entry.schema.identity_hash.as_deref() == Some(requested)),
    )? {
        return Ok(Some(canonical));
    }
    if let Some((owner, local)) = requested.split_once('/') {
        if !owner.is_empty() && !local.is_empty() {
            let matches = schemas
                .iter()
                .filter(|entry| entry.schema.owner_app_id.as_deref() == Some(owner))
                .filter(|entry| {
                    entry.schema.name == local
                        || entry.schema.descriptive_name.as_deref() == Some(local)
                });
            if let Some(canonical) = resolve_matches(requested, matches)? {
                return Ok(Some(canonical));
            }
        }
    }
    resolve_matches(
        requested,
        schemas
            .iter()
            .filter(|entry| entry.schema.descriptive_name.as_deref() == Some(requested)),
    )
}

fn resolve_folded_alias(
    trimmed: &str,
    schemas: &[SchemaWithState],
) -> Result<Option<String>, HostError> {
    let folded = trimmed.to_lowercase();
    resolve_matches(
        trimmed,
        schemas.iter().filter(|entry| {
            entry
                .schema
                .descriptive_name
                .as_deref()
                .is_some_and(|value| value.to_lowercase() == folded)
        }),
    )
}

fn resolve_matches<'a>(
    requested: &str,
    matches: impl Iterator<Item = &'a SchemaWithState>,
) -> Result<Option<String>, HostError> {
    let mut blocked = false;
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for entry in matches {
        if entry.state == SchemaState::Blocked {
            blocked = true;
        } else if seen.insert(entry.schema.name.as_str()) {
            candidates.push(&entry.schema);
        }
    }
    let names: HashSet<&str> = candidates
        .iter()
        .map(|schema| schema.name.as_str())
        .collect();
    let superseded_bases: HashSet<&str> = candidates
        .iter()
        .filter_map(|schema| {
            let (_prefix, base) = schema.name.split_once(':')?;
            names.contains(base).then_some(base)
        })
        .collect();
    candidates.retain(|schema| !superseded_bases.contains(schema.name.as_str()));

    match candidates.len() {
        0 if blocked => Err(HostError::new(
            400,
            format!("Schema '{requested}' is blocked and cannot be queried or mutated"),
        )),
        0 => Ok(None),
        1 => Ok(Some(candidates[0].name.clone())),
        _ => {
            candidates.sort_by(|a, b| a.name.cmp(&b.name));
            let details = candidates
                .iter()
                .map(|schema| candidate_summary(schema))
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

fn candidate_summary(schema: &Schema) -> String {
    format!(
        "canonical={} descriptive_name={} owner_app_id={}",
        schema.name,
        schema.descriptive_name.as_deref().unwrap_or("<none>"),
        schema.owner_app_id.as_deref().unwrap_or("<none>"),
    )
}
