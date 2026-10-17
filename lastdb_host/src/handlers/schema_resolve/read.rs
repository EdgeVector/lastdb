//! Native aliases resolve from the same captured view as their field values.

use super::*;

pub(in crate::handlers) async fn resolve_schema_name_for_read<H: HostNode>(
    host: &H,
    name: &str,
) -> Result<Option<String>, HostError> {
    let manager = host.fold_db().schema_manager();
    match manager
        .captured_name_exists(name)
        .await
        .map_err(|error| HostError::new(503, error.to_string()))?
    {
        None => return resolve_schema_name(host, name),
        Some(true) => return Ok(Some(name.to_string())),
        Some(false) => {}
    }
    let schemas = manager
        .captured_name_candidates()
        .await
        .map_err(|error| HostError::new(503, error.to_string()))?
        .ok_or_else(|| HostError::new(503, "captured schema directory is absent".to_string()))?;
    if let Some((owner, local)) = name.split_once('/') {
        if !owner.is_empty() && !local.is_empty() {
            let matches: Vec<_> = schemas
                .iter()
                .filter(|entry| entry.schema.owner_app_id.as_deref() == Some(owner))
                .filter(|entry| {
                    entry.schema.name == local
                        || entry.schema.descriptive_name.as_deref() == Some(local)
                })
                .cloned()
                .collect();
            if let Some(canonical) = resolve_descriptive_matches(name, &matches)? {
                return Ok(Some(canonical));
            }
        }
    }
    let exact: Vec<_> = schemas
        .iter()
        .filter(|entry| entry.schema.descriptive_name.as_deref() == Some(name))
        .cloned()
        .collect();
    if let Some(canonical) = resolve_descriptive_matches(name, &exact)? {
        return Ok(Some(canonical));
    }
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed != name
        && manager
            .captured_name_exists(trimmed)
            .await
            .map_err(|error| HostError::new(503, error.to_string()))?
            == Some(true)
    {
        return Ok(Some(trimmed.to_string()));
    }
    let folded = trimmed.to_lowercase();
    let matches: Vec<_> = schemas
        .iter()
        .filter(|entry| {
            entry
                .schema
                .descriptive_name
                .as_deref()
                .is_some_and(|value| value.to_lowercase() == folded)
        })
        .cloned()
        .collect();
    resolve_descriptive_matches(trimmed, &matches)
}
