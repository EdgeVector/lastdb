//! Schema-name resolution shared by the query and mutation handlers.

use super::*;

mod read;
mod rules;
pub(in crate::handlers) use read::resolve_schema_name_for_read;

use rules::{resolve_exact_alias, resolve_folded_alias};

/// Resolve a caller-supplied schema name to its canonical runtime name:
/// exact canonical/runtime name, then exact `owner_app_id/name` or
/// `owner_app_id/descriptive_name`, then exact `descriptive_name`, then a
/// trimmed/case-folded lenient pass. `Ok(None)` means no such schema.
///
/// Host reads stay here; alias selection uses only the supplied candidates.
/// The captured read path uses the same rules with its own directory.
///
/// # Errors
/// Returns a [`HostError`] `500` when the schema manager read fails, `400` for
/// blocked aliases, and `409` for ambiguous aliases.
pub fn resolve_schema_name<H: HostNode>(host: &H, name: &str) -> Result<Option<String>, HostError> {
    let mgr = host.fold_db().schema_manager();

    // Canonical names resolve before retired claims are removed. Retiring an
    // alias must not make a by-hash pin unresolvable.
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
        .filter(|entry| !retired_claims.contains(&entry.schema.name))
        .collect();
    if let Some(canonical) = resolve_exact_alias(name, &schemas)? {
        return Ok(Some(canonical));
    }

    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    // This lookup follows exact aliases, so their errors retain precedence.
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
