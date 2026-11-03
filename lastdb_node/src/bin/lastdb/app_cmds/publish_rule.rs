//! The publishing rule (decision A8 of
//! `decision-2026-11-02-advisory-schema-service-types-and-manifests`): an app
//! on the registry lists every schema it declares, with every field typed.
//! `lastdb app publish` checks it before it signs anything. A local app that is
//! never published needs none of this.

use super::*;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::exec::normalize_app_manifest_schema;
use serde_json::Value;

/// The schema list read is the light default projection (no record counts, no
/// per-field metadata), so a short deadline is enough.
const NODE_SCHEMAS_TIMEOUT: Duration = Duration::from_secs(30);

/// One manifest schema as the node installs it when the app declares it.
pub(super) struct ListedSchema {
    name: String,
    descriptive_name: Option<String>,
    identity_hash: String,
}

/// One schema the running node holds for the app.
struct HeldSchema {
    name: String,
    descriptive_name: Option<String>,
    identity_hash: Option<String>,
}

/// Every field of every schema the manifest lists names a type
/// (`decision-2026-11-02-field-types-no-any`). The node refuses the same
/// declaration, so publish refuses it first and names the fields.
fn require_typed_fields(schemas: &[Value]) -> Result<(), String> {
    let untyped: Vec<String> = schemas
        .iter()
        .flat_map(|schema| {
            let name = schema.get("name").and_then(Value::as_str).unwrap_or("?");
            let typed = schema.get("field_types").and_then(Value::as_object);
            schema
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(move |field| !typed.is_some_and(|map| map.contains_key(*field)))
                .map(move |field| format!("{name}.{field}"))
        })
        .collect();
    if untyped.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "every schema field needs a type in field_types; these fields have none: {}",
            untyped.join(", ")
        ))
    }
}

/// One manifest entry, normalized by the declare route's own code with
/// namespace = `app_id`. `retention` is node-local policy that never reaches
/// the declared schema, so it is dropped first, as `lastdb app check` does.
fn listed_schema(app_id: &str, entry: &Value) -> Result<ListedSchema, String> {
    let label = entry
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>");
    let mut entry = entry.clone();
    if let Some(object) = entry.as_object_mut() {
        object.remove("retention");
    }
    let schema: DeclarativeSchemaDefinition = serde_json::from_value(entry)
        .map_err(|e| format!("manifest schema '{label}' is not a schema declaration: {e}"))?;
    let schema = normalize_app_manifest_schema(app_id, schema)
        .map_err(|e| format!("manifest schema '{label}' cannot be declared: {e}"))?;
    let identity_hash = schema
        .identity_hash
        .clone()
        .ok_or_else(|| format!("manifest schema '{label}' has no identity hash"))?;
    Ok(ListedSchema {
        name: schema.name.clone(),
        descriptive_name: schema.descriptive_name.clone(),
        identity_hash,
    })
}

/// Checks 1 and 2 of the rule, and the identity of each listed schema for
/// check 3.
///
/// Check 1 requires `schemas` to be present, not to be non-empty. Some registry
/// apps own no schema (LastStack, LastDB Browser), and a dummy schema would put
/// a false declaration in the catalog. An explicit empty list passes only when
/// the running node holds no schema owned by the app, which check 3 enforces.
pub(super) fn listed_schemas(manifest: &Value, app_id: &str) -> Result<Vec<ListedSchema>, String> {
    let schemas = manifest
        .get("schemas")
        .ok_or("manifest must list the schemas the app declares in `schemas` (an empty list when it declares none)")?
        .as_array()
        .ok_or("manifest `schemas` must be a list of schema declarations")?;
    require_typed_fields(schemas)?;
    schemas
        .iter()
        .map(|entry| listed_schema(app_id, entry))
        .collect()
}

/// The schemas the running node holds with `owner_app_id` = `app_id`, from the
/// node's schema list (`GET /api/schemas`, default projection). System seeds
/// are left out: the node installs them, not the app.
fn held_schemas(socket: &Path, app_id: &str) -> Result<Vec<HeldSchema>, String> {
    let request = format!(
        "GET /api/schemas HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_readonly_with_retry(socket, request.as_bytes(), NODE_SCHEMAS_TIMEOUT)?;
    let value = parse_json_response(&response, "GET /api/schemas")?;
    let rows = value
        .get("data")
        .unwrap_or(&value)
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or("the node's schema list has no schemas array")?;
    let text = |row: &Value, key: &str| row.get(key).and_then(Value::as_str).map(str::to_string);
    Ok(rows
        .iter()
        .filter(|row| row.get("owner_app_id").and_then(Value::as_str) == Some(app_id))
        .map(|row| HeldSchema {
            name: text(row, "name").unwrap_or_default(),
            descriptive_name: text(row, "descriptive_name"),
            identity_hash: text(row, "identity_hash"),
        })
        .collect())
}

/// Why `held` is not in the manifest, or `None` when it is. A manifest schema
/// relates to a held schema as the node relates two declarations: the same
/// name, the same identity, or the same descriptive name. A related schema
/// must also have the held identity hash.
fn unlisted(held: &HeldSchema, listed: &[ListedSchema]) -> Option<String> {
    let held_identity = held.identity_hash.as_deref();
    let related: Vec<&ListedSchema> = listed
        .iter()
        .filter(|schema| {
            schema.name == held.name
                || held_identity == Some(schema.identity_hash.as_str())
                || (held.descriptive_name.is_some()
                    && schema.descriptive_name == held.descriptive_name)
        })
        .collect();
    if related
        .iter()
        .any(|schema| held_identity == Some(schema.identity_hash.as_str()))
    {
        return None;
    }
    let shown = held_identity.unwrap_or("none");
    Some(related.first().map_or_else(
        || format!("{} (identity {shown}) is not listed", held.name),
        |schema| {
            format!(
                "{} has identity {shown} on the node, but the manifest's {} has identity {}",
                held.name, schema.name, schema.identity_hash
            )
        },
    ))
}

/// Check 3: every schema the running node holds for the app is in the
/// manifest by name and identity hash. The developer ran the app on this node,
/// so the node holds what the app declares.
pub(super) fn require_held_schemas_listed(
    socket: &Path,
    app_id: &str,
    listed: &[ListedSchema],
) -> Result<(), String> {
    let held = held_schemas(socket, app_id).map_err(|e| {
        format!(
            "publish reads the running node at {} to check that the manifest lists every schema app '{app_id}' declares: {e}",
            socket.display()
        )
    })?;
    let unlisted: Vec<String> = held
        .iter()
        .filter_map(|schema| unlisted(schema, listed))
        .collect();
    if unlisted.is_empty() {
        return Ok(());
    }
    Err(format!(
        "the manifest must list every schema app '{app_id}' declares, exactly as declared; the running node holds: {}",
        unlisted.join("; ")
    ))
}
