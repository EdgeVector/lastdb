//! Direct-declare proposal types and normalization.

use super::*;
use fold_db::schema::ManifestRetention;

#[derive(Debug)]
pub(crate) struct DirectDeclareProposal {
    pub(crate) namespace: String,
    pub(crate) local_schema_id: String,
    pub(crate) catalog_schema: DeclarativeSchemaDefinition,
    /// The manifest TTL for this schema, when the request asked for one. It is
    /// node-local policy kept beside the schema, never inside it.
    pub(crate) retention: Option<ManifestRetention>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SchemaSyncIntent {
    /// Reuse or register the definition, load owned values, and audit.
    #[default]
    CatalogSync,
    /// Read-only: check the definition and retention policy without writes.
    Check,
}

pub(crate) fn body_has_embedded_retention(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|raw| raw.get("schema")?.get("retention").cloned())
        .is_some()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppsDeclareRequest {
    pub(crate) app_id: String,
    pub(crate) schema: DeclarativeSchemaDefinition,
    #[serde(default)]
    pub(crate) intent: SchemaSyncIntent,
    /// The manifest `retention` of this schema, beside `schema` so it cannot
    /// reach the schema artifact or its identity hash. Owner socket only.
    #[serde(default)]
    pub(crate) retention: Option<ManifestRetention>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchemaDeclareRequest {
    pub(crate) namespace: String,
    pub(crate) schema: DeclarativeSchemaDefinition,
    #[serde(default)]
    pub(crate) intent: SchemaSyncIntent,
}

/// One `lastdb-app.json` schema entry of `app_id`, normalized exactly as the
/// declare route normalizes it: name `<app_id>/<name>`, `owner_app_id`, and
/// the identity hash. `lastdb app publish` compares it with the schemas the
/// running node holds, so the hash has one implementation.
pub fn normalize_app_manifest_schema(
    app_id: &str,
    schema: DeclarativeSchemaDefinition,
) -> Result<DeclarativeSchemaDefinition, String> {
    normalize_direct_declare_proposal("app_id", app_id, schema)
        .map(|proposal| proposal.catalog_schema)
}

/// The owned normalization lives in `schema_types`, shared with the Schema
/// Service catalog feed; the node adds its runtime fields.
pub(crate) fn normalize_direct_declare_proposal(
    namespace_label: &str,
    namespace: &str,
    mut schema: DeclarativeSchemaDefinition,
) -> Result<DirectDeclareProposal, String> {
    let schema_types::OwnedDeclaration {
        namespace,
        local_schema_id,
    } = schema.normalize_owned_declaration(namespace_label, namespace)?;
    schema.validate().map_err(|e| e.to_string())?;
    Ok(DirectDeclareProposal {
        namespace,
        local_schema_id,
        catalog_schema: schema,
        retention: None,
    })
}
