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

/// Body facts needed by retention and rejection audit. The typed request
/// decoder stays separate so its grammar and error locations remain intact.
pub(crate) struct DeclareRequestFacts {
    pub(crate) check: bool,
    pub(crate) embedded_retention: bool,
    pub(crate) app_id: String,
    pub(crate) schema: String,
    pub(crate) proposal_identity_hash: Option<String>,
    /// The definition maps a field to the values of another schema.
    pub(crate) value_mappers: bool,
}

impl DeclareRequestFacts {
    pub(crate) fn from_body(body: &[u8]) -> Self {
        let raw = serde_json::from_slice::<Value>(body).unwrap_or_default();
        Self {
            value_mappers: raw
                .get("schema")
                .and_then(|s| s.get("field_mappers"))
                .is_some_and(|m| !m.is_null() && m.as_object().is_none_or(|map| !map.is_empty())),
            check: raw.get("intent").and_then(Value::as_str) == Some("check"),
            embedded_retention: raw.get("schema").and_then(|s| s.get("retention")).is_some(),
            app_id: raw
                .get("app_id")
                .or_else(|| raw.get("namespace"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            schema: raw
                .get("schema")
                .and_then(|s| s.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            proposal_identity_hash: raw
                .get("schema")
                .and_then(|s| s.get("identity_hash"))
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }

    /// Every schema owns its record values. A definition that reads a field
    /// through another schema asks for the retired cross-schema value sharing.
    pub(crate) fn refuse_value_mappers(&self) -> Result<(), String> {
        if self.value_mappers {
            return Err(
                "schema.field_mappers is not supported; each schema owns its values".into(),
            );
        }
        Ok(())
    }
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

pub(crate) fn normalize_direct_declare_proposal(
    namespace_label: &str,
    namespace: &str,
    mut schema: DeclarativeSchemaDefinition,
) -> Result<DirectDeclareProposal, String> {
    let namespace = namespace.trim().to_string();
    if namespace.is_empty() || namespace.contains('/') {
        return Err(format!(
            "{namespace_label} must be non-empty and must not contain '/'"
        ));
    }
    let raw_name = schema.name.trim().to_string();
    if raw_name.is_empty() {
        return Err("schema.name must be non-empty".into());
    }
    let local_name = match raw_name.split_once('/') {
        Some((owner, local)) if owner == namespace && !local.is_empty() => local.to_string(),
        Some((_owner, _local)) => {
            return Err(format!(
                "schema.name namespace must match request.{namespace_label}"
            ))
        }
        None => raw_name,
    };
    let local_schema_id = format!("{namespace}/{local_name}");
    schema.owner_app_id = Some(namespace.clone());
    if schema
        .descriptive_name
        .as_deref()
        .is_none_or(|name| name.trim().is_empty())
    {
        schema.descriptive_name = Some(local_schema_id.rsplit('/').next().unwrap().to_string());
    }
    schema.name = local_schema_id.clone();
    schema.owned_values = true;
    schema.validate_alternate_keys()?;
    schema.compute_identity_hash();
    schema
        .populate_runtime_fields()
        .map_err(|e| e.to_string())?;
    if schema.get_identity_hash().is_none() {
        return Err("schema identity hash missing after compute_identity_hash".into());
    }

    let fields = schema.fields.clone().unwrap_or_default();
    if fields.is_empty() || fields.iter().any(|f| f.trim().is_empty()) {
        return Err("schema.fields must be non-empty without blank names".into());
    }

    Ok(DirectDeclareProposal {
        namespace,
        local_schema_id,
        catalog_schema: schema,
        retention: None,
    })
}
