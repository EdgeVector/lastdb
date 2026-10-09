//! Direct-declare proposal types and normalization.

use super::*;

#[derive(Debug)]
pub(crate) struct DirectDeclareProposal {
    pub(crate) namespace: String,
    pub(crate) local_schema_id: String,
    pub(crate) catalog_schema: DeclarativeSchemaDefinition,
    pub(crate) proposal: SchemaResolveProposal,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SchemaSyncIntent {
    /// Resolve, register or expand, bind, and audit. The default.
    #[default]
    CatalogSync,
    /// Read-only: run the same resolution and bind validation, then report
    /// what `catalog_sync` would do. Never registers, expands, loads, binds,
    /// persists an alias, or writes a schema-sync audit event.
    Check,
}

/// True when a raw declare body asks for the read-only `check` intent. The
/// reject path uses it to keep a refused check out of the audit log.
pub(crate) fn raw_body_is_check_intent(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|raw| {
            raw.get("intent")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|intent| intent == "check")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppsDeclareRequest {
    pub(crate) app_id: String,
    pub(crate) schema: DeclarativeSchemaDefinition,
    #[serde(default)]
    pub(crate) intent: SchemaSyncIntent,
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
    schema.compute_identity_hash();
    schema
        .populate_runtime_fields()
        .map_err(|e| e.to_string())?;
    let Some(identity_hash) = schema.get_identity_hash().cloned() else {
        return Err("schema identity hash missing after compute_identity_hash".into());
    };

    let fields = schema.fields.clone().unwrap_or_default();
    if fields.is_empty() || fields.iter().any(|f| f.trim().is_empty()) {
        return Err("schema.fields must be non-empty without blank names".into());
    }

    Ok(DirectDeclareProposal {
        namespace,
        local_schema_id,
        catalog_schema: schema.clone(),
        proposal: SchemaResolveProposal {
            descriptive_name: schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema.name.clone()),
            fields,
            field_descriptions: schema.field_descriptions.clone(),
            purpose_statement: schema.purpose_statement.clone(),
            identity_hash: Some(identity_hash),
            owner_app_id: schema.owner_app_id.clone(),
        },
    })
}

/// One catalog component accepted for direct declare bind.
#[derive(Debug, Clone)]
pub(crate) struct DirectDeclareComponent {
    pub(crate) catalog_hash: String,
    pub(crate) field_mappers: HashMap<String, String>,
    pub(crate) matched_descriptive_name: Option<String>,
    pub(crate) unmapped_fields: Vec<String>,
    pub(crate) is_exact_match: Option<bool>,
    pub(crate) is_superset: Option<bool>,
}

/// Result of resolving a direct declare proposal against the catalog.
#[derive(Debug, Clone)]
pub(crate) enum DirectDeclareResolution {
    /// Novel/expanded proposal registered with Schema Service, then loaded.
    Register {
        catalog_hash: String,
        field_mappers: HashMap<String, String>,
        replaced_schema: Option<String>,
    },
    /// Single catalog schema covers the proposal (`resolution: "reuse"`).
    Reuse {
        catalog_hash: String,
        field_mappers: HashMap<String, String>,
        confidence: Option<f32>,
    },
    /// Beam / component-cover: multiple catalog schemas cover the proposal
    /// (`resolution: "compose"`).
    Compose {
        components: Vec<DirectDeclareComponent>,
        confidence: Option<f32>,
    },
}

pub(crate) fn reuse_field_mappers(schema: &DeclarativeSchemaDefinition) -> HashMap<String, String> {
    schema
        .field_mappers
        .as_ref()
        .map(|mappers| {
            mappers
                .iter()
                .map(|(field, mapper)| {
                    (
                        field.clone(),
                        format!("{}.{}", mapper.source_schema(), mapper.source_field()),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}
