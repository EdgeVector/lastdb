//! Direct-declare proposal types and normalization.

use super::*;
use fold_db::schema::ManifestRetention;

#[derive(Debug)]
pub(crate) struct DirectDeclareProposal {
    pub(crate) namespace: String,
    pub(crate) local_schema_id: String,
    pub(crate) catalog_schema: DeclarativeSchemaDefinition,
    pub(crate) proposal: SchemaResolveProposal,
    /// The manifest TTL for this schema, when the request asked for one. It is
    /// node-local policy kept beside the schema, never inside it.
    pub(crate) retention: Option<ManifestRetention>,
    /// The lock of the catalog schema this declare binds to, held until the
    /// declare returns. See `anchor_lock`.
    pub(crate) anchor_lock: DeclareLock,
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

/// Body facts needed by retention and rejection audit. The typed request
/// decoder stays separate so its grammar and error locations remain intact.
pub(crate) struct DeclareRequestFacts {
    pub(crate) check: bool,
    pub(crate) embedded_retention: bool,
    pub(crate) app_id: String,
    pub(crate) schema: String,
    pub(crate) proposal_identity_hash: Option<String>,
}

impl DeclareRequestFacts {
    pub(crate) fn from_body(body: &[u8]) -> Self {
        let raw = serde_json::from_slice::<Value>(body).unwrap_or_default();
        Self {
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
        retention: None,
        anchor_lock: DeclareLock::default(),
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
