//! Pack artifact construction: snapshot projection into schema and field records.

use super::embeddings::*;
use super::manifest::*;
use schema_service_core::resolver_config::ResolverConfig;
use schema_service_core::resolver_pack::{
    ResolverCanonicalFieldRecord, ResolverFieldRecord, ResolverSchemaRecord, SchemaLifecycle,
    SchemaSnapshotArtifact, RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION,
};
use schema_service_core::snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, SnapshotEnvelope,
    SNAPSHOT_FORMAT_VERSION,
};
use schema_service_core::types::CanonicalField;

#[derive(Clone, Copy)]
pub(crate) struct PackInputs<'a> {
    pub(crate) resolver_config: &'a [u8],
    pub(crate) schema_snapshot: &'a [u8],
    pub(crate) embedding_artifact: &'a [u8],
}

#[derive(Debug)]
pub(crate) struct BuiltArtifacts {
    pub(crate) schema_snapshot: Vec<u8>,
    pub(crate) embedding_artifact: Vec<u8>,
    pub(crate) resolver_config: ResolverConfig,
}

pub(crate) fn build_artifacts_from_snapshot(
    snapshot: &SnapshotEnvelope,
    policy_version: &str,
) -> Result<BuiltArtifacts, String> {
    if snapshot.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(format!(
            "snapshot format_version {} not supported (this binary supports {})",
            snapshot.format_version, SNAPSHOT_FORMAT_VERSION
        ));
    }

    // Callers that skip `build()` still project so private schemas never
    // reach pack artifacts.
    let snapshot = project_snapshot_shared_only(snapshot.clone(), offline_is_system_schema);
    let snapshot = &snapshot;

    let schema_snapshot = build_schema_snapshot_artifact(snapshot)?;
    let schema_snapshot_bytes = serde_json::to_vec_pretty(&schema_snapshot)
        .map_err(|e| format!("schema snapshot serialization failed: {e}"))?;
    let embedding_artifact = build_embedding_artifact(snapshot)?;
    let embedding_artifact_bytes = serde_json::to_vec_pretty(&embedding_artifact)
        .map_err(|e| format!("embedding artifact serialization failed: {e}"))?;

    Ok(BuiltArtifacts {
        schema_snapshot: schema_snapshot_bytes,
        embedding_artifact: embedding_artifact_bytes,
        resolver_config: default_resolver_config(policy_version),
    })
}

pub(crate) fn build_schema_snapshot_artifact(
    snapshot: &SnapshotEnvelope,
) -> Result<SchemaSnapshotArtifact, String> {
    for (name, field) in &snapshot.canonical_fields {
        schema_service_core::field_name_policy::validate_approved_field(name, field)
            .map_err(|error| error.to_string())?;
    }
    for schema in &snapshot.schemas {
        schema_service_core::field_name_policy::validate_schema_vocabulary(
            schema,
            &snapshot.canonical_fields,
        )
        .map_err(|error| error.to_string())?;
    }
    let mut schemas = Vec::new();
    for schema in snapshot
        .schemas
        .iter()
        .filter(|schema| schema.superseded_by.is_none())
    {
        let descriptive_name = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        let purpose_statement = schema
            .purpose_statement
            .clone()
            .unwrap_or_else(|| descriptive_name.clone());
        let schema_id = schema_id(schema);
        let mut field_names = schema.fields.clone().unwrap_or_default();
        field_names.extend(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        );
        field_names.sort();
        field_names.dedup();

        let mut fields = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let field_id = format!("{schema_id}#{field_name}");
            let description = schema
                .field_descriptions
                .get(&field_name)
                .cloned()
                .unwrap_or_else(|| field_name.clone());
            let mut canonical_field_ids = Vec::new();
            if snapshot.canonical_fields.contains_key(&field_name) {
                canonical_field_ids.push(canonical_field_id(&field_name));
            }
            fields.push(ResolverFieldRecord {
                field_id,
                field_name: field_name.clone(),
                field_type: schema.get_field_type(&field_name).to_string(),
                description,
                canonical_field_ids,
            });
        }

        schemas.push(ResolverSchemaRecord {
            schema_id,
            descriptive_name,
            purpose_statement,
            lifecycle: SchemaLifecycle::Active,
            fields,
        });
    }

    let mut canonical_fields: Vec<_> = snapshot
        .canonical_fields
        .iter()
        .map(|(name, field)| resolver_canonical_field(name, field))
        .collect();
    canonical_fields.sort_by(|a, b| a.canonical_field_id.cmp(&b.canonical_field_id));

    Ok(SchemaSnapshotArtifact {
        format_version: RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION,
        name_policy_version: schema_service_core::field_name_policy::NAME_POLICY_VERSION,
        captured_at: snapshot.captured_at.clone(),
        source_snapshot_format_version: snapshot.format_version,
        schemas,
        canonical_fields,
    })
}

pub(crate) fn schema_id(schema: &schema_types::Schema) -> String {
    schema
        .identity_hash
        .clone()
        .unwrap_or_else(|| schema.name.clone())
}

pub(crate) fn canonical_field_id(name: &str) -> String {
    format!("canonical:{name}")
}

pub(crate) fn resolver_canonical_field(
    name: &str,
    field: &CanonicalField,
) -> ResolverCanonicalFieldRecord {
    ResolverCanonicalFieldRecord {
        canonical_field_id: canonical_field_id(name),
        description: field.description.clone(),
        field_type: field.field_type.to_string(),
    }
}

pub(crate) fn namespaced_descriptive_name_key(
    owner_app_id: Option<&str>,
    descriptive_name: &str,
) -> String {
    match owner_app_id.filter(|s| !s.is_empty()) {
        Some(app_id) => format!("{app_id}/{descriptive_name}"),
        None => descriptive_name.to_string(),
    }
}
