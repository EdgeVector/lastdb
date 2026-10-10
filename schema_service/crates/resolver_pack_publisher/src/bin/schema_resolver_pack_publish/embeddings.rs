//! Embedding artifact construction and snapshot embedding recompute.

use std::hash::{Hash, Hasher};

use super::artifacts::*;
use schema_service_core::resolver_pack::{
    EmbeddingArtifact, EmbeddingVectorRecord, RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION,
};
use schema_service_core::snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, SnapshotEnvelope,
};

pub(crate) fn build_embedding_artifact(
    snapshot: &SnapshotEnvelope,
) -> Result<EmbeddingArtifact, String> {
    let mut descriptive_names = Vec::new();
    let mut schema_field_contexts = Vec::new();
    let mut canonical_fields = Vec::new();

    for schema in snapshot
        .schemas
        .iter()
        .filter(|schema| schema.superseded_by.is_none())
    {
        let descriptive_name = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        let schema_key =
            namespaced_descriptive_name_key(schema.owner_app_id.as_deref(), &descriptive_name);
        let schema_vector = snapshot
            .embeddings
            .descriptive_names
            .get(&schema_key)
            .or_else(|| snapshot.embeddings.descriptive_names.get(&schema.name))
            .or_else(|| snapshot.embeddings.descriptive_names.get(&descriptive_name))
            .ok_or_else(|| {
                format!("missing descriptive-name embedding for schema {schema_key:?}")
            })?;
        descriptive_names.push(EmbeddingVectorRecord {
            target_id: schema_id(schema),
            vector: schema_vector.clone(),
        });

        let mut field_names = schema.fields.clone().unwrap_or_default();
        field_names.extend(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        );
        field_names.sort();
        field_names.dedup();
        for field_name in field_names {
            let description = schema
                .field_descriptions
                .get(&field_name)
                .map(String::as_str);
            let key = field_embedding_cache_key(&descriptive_name, &field_name, description);
            let vector = snapshot.embeddings.fields.get(&key).ok_or_else(|| {
                format!(
                    "missing field-context embedding for schema {schema_key:?} field {field_name:?}"
                )
            })?;
            schema_field_contexts.push(EmbeddingVectorRecord {
                target_id: format!("{}#{field_name}", schema_id(schema)),
                vector: vector.clone(),
            });
        }
    }

    for name in snapshot.canonical_fields.keys() {
        let vector = snapshot
            .embeddings
            .canonical_fields
            .get(name)
            .ok_or_else(|| format!("missing canonical-field embedding for {name:?}"))?;
        canonical_fields.push(EmbeddingVectorRecord {
            target_id: canonical_field_id(name),
            vector: vector.clone(),
        });
    }

    descriptive_names.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    schema_field_contexts.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    canonical_fields.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    let dimensions = infer_embedding_dimensions(&[
        &descriptive_names,
        &schema_field_contexts,
        &canonical_fields,
    ])?;

    Ok(EmbeddingArtifact {
        format_version: RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION,
        embedder_id: snapshot.embedder_version.clone(),
        generated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        dimensions,
        descriptive_names,
        schema_field_contexts,
        canonical_fields,
    })
}

/// Production wire snapshots strip embeddings (Lambda size). Rebuild with
/// FastEmbed so pack artifacts match Mini's proposal embedder id.
pub(crate) fn recompute_snapshot_embeddings(
    mut snapshot: SnapshotEnvelope,
    max_schemas: Option<usize>,
) -> Result<SnapshotEnvelope, String> {
    snapshot = project_snapshot_shared_only(snapshot, offline_is_system_schema);
    if let Some(max) = max_schemas {
        snapshot.schemas.truncate(max);
    }
    #[cfg(feature = "recompute-embeddings")]
    {
        use std::collections::HashMap;

        use schema_service_core::Embedder as _;
        use schema_service_server_shared::FoldDbFastEmbedder;

        const EMBEDDER_ID: &str = "fastembed/all-MiniLM-L6-v2";
        let model = FoldDbFastEmbedder::new();
        let mut descriptive_names = HashMap::new();
        let mut fields = HashMap::new();
        let mut canonical_fields = HashMap::new();

        for schema in snapshot
            .schemas
            .iter()
            .filter(|s| s.superseded_by.is_none())
        {
            let descriptive_name = schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema.name.clone());
            let schema_key =
                namespaced_descriptive_name_key(schema.owner_app_id.as_deref(), &descriptive_name);
            let name_vec = model
                .embed_text(&descriptive_name)
                .map_err(|e| format!("embed descriptive_name {descriptive_name:?}: {e}"))?;
            descriptive_names.insert(schema_key, name_vec.clone());
            descriptive_names.insert(descriptive_name.clone(), name_vec.clone());
            descriptive_names.insert(schema.name.clone(), name_vec);

            let mut field_names = schema.fields.clone().unwrap_or_default();
            field_names.extend(
                schema
                    .transform_fields
                    .iter()
                    .flat_map(|m| m.keys().cloned()),
            );
            field_names.sort();
            field_names.dedup();
            for field_name in field_names {
                let description = schema
                    .field_descriptions
                    .get(&field_name)
                    .map(String::as_str);
                let text = schema_service_core::field_context_text(
                    &descriptive_name,
                    &field_name,
                    description,
                );
                let key = field_embedding_cache_key(&descriptive_name, &field_name, description);
                let vec = model
                    .embed_text(&text)
                    .map_err(|e| format!("embed field {field_name:?}: {e}"))?;
                fields.insert(key, vec);
            }
        }

        for (name, field) in &snapshot.canonical_fields {
            let text = if field.description.trim().is_empty() {
                name.clone()
            } else {
                format!("{name}: {}", field.description)
            };
            let vec = model
                .embed_text(&text)
                .map_err(|e| format!("embed canonical {name:?}: {e}"))?;
            canonical_fields.insert(name.clone(), vec);
        }

        snapshot.embedder_version = EMBEDDER_ID.to_string();
        snapshot.embeddings = schema_service_core::snapshot::SnapshotEmbeddings {
            descriptive_names,
            fields,
            canonical_fields,
        };
        Ok(snapshot)
    }
    #[cfg(not(feature = "recompute-embeddings"))]
    {
        let _ = max_schemas;
        Err(
            "--recompute-embeddings requires building with --features recompute-embeddings"
                .to_string(),
        )
    }
}

pub(crate) fn field_embedding_cache_key(
    descriptive_name: &str,
    field_name: &str,
    field_description: Option<&str>,
) -> String {
    let desc_hash = match field_description {
        Some(desc) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            desc.hash(&mut hasher);
            hasher.finish()
        }
        None => 0,
    };
    format!("{descriptive_name}:{field_name}:{desc_hash}")
}

pub(crate) fn infer_embedding_dimensions(
    classes: &[&[EmbeddingVectorRecord]],
) -> Result<u32, String> {
    let mut dimensions = None;
    for record in classes.iter().flat_map(|records| records.iter()) {
        if record.vector.is_empty() {
            continue;
        }
        let len = record.vector.len() as u32;
        match dimensions {
            Some(existing) if existing != len => {
                return Err(format!(
                    "embedding dimension mismatch: saw {len}, expected {existing}"
                ));
            }
            Some(_) => {}
            None => dimensions = Some(len),
        }
    }
    Ok(dimensions.unwrap_or(0))
}
