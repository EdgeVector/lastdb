//! Pack artifact construction: snapshot projection, embeddings, resolver config,
//! and the signed manifest.

use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;

use super::upload::PublishPlan;
use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, Env, Purpose, SignatureEnvelope, SigningKey,
    ALG_ED25519, ENVELOPE_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use schema_service_core::resolver_config::{AlgorithmSelector, ResolverConfig};
use schema_service_core::resolver_pack::{
    artifact_sha256_hex, manifest_payload_value, parse_embedding_artifact, parse_resolver_config,
    parse_schema_snapshot_artifact, EmbeddingArtifact, EmbeddingVectorRecord,
    ResolverCanonicalFieldRecord, ResolverFieldRecord, ResolverPackArtifactSizes,
    ResolverPackCounts, ResolverPackManifest, ResolverSchemaRecord, SchemaLifecycle,
    SchemaSnapshotArtifact, RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION,
    RESOLVER_PACK_FORMAT_VERSION, RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION,
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

    // Callers that skip `build()` (unit tests) still project so private
    // schemas never reach pack artifacts.
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
        captured_at: snapshot.captured_at.clone(),
        source_snapshot_format_version: snapshot.format_version,
        schemas,
        canonical_fields,
    })
}

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
                let text = field_context_text(&descriptive_name, &field_name, description);
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

#[cfg(feature = "recompute-embeddings")]
pub(crate) fn field_context_text(
    descriptive_name: &str,
    field_name: &str,
    field_description: Option<&str>,
) -> String {
    match field_description.map(str::trim).filter(|s| !s.is_empty()) {
        Some(desc) => format!("the {field_name} of the {descriptive_name}: {desc}"),
        None => format!("the {field_name} of the {descriptive_name}"),
    }
}

pub(crate) fn write_pack_local_layout(
    root: &Path,
    plan: &PublishPlan,
    manifest_bytes: &[u8],
    inputs: PackInputs<'_>,
) -> Result<(), String> {
    let write = |rel: &str, bytes: &[u8]| -> Result<(), String> {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
        }
        fs::write(&path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
    };
    write(&plan.resolver_config_key, inputs.resolver_config)?;
    write(&plan.schema_snapshot_key, inputs.schema_snapshot)?;
    write(&plan.embedding_artifact_key, inputs.embedding_artifact)?;
    write(&plan.manifest_key, manifest_bytes)?;
    write(&plan.latest_manifest_pointer_key, manifest_bytes)?;
    Ok(())
}

pub(crate) fn build_signed_manifest(
    inputs: PackInputs<'_>,
    env: Env,
    signing_key: &SigningKey,
    issued_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
) -> Result<ResolverPackManifest, String> {
    let schema_snapshot: SchemaSnapshotArtifact =
        parse_schema_snapshot_artifact(inputs.schema_snapshot)
            .map_err(|e| format!("schema snapshot parse failed: {e}"))?;
    let embedding: EmbeddingArtifact = parse_embedding_artifact(inputs.embedding_artifact)
        .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
    let config = parse_resolver_config(inputs.resolver_config)
        .map_err(|e| format!("resolver_config parse failed: {e}"))?;
    config
        .validate()
        .map_err(|e| format!("resolver_config invalid: {e}"))?;
    let signing_key_id = key_id(&signing_key.verifying_key());
    let pack_expires_at = expires_at.unwrap_or(issued_at + Duration::days(90));

    let counts = ResolverPackCounts {
        schemas: schema_snapshot.schemas.len() as u32,
        canonical_fields: schema_snapshot.canonical_fields.len() as u32,
        descriptive_name_embeddings: embedding.descriptive_names.len() as u32,
        schema_field_context_embeddings: embedding.schema_field_contexts.len() as u32,
        canonical_field_embeddings: embedding.canonical_fields.len() as u32,
    };

    let mut manifest_bytes = 0;
    let mut signed = None;
    for _ in 0..8 {
        let placeholder = SignatureEnvelope {
            version: ENVELOPE_VERSION,
            purpose: Purpose::SchemaResolverPack,
            alg: ALG_ED25519.to_string(),
            key_id: signing_key_id.clone(),
            issued_at,
            expires_at,
            env,
            payload_hash: String::new(),
            sig: None,
        };
        let mut manifest = ResolverPackManifest {
            format_version: RESOLVER_PACK_FORMAT_VERSION,
            resolver_contract_version: config.resolver_contract_version,
            algorithm: AlgorithmSelector {
                id: config.algorithm.id.clone(),
                version: config.algorithm.version,
            },
            env,
            embedder_id: embedding.embedder_id.clone(),
            generated_at: issued_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            expires_at: pack_expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            schema_snapshot_hash: artifact_sha256_hex(inputs.schema_snapshot),
            embedding_artifact_hash: artifact_sha256_hex(inputs.embedding_artifact),
            resolver_config_hash: artifact_sha256_hex(inputs.resolver_config),
            policy_version: config.policy_version.clone(),
            counts: counts.clone(),
            artifact_sizes: ResolverPackArtifactSizes {
                manifest_bytes,
                schema_snapshot_bytes: inputs.schema_snapshot.len() as u64,
                embedding_artifact_bytes: inputs.embedding_artifact.len() as u64,
                resolver_config_bytes: inputs.resolver_config.len() as u64,
            },
            signing_key_id: signing_key_id.clone(),
            signature: placeholder,
        };
        let payload = manifest_payload_value(&manifest).map_err(|e| e.to_string())?;
        manifest.signature.payload_hash =
            compute_payload_hash(&payload).map_err(|e| e.to_string())?;
        manifest.signature =
            sign_envelope(signing_key, manifest.signature.clone()).map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
        let next_size = bytes.len() as u64;
        signed = Some(manifest);
        if next_size == manifest_bytes {
            break;
        }
        manifest_bytes = next_size;
    }
    signed.ok_or_else(|| "failed to sign manifest".to_string())
}

pub(crate) fn default_resolver_config(policy_version: &str) -> ResolverConfig {
    let mut config = ResolverConfig::embedding_beam_shadow_defaults();
    config.policy_version = policy_version.to_string();
    config
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
