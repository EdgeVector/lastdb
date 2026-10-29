use super::corpus::{Case, Expected, Field};
use schema_service_core::{
    cosine_similarity, Embedder, ExternalSchemaPersistence, InMemorySchemaPersistence,
    SchemaConfig, SchemaServiceState,
};
use schema_service_server_shared::{FoldDbFastEmbedder, SchemaConfigEnv};
use serde::Serialize;
use std::{path::Path, sync::Arc};

#[derive(Debug, Serialize)]
pub struct Observation {
    pub case: String,
    pub category: String,
    pub expected: Expected,
    pub cold_registry: String,
    pub warm_replay: String,
    pub cold_detail: Option<String>,
    pub warm_detail: Option<String>,
    pub description_cosine: Option<f32>,
    pub state_preserved_on_rejection: bool,
    pub passed: bool,
}

fn classify(result: &schema_types::FoldDbResult<()>) -> String {
    let Err(error) = result else {
        return "accepted".into();
    };
    let message = error.to_string();
    for (needle, outcome) in [
        ("canonical_field_reuse_required", "reuse_required"),
        ("canonical_field_conflict", "definition_conflict"),
        ("invalid_canonical_meaning", "invalid_meaning"),
        ("generic text container", "generic_container"),
    ] {
        if message.contains(needle) {
            return outcome.into();
        }
    }
    "other_error".into()
}

fn expected_name(expected: Expected) -> String {
    serde_json::to_value(expected)
        .expect("fixed enum")
        .as_str()
        .expect("enum string")
        .into()
}

async fn add(state: &SchemaServiceState, field: Field) -> schema_types::FoldDbResult<()> {
    state
        .add_canonical_field(field.name, field.definition())
        .await
}

pub async fn observe(case: &Case, embedder: Arc<dyn Embedder>) -> Result<Observation, String> {
    let backend = Arc::new(InMemorySchemaPersistence::default());
    let state = SchemaServiceState::new_with_external(
        backend.clone(),
        embedder.clone(),
        SchemaConfig::from_env(),
    )
    .await
    .map_err(|error| error.to_string())?;
    if let Some(prior) = case.prior {
        add(&state, prior)
            .await
            .map_err(|error| format!("{} setup: {error}", case.id))?;
    }
    let before = backend
        .load_all_canonical_fields()
        .await
        .map_err(|error| error.to_string())?;
    let first = add(&state, case.proposed).await;
    let after = backend
        .load_all_canonical_fields()
        .await
        .map_err(|error| error.to_string())?;
    let preserved = first.is_ok() || before == after;
    let repeated = add(&state, case.proposed).await;
    let cold = classify(&first);
    let warm = classify(&repeated);
    let expected = expected_name(case.expected);
    let cosine = case.prior.and_then(|prior| {
        let left = embedder.embed_text(prior.description).ok()?;
        let right = embedder.embed_text(case.proposed.description).ok()?;
        Some(cosine_similarity(&left, &right))
    });
    Ok(Observation {
        case: case.id.into(),
        category: case.category.into(),
        expected: case.expected,
        passed: cold == expected && warm == expected && preserved,
        cold_registry: cold,
        warm_replay: warm,
        cold_detail: first.err().map(|error| error.to_string()),
        warm_detail: repeated.err().map(|error| error.to_string()),
        description_cosine: cosine,
        state_preserved_on_rejection: preserved,
    })
}

#[derive(Serialize)]
pub struct ModelConsistency {
    pub dimensions: usize,
    pub max_absolute_difference: f32,
    pub passed: bool,
}

pub fn model_consistency(embedder: &dyn Embedder) -> Result<ModelConsistency, String> {
    compare_embedders(embedder, embedder)
}

pub fn layer_consistency(
    cache: &Path,
    embedder: &dyn Embedder,
) -> Result<ModelConsistency, String> {
    let bundled = FoldDbFastEmbedder::from_local_model_dir(cache);
    compare_embedders(embedder, &bundled)
}

fn compare_embedders(
    initial: &dyn Embedder,
    repeated: &dyn Embedder,
) -> Result<ModelConsistency, String> {
    let text = "Parcel delivery status reported by the carrier";
    let first = initial
        .embed_text(text)
        .map_err(|error| error.to_string())?;
    let second = repeated
        .embed_text(text)
        .map_err(|error| error.to_string())?;
    if first.len() != 384
        || first.len() != second.len()
        || first.iter().chain(&second).any(|value| !value.is_finite())
    {
        return Err("the product model returned invalid embedding dimensions or values".into());
    }
    let max = first
        .iter()
        .zip(&second)
        .map(|(left, right)| (left - right).abs())
        .fold(0.0_f32, f32::max);
    Ok(ModelConsistency {
        dimensions: first.len(),
        max_absolute_difference: max,
        passed: max <= 1e-6,
    })
}

#[derive(Serialize)]
pub struct ModelFailure {
    pub initialization_error: String,
    pub lexically_supported_novel_field: Observation,
    pub exact_duplicate_without_model: Observation,
    pub unsupported_name_without_model: Observation,
    pub passed: bool,
}

pub async fn failed_model(root: &Path) -> Result<ModelFailure, String> {
    let missing = root.join("intentionally_missing_model");
    if missing.exists() {
        return Err("the model-failure path must remain absent".into());
    }
    let broken = Arc::new(FoldDbFastEmbedder::from_local_model_dir(missing));
    let initialization_error = broken
        .embed_text("probe")
        .err()
        .ok_or("the missing product model unexpectedly initialized")?
        .to_string();
    let novel = Field {
        name: "parcel_delivery_status",
        description: "Parcel delivery status from carrier",
        kind: super::corpus::Kind::String,
    };
    let supported = observe(
        &Case {
            id: "missing_model_lexical_support",
            category: "model_failure",
            prior: None,
            proposed: novel,
            expected: Expected::Accepted,
        },
        broken.clone(),
    )
    .await?;
    let duplicate = observe(
        &Case {
            id: "missing_model_exact_duplicate",
            category: "model_failure",
            prior: Some(novel),
            proposed: Field {
                name: "parcel_delivery_state",
                ..novel
            },
            expected: Expected::ReuseRequired,
        },
        broken.clone(),
    )
    .await?;
    let unsupported = observe(
        &Case {
            id: "missing_model_unsupported_name",
            category: "model_failure",
            prior: None,
            proposed: Field {
                name: "parcel_tracking_number",
                description: "The time a telescope exposure starts",
                kind: super::corpus::Kind::String,
            },
            expected: Expected::InvalidMeaning,
        },
        broken,
    )
    .await?;
    Ok(ModelFailure {
        passed: supported.passed && duplicate.passed && unsupported.passed,
        initialization_error,
        lexically_supported_novel_field: supported,
        exact_duplicate_without_model: duplicate,
        unsupported_name_without_model: unsupported,
    })
}
