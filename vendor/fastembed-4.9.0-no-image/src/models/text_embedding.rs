use std::{collections::HashMap, convert::TryFrom, fmt::Display, str::FromStr, sync::OnceLock};

use super::model_info::ModelInfo;

/// Lazy static list of all available models.
static MODEL_MAP: OnceLock<HashMap<EmbeddingModel, ModelInfo<EmbeddingModel>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EmbeddingModel {
    /// sentence-transformers/all-MiniLM-L6-v2 - Default
    AllMiniLML6V2,
}

/// Centralized function to initialize the models map.
fn init_models_map() -> HashMap<EmbeddingModel, ModelInfo<EmbeddingModel>> {
    let models_list = vec![ModelInfo {
        model: EmbeddingModel::AllMiniLML6V2,
        dim: 384,
        description: String::from("Sentence Transformer model, MiniLM-L6-v2"),
        model_code: String::from("Qdrant/all-MiniLM-L6-v2-onnx"),
        model_file: String::from("model.onnx"),
        additional_files: Vec::new(),
    }];

    // TODO: Use when out in stable
    // assert_eq!(
    //     std::mem::variant_count::<EmbeddingModel>(),
    //     models_list.len(),
    //     "models::models() is not exhaustive"
    // );

    models_list
        .into_iter()
        .fold(HashMap::new(), |mut map, model| {
            // Insert the model into the map
            map.insert(model.model.clone(), model);
            map
        })
}

/// Get a map of all available models.
pub fn models_map() -> &'static HashMap<EmbeddingModel, ModelInfo<EmbeddingModel>> {
    MODEL_MAP.get_or_init(init_models_map)
}

/// Get model information by model code.
pub fn get_model_info(model: &EmbeddingModel) -> Option<&ModelInfo<EmbeddingModel>> {
    models_map().get(model)
}

/// Get a list of all available models.
///
/// This will assign new memory to the models list; where possible, use
/// [`models_map`] instead.
pub fn models_list() -> Vec<ModelInfo<EmbeddingModel>> {
    models_map().values().cloned().collect()
}

impl Display for EmbeddingModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let model_info = get_model_info(self).expect("Model not found.");
        write!(f, "{}", model_info.model_code)
    }
}

impl FromStr for EmbeddingModel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        models_list()
            .into_iter()
            .find(|m| m.model_code.eq_ignore_ascii_case(s))
            .map(|m| m.model)
            .ok_or_else(|| format!("Unknown embedding model: {s}"))
    }
}

impl TryFrom<String> for EmbeddingModel {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
