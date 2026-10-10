//! Text-only FastEmbed adapter for schema-service binaries.
//!
//! Local/dev tools can opt into the normal FastEmbed HuggingFace cache path via
//! the `fastembed` feature. Lambda opts into `fastembed-layer`, which builds the
//! same all-MiniLM-L6-v2 text model from bundled layer files and does not enable
//! FastEmbed's `hf-hub` downloader feature.

#[cfg(feature = "fastembed")]
use fastembed_crate::EmbeddingModel;
#[cfg(feature = "fastembed")]
use fastembed_crate::InitOptions;
use fastembed_crate::{
    InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};
use schema_service_core::{EmbedError, Embedder};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const EMBEDDER_ID: &str = "fastembed/all-MiniLM-L6-v2";
const DEFAULT_LAMBDA_FASTEMBED_DIR: &str = "/opt/fastembed_cache";
const HF_CACHE_REPO_DIR: &str = "models--Qdrant--all-MiniLM-L6-v2-onnx";

#[derive(Debug, Clone)]
enum FastEmbedSource {
    #[cfg(feature = "fastembed")]
    OnlineCache(Option<PathBuf>),
    LocalLayer(PathBuf),
}

/// Schema-service FastEmbed adapter. The public name is retained for callers,
/// but the Lambda path no longer routes through `fold_db`'s semantic-search
/// feature or HuggingFace runtime downloads.
pub struct FoldDbFastEmbedder {
    model: OnceLock<Result<TextEmbedding, String>>,
    source: FastEmbedSource,
}

impl FoldDbFastEmbedder {
    #[cfg(feature = "fastembed")]
    pub fn new() -> Self {
        Self {
            model: OnceLock::new(),
            source: FastEmbedSource::OnlineCache(
                std::env::var_os("FASTEMBED_CACHE_DIR").map(PathBuf::from),
            ),
        }
    }

    #[cfg(all(not(feature = "fastembed"), feature = "fastembed-layer"))]
    pub fn new() -> Self {
        Self::from_lambda_layer()
    }

    pub fn from_lambda_layer() -> Self {
        Self::from_local_model_dir(lambda_fastembed_dir())
    }

    pub fn from_local_model_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            model: OnceLock::new(),
            source: FastEmbedSource::LocalLayer(dir.into()),
        }
    }

    /// Eagerly initialize the underlying ONNX model (best-effort).
    pub fn warm(&self) {
        let _ = self.model();
    }

    fn model(&self) -> Result<&TextEmbedding, EmbedError> {
        match self.model.get_or_init(|| self.init_model()) {
            Ok(model) => Ok(model),
            Err(err) => Err(EmbedError::InitFailed(err.clone())),
        }
    }

    fn init_model(&self) -> Result<TextEmbedding, String> {
        match &self.source {
            #[cfg(feature = "fastembed")]
            FastEmbedSource::OnlineCache(cache_dir) => {
                let mut opts = InitOptions::new(EmbeddingModel::AllMiniLML6V2)
                    .with_show_download_progress(false);
                if let Some(dir) = cache_dir {
                    opts = opts.with_cache_dir(dir.clone());
                }
                TextEmbedding::try_new(opts).map_err(|e| e.to_string())
            }
            FastEmbedSource::LocalLayer(dir) => load_local_layer_model(dir),
        }
    }

    fn map_embed_error(err: &impl ToString) -> EmbedError {
        EmbedError::EmbedFailed(err.to_string())
    }
}

impl Default for FoldDbFastEmbedder {
    fn default() -> Self {
        Self::new()
    }
}

impl Embedder for FoldDbFastEmbedder {
    fn embedder_id(&self) -> &'static str {
        // Matches the `embedder` field in
        // `schema_service_core/data/schema_org/validated_canonical_fields.json`
        // and the snapshot envelope's `embedder_version`.
        EMBEDDER_ID
    }

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let mut results = self
            .model()?
            .embed(vec![text], None)
            .map_err(|err| Self::map_embed_error(&err))?;

        results
            .pop()
            .ok_or_else(|| EmbedError::EmbedFailed("No embedding returned".to_string()))
    }
}

fn lambda_fastembed_dir() -> PathBuf {
    PathBuf::from(
        std::env::var_os("SCHEMA_FASTEMBED_MODEL_DIR")
            .or_else(|| std::env::var_os("FASTEMBED_CACHE_DIR"))
            .unwrap_or_else(|| DEFAULT_LAMBDA_FASTEMBED_DIR.into()),
    )
}

fn load_local_layer_model(root: &Path) -> Result<TextEmbedding, String> {
    let model_dir = resolve_model_dir(root)?;
    let read = |name: &str| -> Result<Vec<u8>, String> {
        fastembed_crate::read_file_to_bytes(&model_dir.join(name))
            .map_err(|e| format!("failed to read {} from {}: {e}", name, model_dir.display()))
    };

    let tokenizer_files = TokenizerFiles {
        tokenizer_file: read("tokenizer.json")?,
        config_file: read("config.json")?,
        special_tokens_map_file: read("special_tokens_map.json")?,
        tokenizer_config_file: read("tokenizer_config.json")?,
    };
    let model = UserDefinedEmbeddingModel::new(read("model.onnx")?, tokenizer_files)
        .with_pooling(Pooling::Mean)
        .with_quantization(QuantizationMode::None);

    TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::new())
        .map_err(|e| e.to_string())
}

fn resolve_model_dir(root: &Path) -> Result<PathBuf, String> {
    if has_layer_files(root) {
        return Ok(root.to_path_buf());
    }

    let snapshots = root.join(HF_CACHE_REPO_DIR).join("snapshots");
    let mut candidates = std::fs::read_dir(&snapshots)
        .map_err(|e| {
            format!(
                "fastembed layer is missing flat model files and {} is unreadable: {e}",
                snapshots.display()
            )
        })?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && has_layer_files(path))
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.pop().ok_or_else(|| {
        format!(
            "fastembed layer at {} does not contain model.onnx and tokenizer files",
            root.display()
        )
    })
}

fn has_layer_files(dir: &Path) -> bool {
    [
        "model.onnx",
        "tokenizer.json",
        "config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
    ]
    .iter()
    .all(|name| dir.join(name).is_file())
}
