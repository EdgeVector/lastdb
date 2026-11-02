//! [FastEmbed](https://github.com/Anush008/fastembed-rs) - Fast, light, accurate library built for retrieval embedding generation.
//!
//! The library provides the TextEmbedding struct to interface with text embedding models.
//!
#![cfg_attr(
    feature = "hf-hub",
    doc = r#"
 ### Instantiating [TextEmbedding](crate::TextEmbedding)
 ```
 use fastembed::{TextEmbedding, InitOptions, EmbeddingModel};

# fn model_demo() -> anyhow::Result<()> {
 // With default InitOptions
 let model = TextEmbedding::try_new(Default::default())?;

 // List all supported models
 dbg!(TextEmbedding::list_supported_models());

 // With custom InitOptions
 let model = TextEmbedding::try_new(
        InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(true),
 )?;
 # Ok(())
 # }
 ```
"#
)]
//! Find more info about the available options in the [InitOptions](crate::InitOptions) documentation.
//!
#![cfg_attr(
    feature = "hf-hub",
    doc = r#"
 ### Embeddings generation
```
# use fastembed::{TextEmbedding, InitOptions, EmbeddingModel};
# fn embedding_demo() -> anyhow::Result<()> {
# let model: TextEmbedding = TextEmbedding::try_new(Default::default())?;
 let documents = vec![
    "passage: Hello, World!",
    "query: Hello, World!",
    "passage: This is an example passage.",
    // You can leave out the prefix but it's recommended
    "fastembed-rs is licensed under MIT"
    ];

 // Generate embeddings with the default batch size, 256
 let embeddings = model.embed(documents, None)?;

 println!("Embeddings length: {}", embeddings.len()); // -> Embeddings length: 4
 # Ok(())
 # }
 ```
"#
)]
// Vendored-fork note: upstream v4.9.0 predates the
// mismatched_lifetime_syntaxes lint; allow it so strict workspace lanes
// (-D warnings) never trip on unmodified upstream code.
#![allow(unknown_lints)]
#![allow(mismatched_lifetime_syntaxes)]

mod common;
mod models;
pub mod output;
mod pooling;
mod text_embedding;

pub use ort::execution_providers::ExecutionProviderDispatch;

pub use crate::common::{get_cache_dir, read_file_to_bytes, Embedding, Error, TokenizerFiles};
pub use crate::models::{model_info::ModelInfo, quantization::QuantizationMode};
pub use crate::output::{EmbeddingOutput, OutputKey, OutputPrecedence, SingleBatchOutput};
pub use crate::pooling::Pooling;

// For Text Embedding
pub use crate::models::text_embedding::EmbeddingModel;
pub use crate::text_embedding::{
    InitOptions, InitOptionsUserDefined, TextEmbedding, UserDefinedEmbeddingModel,
};
