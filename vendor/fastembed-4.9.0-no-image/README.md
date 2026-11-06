<div align="center">
  <h1><a href="https://crates.io/crates/fastembed">FastEmbed-rs 🦀</a></h1>
 <h3>Rust library for generating vector embeddings locally.</h3>
  <a href="https://crates.io/crates/fastembed"><img src="https://img.shields.io/crates/v/fastembed.svg" alt="Crates.io"></a>
  <a href="https://github.com/Anush008/fastembed-rs/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-apache-blue.svg" alt="MIT Licensed"></a>
  <a href="https://github.com/Anush008/fastembed-rs/actions/workflows/release.yml"><img src="https://github.com/Anush008/fastembed-rs/actions/workflows/release.yml/badge.svg?branch=main" alt="Semantic release"></a>
</div>

This LastDB fork keeps text embeddings only. Image embeddings, reranking, and sparse text embeddings are removed.

## 🍕 Features

- Supports synchronous usage. No dependency on Tokio.
- Uses [@pykeio/ort](https://github.com/pykeio/ort) for performant ONNX inference.
- Uses [@huggingface/tokenizers](https://github.com/huggingface/tokenizers) for fast encodings.
- Supports batch embeddings generation with parallelism using [@rayon-rs/rayon](https://github.com/rayon-rs/rayon).

## 🔍 Not looking for Rust?

- Python 🐍: [fastembed](https://github.com/qdrant/fastembed)
- Go 🐳: [fastembed-go](https://github.com/Anush008/fastembed-go)
- JavaScript 🌐: [fastembed-js](https://github.com/Anush008/fastembed-js)

## 🤖 Models

### Text Embedding

- [**sentence-transformers/all-MiniLM-L6-v2**](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) - Default (`Qdrant/all-MiniLM-L6-v2-onnx`, `model.onnx`)

### Sparse Text Embedding

- [**prithivida/Splade_PP_en_v1**](https://huggingface.co/prithivida/Splade_PP_en_v1) - Default

### Image Embedding

- [**Qdrant/clip-ViT-B-32-vision**](https://huggingface.co/Qdrant/clip-ViT-B-32-vision) - Default
- [**Qdrant/resnet50-onnx**](https://huggingface.co/Qdrant/resnet50-onnx)
- [**Qdrant/Unicom-ViT-B-16**](https://huggingface.co/Qdrant/Unicom-ViT-B-16)
- [**Qdrant/Unicom-ViT-B-32**](https://huggingface.co/Qdrant/Unicom-ViT-B-32)
- [**nomic-ai/nomic-embed-vision-v1.5**](https://huggingface.co/nomic-ai/nomic-embed-vision-v1.5)

### Reranking

- [**BAAI/bge-reranker-base**](https://huggingface.co/BAAI/bge-reranker-base) - Default
- [**BAAI/bge-reranker-v2-m3**](https://huggingface.co/BAAI/bge-reranker-v2-m3)
- [**jinaai/jina-reranker-v1-turbo-en**](https://huggingface.co/jinaai/jina-reranker-v1-turbo-en)
- [**jinaai/jina-reranker-v2-base-multiligual**](https://huggingface.co/jinaai/jina-reranker-v2-base-multilingual)

## 🚀 Installation

Run the following command in your project directory:

```bash
cargo add fastembed
```

Or add the following line to your Cargo.toml:

```toml
[dependencies]
fastembed = "4"
```

## 📖 Usage

### Text Embeddings

```rust
use fastembed::{TextEmbedding, InitOptions, EmbeddingModel};

// With default InitOptions
let model = TextEmbedding::try_new(Default::default())?;

// With custom InitOptions
let model = TextEmbedding::try_new(
    InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(true),
)?;

let documents = vec![
    "passage: Hello, World!",
    "query: Hello, World!",
    "passage: This is an example passage.",
    // You can leave out the prefix but it's recommended
    "fastembed-rs is licensed under Apache  2.0"
    ];

 // Generate embeddings with the default batch size, 256
 let embeddings = model.embed(documents, None)?;

 println!("Embeddings length: {}", embeddings.len()); // -> Embeddings length: 4
 println!("Embedding dimension: {}", embeddings[0].len()); // -> Embedding dimension: 384

```

Alternatively, local model files can be used for inference via the `try_new_from_user_defined(...)` methods of respective structs.

## ✊ Support

To support the library, please donate to our primary upstream dependency, [`ort`](https://github.com/pykeio/ort?tab=readme-ov-file#-sponsor-ort) - The Rust wrapper for the ONNX runtime.

## 📄 LICENSE

Apache 2.0 © [2024](https://github.com/Anush008/fastembed-rs/blob/main/LICENSE)
