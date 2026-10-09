//! schema_service_server_shared
//!
//! Framework-shared HTTP handlers for the schema service. The actix dev
//! binary in `schema_service_server_http` and the Lambda in
//! `schema_service_server_lambda` both mount these handlers so the two
//! surfaces cannot drift.
//!
//! Phase 2 of `projects/extract-schema-service-repo` moved the registry
//! brain out of `fold_db::schema_service::*` into `schema_service_core`;
//! this crate now re-exports core's modules under the original names and
//! optionally adds [`FoldDbFastEmbedder`], the fastembed-backed
//! [`schema_service_core::Embedder`] implementation that binaries inject
//! at construction when compiled with the explicit `fastembed` feature.

#[cfg(any(feature = "fastembed", feature = "fastembed-layer"))]
pub mod fastembed_adapter;
#[cfg(feature = "actix")]
pub mod handlers;

#[cfg(any(feature = "fastembed", feature = "fastembed-layer"))]
pub use fastembed_adapter::FoldDbFastEmbedder;
pub use schema_service_core::Embedder;
pub use schema_service_core::{builtin_schemas, state, types};
