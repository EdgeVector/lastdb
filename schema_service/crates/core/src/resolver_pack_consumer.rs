//! Dev/off-by-default local consumer for signed Schema Resolver Packs.
//!
//! The consumer is deliberately transport-agnostic: callers inject an object
//! store that can read R2 keys, while this module owns compatibility checks,
//! changed-artifact caching, last-known-good persistence, and local-vs-live
//! routing decisions.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use app_identity_crypto::{Env, VerifyError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::resolver_config::{
    ResolverConfig, NATIVE_COMPONENT_COVER_ALGORITHM_ID, NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
};
use crate::resolver_pack::{
    artifact_sha256_hex, latest_compatible_manifest_pointer_key, parse_embedding_artifact,
    parse_manifest, parse_resolver_config, parse_schema_snapshot_artifact,
    resolver_pack_artifact_key, verify_resolver_pack_manifest, EmbeddingArtifact,
    ResolverPackArtifactKind, ResolverPackManifest, ResolverPackVerifyError,
    SchemaSnapshotArtifact, TrustedResolverPackKey, RESOLVER_PACK_FORMAT_VERSION,
    SUPPORTED_RESOLVER_CONTRACT_VERSION,
};
use crate::schema_resolver_abi::{RegistryEmbeddings, ResolverDecision};

mod config;
pub use config::*;
mod types;
pub use types::*;
mod cache;
mod consumer_load;
mod consumer_support;
pub use cache::*;
mod helpers;
use helpers::*;

pub struct ResolverPackConsumer<S> {
    store: S,
    cache: FsResolverPackCache,
    config: ResolverPackConsumerConfig,
    telemetry: Arc<Mutex<ResolverPackTelemetrySnapshot>>,
}
