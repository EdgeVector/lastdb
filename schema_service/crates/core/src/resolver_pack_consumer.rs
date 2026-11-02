//! Dev/off-by-default local consumer for signed Schema Resolver Packs.
//!
//! The consumer is deliberately transport-agnostic: callers inject an object
//! store that can read R2 keys and a [`ResolverPackCache`] for artifact bytes.
//! This module owns compatibility checks, last-known-good decisions, and
//! local-vs-live routing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::clock::Clock;

use app_identity_crypto::{Env, VerifyError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::resolver_config::{
    ResolverConfig, NATIVE_COMPONENT_COVER_ALGORITHM_ID, NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
};
use crate::resolver_pack::{
    latest_compatible_manifest_pointer_key, parse_embedding_artifact, parse_manifest,
    parse_resolver_config, parse_schema_snapshot_artifact, resolver_pack_artifact_key,
    verify_resolver_pack_manifest, EmbeddingArtifact, ResolverPackArtifactKind,
    ResolverPackManifest, ResolverPackVerifyError, SchemaSnapshotArtifact, TrustedResolverPackKey,
    SUPPORTED_RESOLVER_CONTRACT_VERSION,
};
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
    cache: Arc<dyn ResolverPackCache>,
    config: ResolverPackConsumerConfig,
    clock: Arc<dyn Clock>,
    telemetry: Arc<Mutex<ResolverPackTelemetrySnapshot>>,
}
