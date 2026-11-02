//! Resolver pack consumer types.
//!
//! An object store reads pack bytes. A [`ResolverPackCache`] holds artifact bytes.

use std::collections::BTreeMap;

use app_identity_crypto::Env;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::resolver_config::ResolverConfig;
use crate::resolver_pack::{
    EmbeddingArtifact, ResolverPackArtifactKind, ResolverPackManifest, ResolverPackVerifyError,
    SchemaSnapshotArtifact, TrustedResolverPackKey,
};

mod config;
pub use config::*;
mod types;
pub use types::*;
mod cache;
pub use cache::*;
