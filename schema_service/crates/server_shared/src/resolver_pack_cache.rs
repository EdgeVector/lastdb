//! Disk adapter for [`schema_service_core::ResolverPackCache`].
//!
//! Path layout matches the previous in-core cache: artifacts under
//! `artifacts/sha256/<hash>/`, last-known-good files under `last_known_good/`,
//! writes staged to a sibling `.tmp` file and renamed into place.

use std::io;
use std::path::{Path, PathBuf};

use schema_service_core::resolver_pack::ResolverPackArtifactKind;
use schema_service_core::{ResolverBootstrapConfig, ResolverPackCache};

/// Filesystem cache for resolver pack artifacts and last-known-good files.
#[derive(Debug, Clone)]
pub struct FsResolverPackCache {
    root: PathBuf,
}

impl FsResolverPackCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Open the identity directory `bootstrap` already computed.
    pub fn from_bootstrap(bootstrap: &ResolverBootstrapConfig) -> Self {
        Self::new(bootstrap.cache_identity_dir())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn artifact_path(&self, kind: ResolverPackArtifactKind, hash: &str) -> PathBuf {
        let filename = match kind {
            ResolverPackArtifactKind::Manifest => "manifest.json",
            ResolverPackArtifactKind::SchemaSnapshot => "schema_snapshot.json",
            ResolverPackArtifactKind::EmbeddingArtifact => "embedding_artifact.json",
            ResolverPackArtifactKind::ResolverConfig => "resolver_config.json",
        };
        self.root
            .join("artifacts")
            .join("sha256")
            .join(hash)
            .join(filename)
    }
}

impl ResolverPackCache for FsResolverPackCache {
    fn get_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
    ) -> Result<Option<Vec<u8>>, io::Error> {
        read_optional(&self.artifact_path(kind, hash))
    }

    fn put_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
        bytes: &[u8],
    ) -> Result<(), io::Error> {
        atomic_write(&self.artifact_path(kind, hash), bytes)
    }

    fn last_known_good_manifest_bytes(&self) -> Result<Option<Vec<u8>>, io::Error> {
        self.last_known_good_file("manifest.json")
    }

    fn last_known_good_file(&self, filename: &str) -> Result<Option<Vec<u8>>, io::Error> {
        read_optional(&self.root.join("last_known_good").join(filename))
    }

    fn put_last_known_good(
        &self,
        manifest_bytes: &[u8],
        resolver_config_bytes: &[u8],
        schema_snapshot_bytes: &[u8],
        embedding_artifact_bytes: &[u8],
    ) -> Result<(), io::Error> {
        let root = self.root.join("last_known_good");
        atomic_write(&root.join("manifest.json"), manifest_bytes)?;
        atomic_write(&root.join("resolver_config.json"), resolver_config_bytes)?;
        atomic_write(&root.join("schema_snapshot.json"), schema_snapshot_bytes)?;
        atomic_write(
            &root.join("embedding_artifact.json"),
            embedding_artifact_bytes,
        )?;
        Ok(())
    }
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, io::Error> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)
}
