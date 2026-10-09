//! Filesystem cache for resolver pack artifacts and last-known-good files.

use super::*;

#[derive(Debug, Clone)]
pub struct FsResolverPackCache {
    pub(super) root: PathBuf,
}

impl FsResolverPackCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn get_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.artifact_path(kind, hash);
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn put_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        let path = self.artifact_path(kind, hash);
        atomic_write(&path, bytes)
    }

    pub fn last_known_good_manifest_bytes(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.last_known_good_file("manifest.json")
    }

    pub fn last_known_good_file(&self, filename: &str) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.root.join("last_known_good").join(filename);
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Persist verified raw artifact bytes for cold-start LKG re-verification.
    ///
    /// Stores the exact verified bytes (not re-serialized structs) so content
    /// hashes continue to match on reload.
    pub(super) fn put_last_known_good(
        &self,
        manifest_bytes: &[u8],
        resolver_config_bytes: &[u8],
        schema_snapshot_bytes: &[u8],
        embedding_artifact_bytes: &[u8],
    ) -> Result<(), std::io::Error> {
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

    pub(super) fn artifact_path(&self, kind: ResolverPackArtifactKind, hash: &str) -> PathBuf {
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
