//! Byte cache for resolver pack artifacts and last-known-good files.
//!
//! The registry core decides which bytes to keep. The shell supplies the
//! storage. The disk adapter lives in `schema_service_server_shared`.

use super::*;

/// Changed-artifact and last-known-good bytes for one resolver pack consumer.
pub trait ResolverPackCache: Send + Sync {
    fn get_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
    ) -> Result<Option<Vec<u8>>, std::io::Error>;

    fn put_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
        bytes: &[u8],
    ) -> Result<(), std::io::Error>;

    fn last_known_good_manifest_bytes(&self) -> Result<Option<Vec<u8>>, std::io::Error>;

    fn last_known_good_file(&self, filename: &str) -> Result<Option<Vec<u8>>, std::io::Error>;

    /// Persist verified raw artifact bytes for cold-start LKG re-verification.
    ///
    /// Stores the exact verified bytes (not re-serialized structs) so content
    /// hashes continue to match on reload.
    fn put_last_known_good(
        &self,
        manifest_bytes: &[u8],
        resolver_config_bytes: &[u8],
        schema_snapshot_bytes: &[u8],
        embedding_artifact_bytes: &[u8],
    ) -> Result<(), std::io::Error>;
}
