use super::BackupChunkRef;
use serde::{Deserialize, Serialize};

/// One original file's location in a byte-for-byte cloud pack.
/// The original digest stays in `BackupChunkRef::sha256`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupPackLocation {
    pub sha256: String,
    pub offset: u64,
    pub length: u64,
    /// Total bytes in the pack object.
    pub bytes: u64,
}

impl BackupChunkRef {
    #[must_use]
    pub fn object_sha256(&self) -> &str {
        self.pack.as_ref().map_or(&self.sha256, |pack| &pack.sha256)
    }
}
