//! Access settings and opaque entries for private contribution streams.
use super::Stream;
use serde::{Deserialize, Serialize};

/// Largest sequence representable exactly by the canonical JSON transport.
pub const MAX_CONTRIBUTION_SEQUENCE: u64 = (1 << 53) - 1;
/// Largest encoded ciphertext admitted for one contribution.
pub const MAX_CONTRIBUTION_CIPHERTEXT_BYTES: usize = 16 * 1024;
/// Maximum unacknowledged contributions in one private stream.
pub const MAX_PENDING_CONTRIBUTIONS: usize = 8;

/// Who may download a stream. Append access is authorized separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadAudience {
    /// The owner and recipients authorized for this publication.
    PublicationReaders,
    /// The owner of this stream's scope.
    ScopeOwner,
}

/// Set one stream's read audience. The service echoes the saved setting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetReadAudience {
    /// Exact account scope and stream ID.
    pub stream: Stream,
    /// Audience for downloads of this stream.
    pub read_audience: ReadAudience,
}

/// One encrypted pending contribution, identified by its stream sequence.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContributionEntry {
    /// Position of this contribution in its stream.
    pub sequence: u64,
    /// URL-safe unpadded base64 representation of encrypted bytes.
    pub ciphertext: String,
}
