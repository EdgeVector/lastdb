//! Authenticated restore instructions. Native files retain their stored bytes.

use crate::{DecimalU64, Digest, Id};
use serde::{Deserialize, Serialize};

/// Service-visible summary. Exact CURRENT and home bytes remain authoritative
/// for directory roots, codecs, logical markers and all native format details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoot {
    /// Id of the backed-up home.
    pub home_id: Id,
    /// Digest of the home.
    pub home_digest: Digest,
    /// SHA-256 of the home's CURRENT list.
    pub current_sha256: Digest,
    /// Home generation.
    pub generation: DecimalU64,
    /// Native frontier.
    pub frontier: DecimalU64,
    /// Feature bits of the run home that wrote this root.
    pub required_features: DecimalU64,
}
