//! Durable aggregate meter state, schema slices, and their key names.

use super::*;

/// Durable projection written to the `keep_small` collection under
/// [`KEEP_SMALL_SNAPSHOT_KEY`]. The durable row is a small header; the
/// per-schema maps live at [`KEEP_SMALL_SCHEMA_SHARD_PREFIX`] keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallSnapshot {
    #[serde(default)]
    pub version: u32,
    pub schemas: HashMap<String, SchemaMeter>,
    /// `Some(true)` only when the shutdown flush wrote this snapshot and no
    /// process has booted on the home since. Every other writer — the
    /// debounced persist, an initial hard-erase baseline or bootstrap flush, and the
    /// re-arm put hydrate makes right after reading a clean snapshot — writes
    /// `Some(false)`. So a `false` at hydrate means the previous process
    /// stopped without its shutdown flush, and metered writes after its last
    /// debounce tick are missing: the counters are a hint, not a measurement.
    /// `None` is the empty-home absent state: no verdict. Soundness does not
    /// need a live epoch after the restart, only that the last writer of this
    /// row is known.
    pub clean_stop: Option<bool>,
    /// Independent provenance for the global and schema counters.
    pub trust: MeterTrustPayload,
}

/// One schema's durable meter slice. A persist writes only the schemas that
/// changed, so bytes on disk do not grow with the number of measured schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallSchemaShard {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaMeter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<MeterDomainTrust>,
}

pub const KEEP_SMALL_SNAPSHOT_VERSION: u32 = 2;

/// Metadata key for [`KeepSmallSnapshot`]. Point get/put — not a store walk.
///
/// This row is a small header (totals, trust, clean-stop). Per-schema maps
/// live at [`KEEP_SMALL_SCHEMA_SHARD_PREFIX`].
pub const KEEP_SMALL_SNAPSHOT_KEY: &str = "keep_small:meters";
/// Prefix for one [`KeepSmallSchemaShard`] per measured schema.
pub const KEEP_SMALL_SCHEMA_SHARD_PREFIX: &str = "keep_small:meters:schema:";

pub(super) const HEX: &[u8; 16] = b"0123456789abcdef";

/// Durable key for one schema's meter shard.
#[must_use]
pub fn keep_small_schema_shard_key(schema: &str) -> String {
    let mut out = String::with_capacity(
        KEEP_SMALL_SCHEMA_SHARD_PREFIX.len() + schema.len().saturating_mul(2),
    );
    out.push_str(KEEP_SMALL_SCHEMA_SHARD_PREFIX);
    for &byte in schema.as_bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
