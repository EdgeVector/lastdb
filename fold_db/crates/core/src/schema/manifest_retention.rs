//! The `retention` key of an app manifest schema entry.
//!
//! This is a request to the node, not part of the schema. It is parsed beside
//! [`crate::schema::types::declarative_schemas::DeclarativeSchemaDefinition`]
//! and never stored in it, so it cannot change a schema artifact, its identity
//! hash, or canonicalization. The identity hash covers the owner app, the
//! readable name, the sorted field names and the key layout, and nothing else.

use serde::{Deserialize, Serialize};

/// The longest TTL a manifest may declare: ten years.
///
/// A larger value is almost certainly a unit mistake (milliseconds given as
/// seconds), and a TTL that long never expires anything in practice.
pub const MAX_MANIFEST_TTL_SECONDS: u64 = 3650 * SECONDS_PER_DAY;

const SECONDS_PER_DAY: u64 = 86_400;

/// `"retention": { "ttl_days": 90 }` or `"retention": { "ttl_seconds": 120 }`.
///
/// Give exactly one of the two keys. Any other key is an error, so a
/// misspelled key can never leave a schema without the TTL the author meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRetention {
    /// Maximum age in whole days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_days: Option<u64>,
    /// Maximum age in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
}

impl ManifestRetention {
    /// Parse the JSON value of a manifest `retention` key and validate it.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        let parsed: Self = serde_json::from_value(value.clone()).map_err(|e| {
            format!(
                "invalid retention: {e}; write {{\"ttl_days\": 90}} or {{\"ttl_seconds\": 120}}"
            )
        })?;
        parsed.ttl()?;
        Ok(parsed)
    }

    /// The TTL in seconds. Rejects zero, a missing or doubled key, an
    /// overflow, and anything above [`MAX_MANIFEST_TTL_SECONDS`].
    pub fn ttl(&self) -> Result<u64, String> {
        let seconds = match (self.ttl_days, self.ttl_seconds) {
            (Some(days), None) => days.checked_mul(SECONDS_PER_DAY),
            (None, Some(seconds)) => Some(seconds),
            (None, None) => {
                return Err("retention needs ttl_days or ttl_seconds".to_string());
            }
            (Some(_), Some(_)) => {
                return Err("retention takes ttl_days or ttl_seconds, not both".to_string());
            }
        };
        match seconds {
            Some(0) => Err("retention ttl must be greater than zero".to_string()),
            Some(seconds) if seconds <= MAX_MANIFEST_TTL_SECONDS => Ok(seconds),
            _ => Err(format!(
                "retention ttl is too large: the limit is {MAX_MANIFEST_TTL_SECONDS} seconds (3650 days)"
            )),
        }
    }
}

/// What applying a manifest retention to a schema did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestRetentionStatus {
    /// No policy was in force. The manifest policy is in force now.
    Applied,
    /// A manifest policy with another TTL was in force. The TTL changed.
    Updated,
    /// The same manifest policy was already in force. Nothing changed.
    Unchanged,
    /// An operator policy is in force and wins. The manifest TTL is kept and
    /// returns when the operator clears the operator policy.
    OperatorOverride,
}

/// Result of [`crate::db_operations::schema_store::SchemaStore::apply_manifest_retention`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestRetentionOutcome {
    /// The schema that holds the retention state for both installed names.
    pub anchor: String,
    pub status: ManifestRetentionStatus,
    /// The policy in force after the call.
    pub effective: crate::schema::SchemaRetentionPolicy,
}
