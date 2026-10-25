//! Passive schema-key classification for local compaction policy.

use super::catalog_keys::*;
use super::SchemaStore;

impl SchemaStore {
    /// Only schema cache authority needs replayable bodies. Retention and
    /// janitor receipts keep their existing physical capture policy.
    pub(crate) fn is_replay_authority_key(namespace: &str, key: &[u8]) -> bool {
        match namespace {
            "schemas" | "schema_superseded_by" => true,
            "schema_states" => !other_schema_metadata_prefixes()
                .iter()
                .any(|prefix| key.starts_with(prefix.as_bytes())),
            _ => false,
        }
    }
}

fn other_schema_metadata_prefixes() -> [&'static str; 6] {
    [
        RETENTION_POLICY_KEY_PREFIX,
        RETENTION_ANCHOR_KEY_PREFIX,
        RETENTION_HASH_PARTITION_KEY_PREFIX,
        RETENTION_AGE_KEY_PREFIX,
        RETENTION_AGE_LATEST_PREFIX,
        SCHEMA_DROP_RECEIPT_KEY_PREFIX,
    ]
}
