//! The internal-index schema allow-list — the schemas hidden from native-index
//! search results unless the caller passes `include_internal=true`.
//!
//! The fingerprint / graph subsystem canonicalizes its schemas to content-hashed
//! names (`sh_…`) while keeping a stable human-readable `descriptive_name`, so
//! [`is_internal_index_schema`] checks BOTH the canonical and descriptive name;
//! the list carries both forms where they differ. This is the single source of
//! truth for the list both socket route executors filter against.

/// Index schemas hidden from native-index search unless `include_internal=true`.
pub const INTERNAL_INDEX_SCHEMAS: &[&str] = &[
    "Fingerprint",
    "Mention",
    "MentionBySource",
    "MentionByFingerprint",
    "Edge",
    "EdgeByFingerprint",
    "ExtractionStatus",
    "IngestionError",
    "TriggerFiring",
    "ai_conversations",
    "AI Conversations",
    "ExtractionRule",
];
