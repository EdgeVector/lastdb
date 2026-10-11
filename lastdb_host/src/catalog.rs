//! The internal-index schema allow-list — the schemas hidden from native-index
//! search results unless the caller passes `include_internal=true`.
//!
//! The fingerprint / graph subsystem canonicalizes its schemas to content-hashed
//! names (`sh_…`) while keeping a stable human-readable `descriptive_name`, so
//! [`is_internal_index_schema`] checks BOTH the canonical and descriptive name;
//! the list carries both forms where they differ. This is the single source of
//! truth for the list both socket route executors filter against.
