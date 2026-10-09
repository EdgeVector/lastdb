//! Bounded rewrite of existing sealed KV rows to an explicit ENB target.
//!
//! Flipping the process write switches only affects new puts. Atom rows are
//! immutable, so existing `ENC:` envelopes stay until an explicit sweep
//! rewrites them under the same key. This module is that sweep: dry-run first,
//! one plane per call, same-key value replace, no dual-key copies.
//!
//! Writes go to the inner KvStore (already-sealed bytes), so they do not
//! consult the process write switches and they do not enter capture put-absorb.

use crate::storage::traits::PhysicalScanCursor;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Planes this verb may rewrite. `cas_blobs` is intentionally absent.
///
/// Extend the list in a later card if another sealed plane needs the same
/// pass. Unknown names return a skipped report, they do not scan.
pub const RESEAL_AT_REST_ALLOWLIST: &[&str] = &[
    "atoms",
    "tips",
    "indexes",
    "metadata",
    "change_feed",
    "atom_locators",
    "field_update_order_log",
];

/// Metadata key prefix for the durable per-plane resume checkpoint.
pub const RESEAL_CHECKPOINT_KEY_PREFIX: &str = "amigr:reseal_at_rest_v2:";

/// Retired checkpoint prefix. V1 did not record target semantics and is never
/// safe to resume, but metadata walks must still exclude its rows.
pub const RESEAL_LEGACY_CHECKPOINT_KEY_PREFIX: &str = "amigr:reseal_at_rest_v1:";

/// Durable checkpoint schema version.
pub const RESEAL_CHECKPOINT_VERSION: u8 = 2;

/// Qualification rules for the current targets.
pub const RESEAL_TARGET_FORMAT_VERSION: u8 = 1;

/// Namespace that holds [`RESEAL_CHECKPOINT_KEY_PREFIX`] rows.
pub const RESEAL_CHECKPOINT_NAMESPACE: &str = "metadata";

/// Exclusive upper bound for a whole-collection physical walk.
///
/// Product keys are ASCII prefixes (`atom:`, `mk:`, `aloc:`, …). Four `0xFF`
/// bytes sit past that keyspace without pretending an empty prefix has a
/// string successor (it does not — see `FilterUtils::create_prefix_end`).
pub const RESEAL_COLLECTION_END: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

/// Default row cap for one invocation.
pub const RESEAL_DEFAULT_MAX_ROWS: usize = 8_192;

/// Default wall-clock cap for one invocation, in seconds.
pub const RESEAL_DEFAULT_MAX_SECS: u64 = 45;

/// Rows requested from one physical page.
pub const RESEAL_PAGE_ROWS: usize = 256;

/// Physical handles resolved per inner page. One handle keeps shard loads
/// bounded the same way `gc-atoms` prune does.
pub const RESEAL_PAGE_HANDLES: usize = 1;

/// Required envelope policy for one reseal pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResealAtRestTarget {
    /// Emit uncompressed ENB envelopes.
    Binary,
    /// Emit ENB envelopes and deflate when the payload qualifies.
    #[default]
    BinaryCompress,
}

/// Options for one `reseal-at-rest` invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResealAtRestOptions {
    /// One allowlisted collection. Required.
    pub collection: String,
    /// Target envelope policy. The compatibility default matches the old verb.
    #[serde(default)]
    pub target: ResealAtRestTarget,
    /// Default true. `--execute` sets this false.
    #[serde(default = "default_true")]
    pub dry_run: bool,
    /// Cap on rows decided this call. Absent = [`RESEAL_DEFAULT_MAX_ROWS`].
    #[serde(default)]
    pub max_rows: Option<usize>,
    /// Cap on wall time this call. Absent = [`RESEAL_DEFAULT_MAX_SECS`].
    #[serde(default)]
    pub max_secs: Option<u64>,
    /// Report the durable checkpoint and exit. No scan, no writes.
    #[serde(default)]
    pub progress_only: bool,
    /// Drop the durable checkpoint and start at the first key.
    #[serde(default)]
    pub restart: bool,
}

fn default_true() -> bool {
    true
}

impl Default for ResealAtRestOptions {
    fn default() -> Self {
        Self {
            collection: String::new(),
            target: ResealAtRestTarget::default(),
            dry_run: true,
            max_rows: None,
            max_secs: None,
            progress_only: false,
            restart: false,
        }
    }
}

/// Durable resume state, stored under [`RESEAL_CHECKPOINT_KEY_PREFIX`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResealAtRestCheckpoint {
    pub version: u8,
    pub target: ResealAtRestTarget,
    pub format_version: u8,
    pub collection: String,
    #[serde(default)]
    pub cursor: Option<PhysicalScanCursor>,
    #[serde(default)]
    pub rows_scanned_total: u64,
    #[serde(default)]
    pub rows_converted_total: u64,
    #[serde(default)]
    pub bytes_before_total: u64,
    #[serde(default)]
    pub bytes_after_total: u64,
    #[serde(default)]
    pub completed: bool,
    pub updated_at: DateTime<Utc>,
}

/// One invocation's report. Always `heavy: true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResealAtRestReport {
    pub collection: String,
    pub target: ResealAtRestTarget,
    pub format_version: u8,
    pub dry_run: bool,
    /// RFC 3339 timestamp when this invocation finished.
    pub measured_at: DateTime<Utc>,
    /// Always true. This is an admin walk, not a cheap status gauge.
    pub heavy: bool,
    pub rows_scanned: u64,
    /// Rows that already satisfy the requested target.
    pub rows_already_target: u64,
    /// Rows this invocation planned or converted.
    pub rows_to_convert: u64,
    /// Rows this invocation rewrote (zero on dry-run).
    pub rows_converted: u64,
    /// Row changed between scan and replace; skipped this pass.
    pub rows_cas_skipped: u64,
    /// Decode or decrypt failed (including unknown-key rows). Left intact.
    pub rows_unreadable: u64,
    /// No `ENC:`/`ENB:`/`ENZ:` prefix. Not this verb's job.
    pub rows_plaintext: u64,
    /// Sealed value bytes before rewrite (or the dry-run plan).
    pub bytes_before: u64,
    /// Sealed value bytes of those same rows after the target encoding.
    pub bytes_after: u64,
    pub more_remaining: bool,
    /// Exclusive physical resume position for the next invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PhysicalScanCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<ResealAtRestCheckpoint>,
}

impl ResealAtRestReport {
    pub fn empty(collection: String, dry_run: bool, target: ResealAtRestTarget) -> Self {
        Self {
            collection,
            target,
            format_version: RESEAL_TARGET_FORMAT_VERSION,
            dry_run,
            measured_at: Utc::now(),
            heavy: true,
            rows_scanned: 0,
            rows_already_target: 0,
            rows_to_convert: 0,
            rows_converted: 0,
            rows_cas_skipped: 0,
            rows_unreadable: 0,
            rows_plaintext: 0,
            bytes_before: 0,
            bytes_after: 0,
            more_remaining: false,
            next_cursor: None,
            skipped_reason: None,
            checkpoint: None,
        }
    }
}

/// True when `name` is on [`RESEAL_AT_REST_ALLOWLIST`].
#[must_use]
pub fn collection_is_allowed(name: &str) -> bool {
    RESEAL_AT_REST_ALLOWLIST.contains(&name)
}

/// Metadata key for one plane's checkpoint.
#[must_use]
pub fn checkpoint_key(collection: &str) -> String {
    format!("{RESEAL_CHECKPOINT_KEY_PREFIX}{collection}")
}

/// Retired V1 metadata key for one plane.
#[must_use]
pub fn legacy_checkpoint_key(collection: &str) -> String {
    format!("{RESEAL_LEGACY_CHECKPOINT_KEY_PREFIX}{collection}")
}

/// True when a metadata key is a reseal checkpoint (skip during metadata walk).
#[must_use]
pub fn is_checkpoint_key(key: &[u8]) -> bool {
    key.starts_with(RESEAL_CHECKPOINT_KEY_PREFIX.as_bytes())
        || key.starts_with(RESEAL_LEGACY_CHECKPOINT_KEY_PREFIX.as_bytes())
}
