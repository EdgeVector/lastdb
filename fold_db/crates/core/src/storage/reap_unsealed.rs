//! Bounded removal of un-enveloped rows from namespaces LastDB encrypts.
//!
//! `decision-2026-09-14-drop-dual-read-unsealed-is-gone`: a value without an
//! at-rest envelope (`ENC:` / `ENZ:` / `ENB:`) in an encrypted namespace is not
//! user data. The read path treats it as absent and deliberately does not
//! delete it, because a read that writes is exactly the defect dual-read had
//! (a stray plaintext row laundered into a durable one on first read). So the
//! bytes stay on disk, travel into every backup cut, and land in every
//! restore, where they are equally invisible and equally permanent.
//!
//! This module is the other half of that decision: an owner-triggered,
//! dry-run-first, bounded, resumable pass that removes those rows and reports
//! how many it removed and how many bytes it returned. It is modelled on
//! `reseal_at_rest`, which walks the same physical surface with the same
//! checkpoint shape.
//!
//! What it never touches:
//!
//! - A sealed row, readable or not. A row that carries an envelope but does not
//!   open under this node's key is *ours and unopenable* (an org row, a rotated
//!   key), not *not ours*. The pass classifies on the envelope prefix alone and
//!   never decrypts, so it cannot confuse the two.
//! - A plaintext-by-policy namespace (`LASTSTORE_PLAINTEXT_NAMESPACES`) or the
//!   reserved marker namespace. Every row there is legitimately un-enveloped;
//!   removing them destroys the schema catalog. The pass refuses those by name
//!   before it opens anything, on top of the structural guarantee that the
//!   encrypting seam never wraps them.
//!
//! Cadence: on request only. On a healthy home the correct count is zero, and
//! a non-zero count is a signal worth a human read (a restore that replayed
//! unsealed, an upgrade that wrote past the seam) before the bytes go away.
//! Do not schedule this until a real home has been measured.

use crate::storage::traits::PhysicalScanCursor;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Metadata key prefix for the durable per-plane resume checkpoint.
pub const REAP_CHECKPOINT_KEY_PREFIX: &str = "amigr:reap_unsealed_v1:";

/// Namespace that holds [`REAP_CHECKPOINT_KEY_PREFIX`] rows. Same plane as the
/// reseal checkpoint so one metadata walk skips both families.
pub const REAP_CHECKPOINT_NAMESPACE: &str = "metadata";

/// Default row cap for one invocation.
pub const REAP_DEFAULT_MAX_ROWS: usize = 8_192;

/// Default wall-clock cap for one invocation, in seconds.
pub const REAP_DEFAULT_MAX_SECS: u64 = 45;

/// Options for one `reap-unsealed` invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReapUnsealedOptions {
    /// One allowlisted, encrypted collection. Required.
    pub collection: String,
    /// Default true. `--execute` sets this false.
    #[serde(default = "default_true")]
    pub dry_run: bool,
    /// Cap on rows decided this call. Absent = [`REAP_DEFAULT_MAX_ROWS`].
    #[serde(default)]
    pub max_rows: Option<usize>,
    /// Cap on wall time this call. Absent = [`REAP_DEFAULT_MAX_SECS`].
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

impl Default for ReapUnsealedOptions {
    fn default() -> Self {
        Self {
            collection: String::new(),
            dry_run: true,
            max_rows: None,
            max_secs: None,
            progress_only: false,
            restart: false,
        }
    }
}

/// Durable resume state, stored under [`REAP_CHECKPOINT_KEY_PREFIX`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReapUnsealedCheckpoint {
    pub collection: String,
    #[serde(default)]
    pub cursor: Option<PhysicalScanCursor>,
    #[serde(default)]
    pub rows_scanned_total: u64,
    #[serde(default)]
    pub rows_removed_total: u64,
    #[serde(default)]
    pub bytes_reclaimed_total: u64,
    #[serde(default)]
    pub completed: bool,
    pub updated_at: DateTime<Utc>,
}

/// One invocation's report. Always `heavy: true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReapUnsealedReport {
    pub collection: String,
    pub dry_run: bool,
    /// RFC 3339 timestamp when this invocation finished.
    pub measured_at: DateTime<Utc>,
    /// Always true. This is an admin walk, not a cheap status gauge.
    pub heavy: bool,
    pub rows_scanned: u64,
    /// Carried an at-rest envelope (any of `ENC:` / `ENZ:` / `ENB:`). Left
    /// untouched whether or not it opens under this node's key.
    pub rows_sealed: u64,
    /// No envelope. Planned for removal (dry-run) or removed (execute).
    pub rows_unsealed: u64,
    /// Rows this invocation deleted (zero on dry-run).
    pub rows_removed: u64,
    /// Row changed between scan and delete; skipped this pass.
    pub rows_cas_skipped: u64,
    /// Stored value bytes of the rows removed (or, on dry-run, planned).
    pub bytes_reclaimed: u64,
    pub more_remaining: bool,
    /// Exclusive physical resume position for the next invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PhysicalScanCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<ReapUnsealedCheckpoint>,
}

impl ReapUnsealedReport {
    pub fn empty(collection: String, dry_run: bool) -> Self {
        Self {
            collection,
            dry_run,
            measured_at: Utc::now(),
            heavy: true,
            rows_scanned: 0,
            rows_sealed: 0,
            rows_unsealed: 0,
            rows_removed: 0,
            rows_cas_skipped: 0,
            bytes_reclaimed: 0,
            more_remaining: false,
            next_cursor: None,
            skipped_reason: None,
            checkpoint: None,
        }
    }
}

/// Metadata key for one plane's checkpoint.
#[must_use]
pub fn reap_checkpoint_key(collection: &str) -> String {
    format!("{REAP_CHECKPOINT_KEY_PREFIX}{collection}")
}

/// True when a metadata key is a reap checkpoint (skip during metadata walk).
#[must_use]
pub fn is_reap_checkpoint_key(key: &[u8]) -> bool {
    key.starts_with(REAP_CHECKPOINT_KEY_PREFIX.as_bytes())
}
