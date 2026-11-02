//! Audit facts about completed record erasures.
//!
//! A native erase stages one final audit row in the same snapshot batch as
//! its data removals. The row has no recovery role. It never drives replay,
//! repairs a counter, or authorizes another delete.
//!
//! The row retains counts, caller, schema, time, and a key fingerprint. It
//! retains no field value, record UUIDs, or plaintext subject key. Existing
//! version-1 attempted-operation rows remain historical audit facts only.
//! Audit rows have no automatic retention policy.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::RecordStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

/// Bare key prefix for ledger rows. Rows sort in time order under a prefix scan
/// because the timestamp component is fixed-width zero-padded nanoseconds.
pub const RECORD_DELETE_LEDGER_PREFIX: &str = "dellog:";

/// Which path performed the delete.
pub const LEDGER_VERB_PURGE: &str = "purge";
pub const LEDGER_VERB_DELETE: &str = "delete";
/// Live `Delete` converge. It counts its records in `records_converged`.
///
/// Deliberately NOT [`LEDGER_VERB_DELETE`]: converge reclaims no record body,
/// so the converge lane gets its own verb.
pub const LEDGER_VERB_DELETE_CONVERGE: &str = "delete-converge";

/// One audit row per hard-delete batch. Version 2 joins the native erase
/// atomically and always has `committed: true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDeleteLedgerEntry {
    /// Row format version. Bump when fields change meaning, not when adding.
    pub version: u32,
    /// [`LEDGER_VERB_PURGE`], [`LEDGER_VERB_DELETE`] or [`LEDGER_VERB_DELETE_CONVERGE`].
    pub verb: String,
    /// When the batch was planned (RFC3339). Matches the key's timestamp.
    pub at: String,
    /// Who asked. Free-form caller tag, e.g. `"mutation-pipeline"` or
    /// `"admin-api"`. Not authenticated — evidence of provenance, not identity.
    pub caller: String,
    /// Version 2 is always true. A false version-1 row is historical evidence
    /// of an attempted operation, never a recovery instruction.
    pub committed: bool,
    /// Purge only: the schema whose record was erased.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Purge only: SHA-256 hex over `{schema}\0{described key}`. See the
    /// module docs for why the key itself is not stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_fingerprint: Option<String>,
    /// `history:` rows removed (purge only).
    #[serde(default)]
    pub history_rows_deleted: u64,
    /// Delete-converge only: records in the batch that had durable residue to
    /// converge. Keys already converged, or never persisted, are dropped
    /// before the row is written and are not counted here.
    #[serde(default)]
    pub records_converged: u64,
    /// Purge only: how many of the hard-deleted records carried a file-blob
    /// reference (a `$lastdb_file` content pointer or `file_blob_ref` /
    /// `file_hash` metadata). A count, never the refs themselves — a blob_ref
    /// is a hash of the file's plaintext, so retaining it would be the same
    /// confirmation oracle the records-uuid rule closes (see module docs). A
    /// nonzero value tells compliance that sealed blob bytes may now be
    /// orphaned. The blob counter reclaims them when the dwell passes.
    #[serde(default)]
    pub file_pointer_records_purged: u64,
}

impl RecordDeleteLedgerEntry {
    /// All-zero row for `verb`, stamped now. Constructors below customize it;
    /// keeping the zeroing in one place is what lets a new counter field land
    /// as one struct field + one line here instead of an edit per verb.
    fn base(verb: &str, caller: &str) -> Self {
        Self {
            version: 2,
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            committed: true,
            schema: None,
            key_fingerprint: None,
            history_rows_deleted: 0,
            records_converged: 0,
            file_pointer_records_purged: 0,
        }
    }

    /// A hard-erasure row (`delete` or `purge`), before the delete batch runs.
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        Self {
            schema: Some(schema.to_string()),
            key_fingerprint: Some(key_fingerprint(schema, described_key)),
            ..Self::base(verb, caller)
        }
    }
}

/// SHA-256 hex over `{schema}\0{described key}`.
///
/// The separator matters: without it `("ab", "c")` and `("a", "bc")` would
/// fingerprint identically, and a purge of one record would read as evidence of
/// purging another.
pub fn key_fingerprint(schema: &str, described_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(schema.as_bytes());
    hasher.update([0u8]);
    hasher.update(described_key.as_bytes());
    format!("{:x}", hasher.finalize())
}

impl RecordStore {
    /// Read ledger rows in time order (oldest first).
    ///
    /// `limit` of 0 means unbounded. This is a prefix scan by design: the ledger
    /// is an append-only time series read by administrative audit, not a
    /// key-addressed record, and the prefix bounds it to ledger rows alone.
    pub async fn list_record_delete_ledger(
        &self,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &lastdb_model::kind_partition::anchored("dellog", ""),
        );
        let rows = self
            .raw()
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("delete ledger scan: {e}")))?;

        // `scan_prefix` ordering is backend-dependent, so sort on the key rather
        // than trusting it. The fixed-width timestamp makes byte order time
        // order.
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = rows;
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        let mut out = Vec::new();
        for (k, v) in rows {
            match serde_json::from_slice::<RecordDeleteLedgerEntry>(&v) {
                Ok(entry) => out.push(entry),
                Err(e) => {
                    // An unreadable audit row is worth knowing about, but must
                    // not blank the rest of the ledger.
                    tracing::warn!(
                        ledger_key = %String::from_utf8_lossy(&k),
                        error = %e,
                        "delete ledger: skipping unreadable row"
                    );
                }
            }
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}
