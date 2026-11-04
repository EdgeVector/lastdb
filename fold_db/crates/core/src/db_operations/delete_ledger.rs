//! Audit facts about completed record erasures.
//!
//! A native erase stages one final audit row in the same snapshot batch as
//! its data removals. The row has no recovery role. It never drives replay,
//! repairs a counter, or authorizes another delete.
//!
//! The row retains counts, caller, schema, time, and a key fingerprint. It
//! retains no field value, record UUIDs, or plaintext subject key. Audit rows
//! have no automatic retention policy.

use chrono::Utc;
use sha2::{Digest, Sha256};

use super::RecordStore;
use crate::schema::SchemaError;

/// Which path performed the delete.
pub const LEDGER_VERB_PURGE: &str = "purge";
pub const LEDGER_VERB_DELETE: &str = "delete";

pub use crate::storage::format::records::RecordDeleteLedgerEntry;

impl RecordDeleteLedgerEntry {
    /// A hard-erasure row (`delete` or `purge`) with zero counts, stamped now,
    /// before the delete batch runs. The caller sets the counts.
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        Self {
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            schema: schema.to_string(),
            key_fingerprint: key_fingerprint(schema, described_key),
            history_rows_deleted: 0,
            records_erased: 0,
            file_records_erased: 0,
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
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix =
            lastdb_model::kind_partition::anchored(crate::storage::format::DELETE_AUDIT_FAMILY, "");
        let rows = self
            .reader()
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
            let entry = serde_json::from_slice::<RecordDeleteLedgerEntry>(&v).map_err(|e| {
                SchemaError::InvalidData(format!(
                    "delete ledger row {} is unreadable: {e}",
                    String::from_utf8_lossy(&k)
                ))
            })?;
            out.push(entry);
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}
