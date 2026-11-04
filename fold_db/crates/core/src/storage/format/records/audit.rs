//! Existing erase-audit wire definition. The domain supplies its facts.

use serde::{Deserialize, Serialize};

/// One audit row per erase batch. The row joins the native erase atomically,
/// so a stored row always names a committed erase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDeleteLedgerEntry {
    /// The retained delete or purge verb.
    pub verb: String,
    /// When the batch was planned (RFC3339); the key is stamped when staged.
    pub at: String,
    /// Who asked. Free-form caller tag, e.g. `"mutation-pipeline"` or
    /// `"admin-api"`. Not authenticated — evidence of provenance, not identity.
    pub caller: String,
    /// The schema whose records were erased.
    pub schema: String,
    /// SHA-256 hex over `{schema}\0{described key}`. See the module docs for
    /// why the key itself is not stored.
    pub key_fingerprint: String,
    /// `rh:` field history rows removed (purge only).
    pub history_rows_deleted: u64,
    /// Records the batch erased.
    pub records_erased: u64,
    /// Erased records that had a file. The same batch deleted each file. A
    /// count, never the file hashes: a hash of the plaintext would be the
    /// confirmation oracle that the records-uuid rule closes (see module docs).
    pub file_records_erased: u64,
}
