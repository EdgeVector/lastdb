//! The row commit, the reference verifier and the offline recount.
//!
//! - [`RecordStore::commit_rows`](super::RecordStore::commit_rows): one native batch
//!   of rows, deletes, guarded deletes and conflict row releases. The retention
//!   janitor, the queue repairs and the field history expiry write through it.
//! - `verify`: `lastdb db verify-refs`. It checks the retention queue, the blob
//!   counters and the blob dwell queue, and writes nothing.
//! - `recount`: `lastdb db recount`, on a stopped home. It counts the file blob
//!   references of the records and the field history, repairs the blob counters
//!   and the blob queue, and gives the retention queue buckets their rows back.
//!
//! The atom counters, the tip records and the atom bodies of format 6 are gone.

mod commit;
mod guarded;
mod recount;
mod report;
pub(crate) mod restore_validation;
mod verify;

pub(crate) use commit::{GuardedDelete, ReleaseRequest, RowChanges};
pub use recount::{RecountOptions, RecountReport};
pub use report::AtomCounterReport;
pub(crate) use verify::VerifyState;
pub use verify::{VerifyReport, VerifyRequest, VerifySummary};

impl super::RecordStore {
    /// Check the scope that a verb names. The only scopes a verb may name are the
    /// personal scope (no value) and a database scope: exactly 64 lowercase hex
    /// characters. A mistyped scope reads an empty range and looks like a clean
    /// scope, so the verbs that take a scope check it before they read. The check is
    /// narrower than the key validation of the storage layer on purpose: a verb
    /// never needs a `cloud:` or `from:` scope or a reserved name.
    pub fn validate_scope(scope: Option<&str>) -> Result<(), crate::schema::SchemaError> {
        let Some(scope) = scope else {
            return Ok(());
        };
        let lower_hex = scope.len() == 64
            && scope
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
        if lower_hex {
            return Ok(());
        }
        let shown: String = scope.chars().take(80).collect();
        Err(crate::schema::SchemaError::InvalidData(format!(
            "invalid scope {shown:?}: a scope is the personal scope (leave the value out) \
             or a database scope of exactly 64 lowercase hex characters"
        )))
    }
}
