//! Conflict records of a format 7 home.
//!
//! A peer import merges each field by stamp (decision D1). A losing value goes
//! to a conflict record when the two stamps come from two devices: a newer
//! peer value replaces a stored one, or a stored newer value keeps an older
//! peer value out. The rows are:
//!
//! - Row `{scope:}rcf:{S}:{digest}`, where `digest` is the SHA-256 in hex of
//!   `{record key}\0{field}\0{losing mutation id}`. The value holds the losing
//!   value inline, both stamps and the expiry. A range can hold a NUL, so the
//!   row key carries a digest and the value names the record.
//! - Marker `{scope:}rcm:{S}:{suffix}` beside the record
//!   `{scope:}rk:{S}:{suffix}`: the conflicted fields of the record, with the
//!   losing mutation and the expiry of each entry. A query reads the markers
//!   of its rows by key to set `has_conflicts`, and a selected read goes from
//!   a marker to its rows by key.
//! - A queue entry and the directory row of its bucket in the retention queue
//!   (`retention`), as for the conflict records of the molecule path. At the
//!   end of the window the janitor deletes the row, and the marker when no
//!   entry of it is left. A row holds no atom or file blob reference.

mod purge;
mod read;
mod release;
mod stage;

pub use read::{RecordConflictEntry, RecordFieldConflicts};

use crate::schema::types::KeyValue;
use lastdb_model::record_value::FieldStamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const ROW_FAMILY: &str = "rcf:";
const MARKER_FAMILY: &str = "rcm:";
const RECORD_FAMILY: &str = "rk:";
const ROW_VERSION: u8 = 1;

/// One losing value that a peer import found.
pub(crate) struct LostWrite {
    /// The full storage key of the record.
    pub record: String,
    pub schema: String,
    pub key: KeyValue,
    pub field: String,
    pub value: Value,
    pub loser: FieldStamp,
    pub winner: FieldStamp,
}

/// The stored value of one conflict row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordConflictRow {
    pub v: u8,
    pub schema: String,
    pub key: KeyValue,
    pub field: String,
    /// The full storage key of the record.
    pub record: String,
    pub value: Value,
    pub loser: FieldStamp,
    pub winner: FieldStamp,
    /// The retention clock when the import found the conflict, in nanoseconds.
    pub detected_at: u64,
    pub expires_at: u64,
}

/// The stored value of one marker: field, then losing mutation, then expiry.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConflictMarker {
    pub v: u8,
    pub fields: BTreeMap<String, BTreeMap<String, u64>>,
}

impl ConflictMarker {
    /// Drop each entry that expired at `now`, and each field without one.
    fn prune(&mut self, now: u64) {
        for entries in self.fields.values_mut() {
            entries.retain(|_, expires| *expires > now);
        }
        self.fields.retain(|_, entries| !entries.is_empty());
    }
}

/// The scope prefix and the part after `rk:` of a record key.
fn split_record_key(record: &str) -> Option<(&str, &str)> {
    let scope = crate::storage::current_keys::exact_scope(record).ok()?;
    let start = scope.map_or(0, |scope| scope.len() + 1);
    let rest = record.get(start..)?.strip_prefix(RECORD_FAMILY)?;
    Some((&record[..start], rest))
}

/// The marker key of the record at `record`.
pub(crate) fn marker_key(record: &str) -> Option<String> {
    let (scope, rest) = split_record_key(record)?;
    Some(format!("{scope}{MARKER_FAMILY}{rest}"))
}

/// The row key of one losing write of one field of the record at `record`.
pub(crate) fn record_conflict_row_key(record: &str, field: &str, mutation: &str) -> Option<String> {
    let (scope, rest) = split_record_key(record)?;
    let (space, _) = rest.split_once(':')?;
    let mut digest = Sha256::new();
    for part in [record, field, mutation] {
        digest.update(part.as_bytes());
        digest.update([0]);
    }
    let digest = digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write;
            let _ = write!(out, "{byte:02x}");
            out
        });
    Some(format!("{scope}{ROW_FAMILY}{space}:{digest}"))
}

/// True for the key of a conflict row of a format 7 home.
pub(crate) fn is_record_conflict_row_key(key: &str) -> bool {
    crate::storage::current_keys::exact_scope(key)
        .ok()
        .and_then(|scope| key.get(scope.map_or(0, |scope| scope.len() + 1)..))
        .is_some_and(|bare| bare.starts_with(ROW_FAMILY))
}
