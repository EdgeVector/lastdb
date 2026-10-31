//! The explicit candidate list of the molecule drain.
//!
//! A molecule becomes a candidate in the batch that lowers its `bindings`
//! counter to zero (`molecule_bindings.rs`). The candidate row is written in
//! that same batch, so a crash cannot leave a molecule that lost its last
//! binding and has no candidate row. The drain deletes the row in the batch that
//! removes the header of the molecule, or when it finds the molecule bound
//! again.
//!
//! Key: `mref:v1:d\0{scope}\0{molecule}`. The hash is the family, so the whole
//! list is one range under one hash: a reader needs no scan to find the
//! candidates after a restart. The scope is part of the key, not a prefix of it,
//! because one list serves every scope of the node (`time_queue` does the same).
//! The personal scope is the empty string.
//!
//! Deleted rows stay as deleted keys in the hash. A reader walks the list with a
//! bounded range walk (`RangeWalk`), which passes deleted keys, so a long history
//! of drained molecules costs reads and never a failure.

use serde::{Deserialize, Serialize};

/// The family and hash of the candidate rows.
pub(crate) const CANDIDATE_FAMILY: &str = "mref:v1:d\0";

/// One molecule in one storage scope that lost its last binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Candidate {
    /// The storage scope prefix, `None` for the personal scope.
    pub scope: Option<String>,
    pub molecule: String,
}

/// The value of a candidate row. The key holds the identity; the value says
/// when the molecule became a candidate.
#[derive(Debug, Serialize, Deserialize)]
struct CandidateRow {
    #[serde(rename = "v")]
    version: u8,
    since_ms: u64,
}

impl Candidate {
    pub(crate) fn new(scope: Option<&str>, molecule: &str) -> Self {
        Self {
            scope: scope.map(str::to_string),
            molecule: molecule.to_string(),
        }
    }

    /// The key of the row.
    pub(crate) fn key(&self) -> String {
        format!(
            "{CANDIDATE_FAMILY}{}\0{}",
            self.scope.as_deref().unwrap_or(""),
            self.molecule
        )
    }

    /// The identity of a row from its key. `None` for a key of another shape.
    pub(crate) fn parse(key: &[u8]) -> Option<Self> {
        let rest = std::str::from_utf8(key)
            .ok()?
            .strip_prefix(CANDIDATE_FAMILY)?;
        let (scope, molecule) = rest.split_once('\0')?;
        if molecule.is_empty() || molecule.contains('\0') {
            return None;
        }
        Some(Self {
            scope: (!scope.is_empty()).then(|| scope.to_string()),
            molecule: molecule.to_string(),
        })
    }

    /// The key and the value of the row, for the batch that writes it.
    pub(crate) fn row(&self) -> (Vec<u8>, Vec<u8>) {
        (
            self.key().into_bytes(),
            serde_json::to_vec(&Self::row_value()).unwrap_or_default(),
        )
    }

    /// The key and the JSON value of the row, for a planner batch.
    pub(crate) fn row_json(&self) -> (String, serde_json::Value) {
        (self.key(), Self::row_value())
    }

    fn row_value() -> serde_json::Value {
        serde_json::to_value(CandidateRow {
            version: 1,
            since_ms: crate::clock::unix_millis(),
        })
        .unwrap_or_default()
    }
}

/// The half-open key range of the whole list.
pub(crate) fn list_bounds() -> (Vec<u8>, Vec<u8>) {
    let (start, end) = crate::kind_partition::exact_prefix_bounds(CANDIDATE_FAMILY);
    (start.into_bytes(), end.into_bytes())
}
