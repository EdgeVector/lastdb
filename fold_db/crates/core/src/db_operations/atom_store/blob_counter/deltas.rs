//! The net change of blob references in one planner run.
//!
//! A blob gains a reference when an atom counter that lists it is created, and
//! when a live tip record names it in its metadata. It loses one when such an
//! atom counter is deleted, or when such a tip record is replaced or removed.
//! The deltas are summed per blob before any counter is read, so a run that
//! removes and adds the same blob changes its counter once.

use crate::db_operations::atom_store::counter::AtomCounter;
use crate::db_operations::atom_store::PerKeyRecord;
use std::collections::BTreeMap;

/// Net change of references for each blob of one planner run.
#[derive(Debug, Default)]
pub(crate) struct BlobDeltas {
    by_blob: BTreeMap<String, i64>,
}

impl BlobDeltas {
    /// An atom counter that lists these blobs is written for the first time.
    pub(crate) fn atom_counter_created(&mut self, blobs: &[String]) {
        self.add(blobs, 1);
    }

    /// An atom counter that lists these blobs is deleted with its body.
    pub(crate) fn atom_counter_deleted(&mut self, blobs: &[String]) {
        self.add(blobs, -1);
    }

    /// A tip record is replaced, removed or created. Each side names its blobs
    /// in its metadata. A tombstoned record names none.
    pub(crate) fn tip_replaced(&mut self, old: Option<&PerKeyRecord>, new: Option<&PerKeyRecord>) {
        if let Some(old) = old {
            self.add(&tip_blobs(old), -1);
        }
        if let Some(new) = new {
            self.add(&tip_blobs(new), 1);
        }
    }

    /// Rows of one storage scope that a drop of the scope deletes. An atom
    /// counter row lowers the blobs it lists, a tip row lowers the blobs that its
    /// metadata names. Every other row holds no blob reference. A row that does
    /// not decode is an error: skipping it would leave a count too high forever.
    pub(crate) fn scope_rows_dropped(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), crate::schema::SchemaError> {
        let bad = |what: &str, error: serde_json::Error| {
            crate::schema::SchemaError::InvalidData(format!(
                "decode {what} of a dropped scope: {error}"
            ))
        };
        for (key, value) in rows {
            let Ok(key) = std::str::from_utf8(key) else {
                continue;
            };
            let Ok(scope) = crate::storage::current_keys::exact_scope(key) else {
                continue;
            };
            let bare = scope.map_or(key, |scope| key.get(scope.len() + 1..).unwrap_or(key));
            if crate::atom::atom_key_codec::is_counter_key(bare) {
                let counter: AtomCounter =
                    serde_json::from_slice(value).map_err(|e| bad("atom counter", e))?;
                self.atom_counter_deleted(&counter.blobs);
            } else if bare.starts_with("mk:") {
                let record: PerKeyRecord =
                    serde_json::from_slice(value).map_err(|e| bad("tip record", e))?;
                self.tip_replaced(Some(&record), None);
            }
        }
        Ok(())
    }

    fn add(&mut self, blobs: &[String], step: i64) {
        for blob in blobs {
            *self.by_blob.entry(blob.clone()).or_default() += step;
        }
    }

    /// The blobs with a net change, in blob order. Zero entries stay out.
    pub(super) fn into_changes(self) -> Vec<(String, i64)> {
        self.by_blob
            .into_iter()
            .filter(|(_, delta)| *delta != 0)
            .collect()
    }
}

/// The blob references that a live tip record names in its metadata.
fn tip_blobs(record: &PerKeyRecord) -> Vec<String> {
    let Some(meta) = record.meta.as_ref().filter(|meta| !meta.tombstoned) else {
        return Vec::new();
    };
    crate::atom::file_pointer::blob_refs_of_atom(&serde_json::Value::Null, meta.metadata.as_ref())
}
