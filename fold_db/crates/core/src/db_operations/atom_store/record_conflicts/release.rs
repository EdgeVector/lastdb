//! The janitor release of the conflict rows of a format 7 home. A row holds
//! no reference, so a release only deletes: the row, its queue rows, and the
//! marker of the record when no live entry of it is left.

use super::{marker_key, ConflictMarker, RecordConflictRow};
use crate::db_operations::atom_store::counter::ReleaseRequest;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::{BTreeMap, BTreeSet};

/// What the release of the conflict rows of one batch deletes.
#[derive(Default)]
pub(crate) struct RecordReleases {
    pub(crate) deletes: Vec<Vec<u8>>,
    /// Rows released.
    pub(crate) released: u64,
    /// Releases whose row was already gone. Their queue rows still go.
    pub(crate) gone: u64,
}

impl AtomStore {
    /// Read the rows that `releases` name, then their markers, in the write
    /// gate. A row that does not decode stays with its queue entry.
    pub(crate) async fn release_record_conflicts(
        &self,
        releases: &[ReleaseRequest],
    ) -> Result<RecordReleases, SchemaError> {
        let mut out = RecordReleases::default();
        if releases.is_empty() {
            return Ok(out);
        }
        let keys: Vec<&String> = releases.iter().map(|release| &release.history).collect();
        let rows = self.read_conflict_keys(&keys).await?;
        let mut by_marker: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
        for (release, bytes) in releases.iter().zip(rows) {
            let Some(bytes) = bytes else {
                out.gone += 1;
                out.deletes.extend(release.deletes.iter().cloned());
                continue;
            };
            let row = match serde_json::from_slice::<RecordConflictRow>(&bytes) {
                Ok(row) => row,
                Err(error) => {
                    tracing::error!(row = %release.history, %error, "record conflict row does not decode; the row stays");
                    self.retention_state().note_undecodable();
                    continue;
                }
            };
            out.released += 1;
            out.deletes.push(release.history.as_bytes().to_vec());
            out.deletes.extend(release.deletes.iter().cloned());
            if let Some(marker) = marker_key(&row.record) {
                by_marker
                    .entry(marker)
                    .or_default()
                    .insert((row.field, row.loser.mutation_id));
            }
        }
        let markers: Vec<&String> = by_marker.keys().collect();
        let stored = self.read_conflict_keys(&markers).await?;
        let now = self.retention_state().clock_nanos();
        for ((key, released), bytes) in by_marker.iter().zip(stored) {
            let Some(marker) =
                bytes.and_then(|bytes| serde_json::from_slice::<ConflictMarker>(&bytes).ok())
            else {
                continue;
            };
            let live = marker.fields.iter().any(|(field, entries)| {
                entries.iter().any(|(mutation, expires)| {
                    *expires > now && !released.contains(&(field.clone(), mutation.clone()))
                })
            });
            if !live {
                out.deletes.push(key.as_bytes().to_vec());
            }
        }
        Ok(out)
    }
}
