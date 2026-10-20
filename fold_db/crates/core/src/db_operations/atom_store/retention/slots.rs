//! The purge side: the records of the slots that an erase removes.

use super::entry::RetentionEntry;
use super::keys;
use crate::db_operations::atom_store::counter::ReleaseRequest;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// The most records one erase request releases. A record costs a few
/// operations and a few reads, so the cap keeps the release small beside the
/// tips of the request. A key that holds more keeps the rest until the window
/// ends, and the report counts them.
pub(crate) const RELEASE_RECORD_CAP: usize = 100;

/// One removed slot, in storage form.
pub(crate) type RemovedSlots<'a> = (&'a str, &'a [(String, String)]);

impl AtomStore {
    /// The releases for the records of the slots that one erase removes. The
    /// caller holds the write gate, so the reads see every accepted batch.
    ///
    /// A molecule with no directory entry costs one read that finds nothing.
    /// For a molecule with entries, each removed slot costs one range read
    /// under the hash of the molecule.
    pub(crate) async fn slot_release_requests(
        &self,
        scope: Option<&str>,
        removals: &[RemovedSlots<'_>],
    ) -> Result<Vec<ReleaseRequest>, SchemaError> {
        let mut by_molecule: BTreeMap<&str, Vec<&(String, String)>> = BTreeMap::new();
        for (molecule, slots) in removals {
            by_molecule
                .entry(molecule)
                .or_default()
                .extend(slots.iter());
        }
        let mut entries: Vec<RetentionEntry> = Vec::new();
        let mut leftover = 0usize;
        for (molecule, slots) in by_molecule {
            if !self.molecule_has_directory(scope, molecule).await? {
                continue;
            }
            for (hash, range) in slots {
                let room = RELEASE_RECORD_CAP.saturating_sub(entries.len());
                let (found, more) = self
                    .slot_directory_entries(scope, molecule, hash, range, room)
                    .await?;
                entries.extend(found);
                leftover += more;
            }
        }
        if leftover > 0 {
            tracing::warn!(
                leftover,
                cap = RELEASE_RECORD_CAP,
                "an erase left kept conflict records to the retention window"
            );
            self.retention_state().note_leftover(leftover as u64);
        }
        Ok(entries.iter().map(RetentionEntry::request).collect())
    }

    /// Whether the molecule has any directory entry. One range read, limit one.
    async fn molecule_has_directory(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<bool, SchemaError> {
        let (start, end) = keys::prefix_range(&keys::molecule_dir_prefix(scope, molecule));
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("probe slot directory: {error}")))?;
        Ok(!rows.is_empty())
    }

    /// The entries of one slot, at most `room`. The second number counts the
    /// entries that did not fit.
    async fn slot_directory_entries(
        &self,
        scope: Option<&str>,
        molecule: &str,
        hash: &str,
        range: &str,
        room: usize,
    ) -> Result<(Vec<RetentionEntry>, usize), SchemaError> {
        let slot = keys::slot_key(scope, molecule, hash, range);
        let (start, end) = keys::prefix_range(&keys::slot_dir_prefix(scope, molecule, hash, range));
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), room + 1)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read slot directory: {error}")))?;
        let mut found = Vec::new();
        let mut more = 0usize;
        for (key, value) in rows {
            let Ok(entry) = serde_json::from_slice::<RetentionEntry>(&value) else {
                // A directory entry that does not decode names nothing to
                // release. Its record stays until the window ends.
                tracing::error!(
                    key = %String::from_utf8_lossy(&key),
                    "slot directory entry does not decode"
                );
                self.retention_state().note_undecodable();
                continue;
            };
            // A range that begins with the same bytes can belong to another
            // slot (a range that holds a NUL). Release only an exact match.
            if entry.slot != slot {
                continue;
            }
            if found.len() == room {
                more += 1;
            } else {
                found.push(entry);
            }
        }
        Ok((found, more))
    }
}
