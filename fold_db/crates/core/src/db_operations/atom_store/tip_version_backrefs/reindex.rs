//! Reindex helpers for the tip-version reverse-reference job.

use super::{
    strip_storage_prefix, TipVersionBackref, TipVersionBackrefReindexStatus, MAX_TIP_CHAIN_WALK,
};
use crate::atom::molecule_key_codec;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use std::collections::HashSet;

impl AtomStore {
    /// Scan one page of `mk:` rows after the cursor. The bool is true when the
    /// `mk:` range is exhausted.
    pub(super) async fn scan_reindex_page(
        &self,
        storage_prefix: Option<&str>,
        status: &TipVersionBackrefReindexStatus,
        page: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool), SchemaError> {
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let start = status
            .after_tip_key
            .clone()
            .unwrap_or_else(|| mk_prefix.clone());
        let raw_limit = page + usize::from(status.after_tip_key.is_some());
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), mk_end.as_bytes(), raw_limit)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("reindex scan mk: {e}")))?;
        let range_exhausted = rows.len() < raw_limit;
        Ok((rows, range_exhausted))
    }

    /// Walk archived tip versions for one slot and queue a reverse ref for each.
    ///
    /// Missing `tv:` ends the
    /// chain without error — same contract as purge's `walk_tip_chain`:
    /// legacy depth-1 heads may store an atom id as `prev_tip_id`, and
    /// GC / dangling history may prune mid-chain. Reverse refs only
    /// exist for rows that are still present; a hard-fail here blocked
    /// soft-delete heal pages (HTTP 400 `missing tv:… while rebuilding
    /// reverse references`) on otherwise healthy deletes.
    pub(super) async fn collect_slot_backrefs(
        &self,
        storage_prefix: Option<&str>,
        record: PerKeyRecord,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        writes: &mut Vec<(String, TipVersionBackref)>,
    ) -> Result<(), SchemaError> {
        let mut version_id = record.entry.prev_tip_id;
        let mut seen = HashSet::new();
        let mut chain_open = true;
        for _ in 0..MAX_TIP_CHAIN_WALK {
            if version_id.is_empty() {
                chain_open = false;
                break;
            }
            if !seen.insert(version_id.clone()) {
                return Err(SchemaError::InvalidData(format!(
                    "tip-version cycle while reindexing molecule {molecule_uuid}"
                )));
            }
            let Some(entry) = self.get_tip_version(&version_id, storage_prefix).await? else {
                // Broken / legacy tail — stop this slot; continue the page.
                chain_open = false;
                break;
            };
            let row = TipVersionBackref {
                atom_uuid: entry.atom_uuid.clone(),
                version_id: version_id.clone(),
                molecule_uuid: molecule_uuid.to_string(),
                hash: hash.to_string(),
                range: range.to_string(),
            };
            writes.push((
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::tip_version_backref_key(&row.atom_uuid, &row.version_id),
                ),
                row,
            ));
            version_id = entry.prev_tip_id;
        }
        // Only refuse when we hit the walk cap with a still-open chain.
        // A non-empty version_id after a missing-tv break is intentional.
        if chain_open && !version_id.is_empty() {
            return Err(SchemaError::InvalidData(format!(
                "tip-version chain exceeded {MAX_TIP_CHAIN_WALK} nodes during reindex"
            )));
        }
        Ok(())
    }

    /// Persist the reindex cursor and, when finished, the completeness marker.
    pub(super) async fn persist_reindex_progress(
        &self,
        storage_prefix: Option<&str>,
        status: &TipVersionBackrefReindexStatus,
    ) -> Result<(), SchemaError> {
        let checkpoint_key = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_REINDEX_CHECKPOINT_KEY,
        );
        let complete_key = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_COMPLETE_KEY,
        );
        self.main_store
            .put_item(&checkpoint_key, status)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("persist tip-version reindex cursor: {e}"))
            })?;
        if status.completed {
            self.main_store
                .put_item(&complete_key, &true)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "persist tip-version backref completeness: {e}"
                    ))
                })?;
        }
        Ok(())
    }
}

/// Decode a full `mk:` storage key into `(molecule_uuid, hash, range)`.
pub(super) fn decode_mk_slot<'a>(
    storage_prefix: Option<&str>,
    full_key: &'a str,
) -> Option<(&'a str, String, String)> {
    let base_key = strip_storage_prefix(storage_prefix, full_key)?;
    let rest = base_key.strip_prefix("mk:")?;
    let (molecule_uuid, _) = rest.split_once(':')?;
    let (hash, range) = molecule_key_codec::decode_hash_range(base_key, molecule_uuid)?;
    Some((molecule_uuid, hash, range))
}
