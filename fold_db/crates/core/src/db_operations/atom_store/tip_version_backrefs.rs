//! Durable atom → archived tip-version reverse references.
//!
//! `tv:` is authoritative and stays readable. `tvr:` is a derived access
//! pattern: current writers dual-write it, while a bounded keyset-paged job
//! rebuilds it from `mk:` heads and their existing `tv:` chains. Readers see
//! an explicit incomplete result until the durable completion marker exists.

use super::{AtomStore, MoleculeData, PerKeyRecord};
use crate::atom::{molecule_key_codec, AtomEntry};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

const MAX_TIP_CHAIN_WALK: usize = 1_000_000;
const DEFAULT_REINDEX_SLOT_PAGE: usize = 256;

/// One derived reverse row. The storage key carries atom + version identity;
/// the value carries the slot identity needed by purge/reference callers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TipVersionBackref {
    pub atom_uuid: String,
    pub version_id: String,
    pub molecule_uuid: String,
    pub hash: String,
    pub range: String,
}

/// Keyed lookup result. Callers must not treat a partial plane as absence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TipVersionBackrefLookup {
    Complete(Vec<TipVersionBackref>),
    Incomplete(Vec<TipVersionBackref>),
}

impl TipVersionBackrefLookup {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    #[must_use]
    pub fn references(&self) -> &[TipVersionBackref] {
        match self {
            Self::Complete(rows) | Self::Incomplete(rows) => rows,
        }
    }
}

/// Durable, operator-readable progress for the background rebuild.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TipVersionBackrefReindexStatus {
    pub version: u8,
    pub after_tip_key: Option<String>,
    pub slots_walked: u64,
    pub backrefs_written: u64,
    pub completed: bool,
}

impl Default for TipVersionBackrefReindexStatus {
    fn default() -> Self {
        Self {
            version: 1,
            after_tip_key: None,
            slots_walked: 0,
            backrefs_written: 0,
            completed: false,
        }
    }
}

/// Work performed by one bounded reindex pass.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TipVersionBackrefReindexReport {
    pub slots_walked: u64,
    pub backrefs_written: u64,
    pub completed: bool,
    pub status: TipVersionBackrefReindexStatus,
}

impl AtomStore {
    /// Build reverse rows for `pending_tip_versions` from the current slot
    /// heads. Pending nodes form chains rooted at the changed `mk:` records, so
    /// this recovers slot identity without changing the authoritative `tv:`
    /// value shape.
    pub(crate) fn pending_tip_version_backref_items(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let pending: HashMap<&str, &AtomEntry> = data
            .pending_tip_versions()
            .iter()
            .map(|(id, entry)| (id.as_str(), entry))
            .collect();
        if pending.is_empty() {
            return Ok(Vec::new());
        }

        let mut emitted = HashSet::with_capacity(pending.len());
        let mut out = Vec::with_capacity(pending.len());
        for (record_key, record) in records {
            let Some((hash, range)) =
                molecule_key_codec::decode_hash_range(record_key, molecule_uuid)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode molecule record while indexing tip versions for {molecule_uuid}"
                )));
            };
            let mut version_id = record.entry.prev_tip_id.as_str();
            let mut slot_seen = HashSet::new();
            while let Some(entry) = pending.get(version_id).copied() {
                if !slot_seen.insert(version_id.to_string()) {
                    return Err(SchemaError::InvalidData(format!(
                        "pending tip-version cycle for molecule {molecule_uuid}"
                    )));
                }
                if emitted.insert(version_id.to_string()) {
                    let row = TipVersionBackref {
                        atom_uuid: entry.atom_uuid.clone(),
                        version_id: version_id.to_string(),
                        molecule_uuid: molecule_uuid.to_string(),
                        hash: hash.clone(),
                        range: range.clone(),
                    };
                    out.push((
                        build_storage_key(
                            storage_prefix,
                            &molecule_key_codec::tip_version_backref_key(
                                &row.atom_uuid,
                                &row.version_id,
                            ),
                        ),
                        serde_json::to_value(row).map_err(|e| {
                            SchemaError::InvalidData(format!("serialize tip-version backref: {e}"))
                        })?,
                    ));
                }
                version_id = entry.prev_tip_id.as_str();
            }
        }

        if emitted.len() != pending.len() {
            return Err(SchemaError::InvalidData(format!(
                "{} pending tip version(s) for molecule {molecule_uuid} are not reachable from a persisted slot",
                pending.len() - emitted.len()
            )));
        }
        Ok(out)
    }

    /// Keyed atom lookup. An incomplete plane is explicit even when rows were
    /// already dual-written or backfilled; absence is authoritative only in the
    /// `Complete` variant.
    pub async fn tip_version_backrefs_for_atom(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<TipVersionBackrefLookup, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::tip_version_backref_prefix(atom_uuid),
        );
        // Derived plane: an undecodable legacy/ciphertext `tvr:` row must not
        // abort the whole lookup (and then soft-delete / mutation heal) with
        // HTTP 400 `Serialization error: expected value at line 1 column 1`.
        // Partition and surface skipped keys; dual-write + reindex heal them.
        let partitioned = self
            .main_store
            .scan_items_with_prefix_partition_undecodable::<TipVersionBackref>(&prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "scan tip-version backrefs for candidate atom: {e}"
                ))
            })?;
        if !partitioned.undecodable.is_empty() {
            tracing::warn!(
                atom_uuid,
                undecodable = partitioned.undecodable.len(),
                first_key = partitioned
                    .undecodable
                    .first()
                    .map_or("", |(k, _)| k.as_str()),
                first_error = partitioned
                    .undecodable
                    .first()
                    .map_or("", |(_, e)| e.as_str()),
                "skipping undecodable tip-version reverse-ref rows (derived plane; \
                 dual-write/reindex heals)"
            );
        }
        let rows: Vec<TipVersionBackref> =
            partitioned.items.into_iter().map(|(_, row)| row).collect();
        let marker = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_COMPLETE_KEY,
        );
        let complete = self.main_store.exists_item(&marker).await.map_err(|e| {
            SchemaError::InvalidData(format!("probe tip-version backref completeness: {e}"))
        })?;
        Ok(if complete {
            TipVersionBackrefLookup::Complete(rows)
        } else {
            TipVersionBackrefLookup::Incomplete(rows)
        })
    }

    /// Read durable reindex progress without scanning the data plane.
    pub async fn tip_version_backref_reindex_status(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<TipVersionBackrefReindexStatus, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_REINDEX_CHECKPOINT_KEY,
        );
        self.main_store
            .get_item(&key)
            .await
            .map(Option::unwrap_or_default)
            .map_err(|e| {
                SchemaError::InvalidData(format!("load tip-version backref reindex status: {e}"))
            })
    }

    /// Run one bounded, resumable page of the existing-data rebuild.
    ///
    /// The cursor is the last walked `mk:` key. New `tv:` writes are already
    /// dual-written, so advancing the old-data cursor cannot miss concurrent
    /// post-upgrade history nodes.
    pub async fn reindex_tip_version_backrefs(
        &self,
        storage_prefix: Option<&str>,
        slot_page: Option<usize>,
    ) -> Result<TipVersionBackrefReindexReport, SchemaError> {
        let page = slot_page.unwrap_or(DEFAULT_REINDEX_SLOT_PAGE).max(1);
        let checkpoint_key = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_REINDEX_CHECKPOINT_KEY,
        );
        let complete_key = build_storage_key(
            storage_prefix,
            molecule_key_codec::TIP_VERSION_BACKREF_COMPLETE_KEY,
        );
        let mut status = self
            .tip_version_backref_reindex_status(storage_prefix)
            .await?;
        if status.completed {
            return Ok(TipVersionBackrefReindexReport {
                slots_walked: 0,
                backrefs_written: 0,
                completed: true,
                status,
            });
        }

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

        let mut walked = 0u64;
        let mut writes = Vec::new();
        let mut last_key = status.after_tip_key.clone();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            if status.after_tip_key.as_deref() == Some(full_key.as_str()) {
                continue;
            }
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                continue;
            };
            let Some(rest) = base_key.strip_prefix("mk:") else {
                continue;
            };
            let Some((molecule_uuid, _)) = rest.split_once(':') else {
                continue;
            };
            let Some((hash, range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                continue;
            };
            // Ciphertext-era / empty / truncated `mk:` tips must not poison the
            // reverse-ref rebuild into a request-level 400 after the caller
            // already decided (purge heal) or after a mutation committed.
            // Skip the slot, advance the cursor, keep paging.
            let record: PerKeyRecord = match serde_json::from_slice(&value) {
                Ok(record) => record,
                Err(e) => {
                    tracing::warn!(
                        key = %full_key,
                        error = %e,
                        "skipping undecodable mk row during tip-version reverse-ref reindex"
                    );
                    walked += 1;
                    last_key = Some(full_key.clone());
                    continue;
                }
            };
            walked += 1;
            last_key = Some(full_key.clone());

            // Walk archived tip versions for this slot. Missing `tv:` ends the
            // chain without error — same contract as purge's `walk_tip_chain`:
            // legacy depth-1 heads may store an atom id as `prev_tip_id`, and
            // GC / dangling history may prune mid-chain. Reverse refs only
            // exist for rows that are still present; a hard-fail here blocked
            // soft-delete heal pages (HTTP 400 `missing tv:… while rebuilding
            // reverse references`) on otherwise healthy deletes.
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
                    hash: hash.clone(),
                    range: range.clone(),
                };
                writes.push((
                    build_storage_key(
                        storage_prefix,
                        &molecule_key_codec::tip_version_backref_key(
                            &row.atom_uuid,
                            &row.version_id,
                        ),
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
        }

        let written = writes.len() as u64;
        if !writes.is_empty() {
            self.main_store.batch_put_items(writes).await.map_err(|e| {
                SchemaError::InvalidData(format!("write tip-version backref page: {e}"))
            })?;
        }
        status.after_tip_key = last_key;
        status.slots_walked += walked;
        status.backrefs_written += written;
        status.completed = range_exhausted;
        self.main_store
            .put_item(&checkpoint_key, &status)
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
        Ok(TipVersionBackrefReindexReport {
            slots_walked: walked,
            backrefs_written: written,
            completed: status.completed,
            status,
        })
    }

    /// Derived rows corresponding to full `tv:` keys about to be deleted.
    pub(crate) async fn tip_version_backref_delete_keys_for_tv_keys(
        &self,
        tv_keys: &[String],
    ) -> Result<Vec<Vec<u8>>, SchemaError> {
        let mut out = Vec::with_capacity(tv_keys.len());
        for full_key in tv_keys {
            let Some((prefix, version_id)) = split_full_tip_version_key(full_key) else {
                continue;
            };
            let entry: Option<AtomEntry> =
                self.main_store.get_item(full_key).await.map_err(|e| {
                    SchemaError::InvalidData(format!("load tip version before backref delete: {e}"))
                })?;
            if let Some(entry) = entry {
                out.push(
                    build_storage_key(
                        prefix,
                        &molecule_key_codec::tip_version_backref_key(&entry.atom_uuid, version_id),
                    )
                    .into_bytes(),
                );
            }
        }
        Ok(out)
    }
}

fn strip_storage_prefix<'a>(storage_prefix: Option<&str>, full_key: &'a str) -> Option<&'a str> {
    match storage_prefix {
        Some(prefix) => full_key.strip_prefix(prefix)?.strip_prefix(':'),
        None => Some(full_key),
    }
}

fn split_full_tip_version_key(full_key: &str) -> Option<(Option<&str>, &str)> {
    let at = full_key
        .rfind("tv\0")
        .or_else(|| full_key.rfind(molecule_key_codec::TIP_VERSION_PREFIX))?;
    let version_id = &full_key[at + 3..];
    if version_id.is_empty() {
        return None;
    }
    let prefix = full_key[..at].strip_suffix(':').filter(|p| !p.is_empty());
    Some((prefix, version_id))
}
