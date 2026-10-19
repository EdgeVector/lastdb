//! Durable atom → archived tip-version reverse references.
//!
//! `tv:` is authoritative for existing physical history and stays readable.
//! Current writes do not create history. A bounded keyset-paged job rebuilds
//! derived `tvr:` references from `mk:` heads and existing chains. Readers see
//! an explicit incomplete result until the durable completion marker exists.

use super::{AtomStore, PerKeyRecord};
use crate::atom::{molecule_key_codec, AtomEntry};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

mod reindex;
use reindex::decode_mk_slot;

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
        // Partition and surface skipped keys; the explicit rebuild repairs them.
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
                 explicit rebuild repairs)"
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
    /// The cursor is the last walked `mk:` key. This administrative path reads
    /// existing physical history; current logical writes create no new nodes.
    pub async fn reindex_tip_version_backrefs(
        &self,
        storage_prefix: Option<&str>,
        slot_page: Option<usize>,
    ) -> Result<TipVersionBackrefReindexReport, SchemaError> {
        let page = slot_page.unwrap_or(DEFAULT_REINDEX_SLOT_PAGE).max(1);
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

        let (rows, range_exhausted) = self
            .scan_reindex_page(storage_prefix, &status, page)
            .await?;

        let mut walked = 0u64;
        let mut writes = Vec::new();
        let mut last_key = status.after_tip_key.clone();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            if status.after_tip_key.as_deref() == Some(full_key.as_str()) {
                continue;
            }
            let Some((molecule_uuid, hash, range)) = decode_mk_slot(storage_prefix, &full_key)
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

            self.collect_slot_backrefs(
                storage_prefix,
                record,
                molecule_uuid,
                &hash,
                &range,
                &mut writes,
            )
            .await?;
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
        self.persist_reindex_progress(storage_prefix, &status)
            .await?;
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
