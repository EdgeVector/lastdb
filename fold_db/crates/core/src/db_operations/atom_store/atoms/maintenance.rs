use super::*;

const REF_BLOB_BLOCKED_SAMPLE_CAP: usize = 16;

/// Running totals for [`AtomStore::purge_legacy_ref_blobs`].
#[derive(Default)]
struct RefBlobTally {
    keys_found: u64,
    bytes_found_approx: u64,
    keys_safe: u64,
    keys_blocked: u64,
    bytes_safe_approx: u64,
    bytes_blocked_approx: u64,
    safe_to_delete: Vec<String>,
    blocked_samples: Vec<String>,
}

impl RefBlobTally {
    fn record_blocked(&mut self, key: String, row_bytes: u64) {
        self.keys_blocked += 1;
        self.bytes_blocked_approx += row_bytes;
        if self.blocked_samples.len() < REF_BLOB_BLOCKED_SAMPLE_CAP {
            self.blocked_samples.push(key);
        }
    }
}

impl AtomStore {
    /// Measure (and optionally delete) legacy `ref:{molecule_uuid}` whole-molecule
    /// blobs — the pre-per-key layout stored in the cold `legacy_blob_refs`
    /// collection. The live read path never dual-reads these
    /// (see `field/base.rs::planted_ref_blob_is_ignored_by_refresh`); product
    /// mutation refuses new writes. Dry-run by default, matching
    /// [`Self::gc_orphan_proteins`]'s shape.
    ///
    /// **Safety gate (CoW purge / delete-train):** a `ref:{M}` key is only
    /// deleted when the molecule is already rehydrated as per-key data —
    /// i.e. `mh:{M}` or any `mk:{M}:…` tip exists. Keys without per-key
    /// coverage are reported as blocked and left in place (do not force-delete
    /// sole-copy residue).
    pub async fn purge_legacy_ref_blobs(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<crate::db_operations::admin_db::LegacyRefBlobPurgeReport, SchemaError> {
        use crate::db_operations::admin_db::LegacyRefBlobPurgeReport;

        let prefix = build_storage_key(storage_prefix, "ref:");
        let rows = self
            .raw()
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan ref: {e}")))?;

        let mut tally = RefBlobTally::default();
        for (k, v) in rows {
            let key = String::from_utf8_lossy(&k).into_owned();
            let row_bytes = k.len() as u64 + v.len() as u64;
            tally.keys_found += 1;
            tally.bytes_found_approx += row_bytes;

            let covered = match Self::molecule_uuid_from_ref_key(&key) {
                Some(mol_uuid) => {
                    self.molecule_has_per_key_coverage(mol_uuid, storage_prefix)
                        .await?
                }
                None => false,
            };
            if covered {
                tally.keys_safe += 1;
                tally.bytes_safe_approx += row_bytes;
                tally.safe_to_delete.push(key);
            } else {
                tally.record_blocked(key, row_bytes);
            }
        }

        let mut keys_deleted = 0u64;
        if !dry_run && !tally.safe_to_delete.is_empty() {
            keys_deleted = tally.safe_to_delete.len() as u64;
            self.raw()
                .batch_delete_keys(tally.safe_to_delete)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("delete ref: {e}")))?;
            let _ = self.flush().await;
        }

        Ok(LegacyRefBlobPurgeReport {
            dry_run,
            collection: "legacy_blob_refs".to_string(),
            keys_found: tally.keys_found,
            bytes_found_approx: tally.bytes_found_approx,
            keys_safe: tally.keys_safe,
            keys_blocked: tally.keys_blocked,
            bytes_safe_approx: tally.bytes_safe_approx,
            bytes_blocked_approx: tally.bytes_blocked_approx,
            keys_deleted,
            blocked_samples: tally.blocked_samples,
            purge_complete: tally.keys_blocked == 0 && (dry_run || keys_deleted == tally.keys_safe),
        })
    }

    /// True when the molecule is already stored as per-key data: `mh:{M}` or
    /// any `mk:{M}:…` tip exists.
    async fn molecule_has_per_key_coverage(
        &self,
        mol_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        use crate::atom::molecule_key_codec;

        let header = build_storage_key(storage_prefix, &molecule_key_codec::header_key(mol_uuid));
        let has_header = self
            .raw()
            .exists_item(&header)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("exists mh: {e}")))?;
        if has_header {
            return Ok(true);
        }
        let mk_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(mol_uuid),
        );
        let mk_rows = self
            .raw()
            .inner()
            .scan_prefix_keys(mk_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan mk: {e}")))?;
        Ok(!mk_rows.is_empty())
    }

    /// Extract molecule uuid from a storage key of the form `ref:{M}` or
    /// `{org}:ref:{M}` (and the same with an outer storage_prefix already
    /// applied via `build_storage_key`).
    pub(super) fn molecule_uuid_from_ref_key(key: &str) -> Option<&str> {
        let (_, mol) = key.rsplit_once("ref:")?;
        let mol = mol.trim();
        if mol.is_empty() || mol.contains(':') {
            // Unexpected extra segments — treat as unclassifiable residue.
            return None;
        }
        Some(mol)
    }
}
