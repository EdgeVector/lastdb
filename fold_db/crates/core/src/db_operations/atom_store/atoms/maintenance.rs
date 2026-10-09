use super::*;

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
        // lint:fn-size-ok verbatim move from atoms.rs; splitting this function is separate work
        use crate::atom::molecule_key_codec;
        use crate::db_operations::admin_db::LegacyRefBlobPurgeReport;

        let prefix = build_storage_key(storage_prefix, "ref:");
        let rows = self
            .raw()
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan ref: {e}")))?;

        let mut keys_found = 0u64;
        let mut bytes_found_approx = 0u64;
        let mut keys_safe = 0u64;
        let mut keys_blocked = 0u64;
        let mut bytes_safe_approx = 0u64;
        let mut bytes_blocked_approx = 0u64;
        let mut safe_to_delete: Vec<String> = Vec::new();
        let mut blocked_samples: Vec<String> = Vec::new();
        const SAMPLE_CAP: usize = 16;

        for (k, v) in rows {
            let key = String::from_utf8_lossy(&k).into_owned();
            let row_bytes = k.len() as u64 + v.len() as u64;
            keys_found += 1;
            bytes_found_approx += row_bytes;

            let Some(mol_uuid) = Self::molecule_uuid_from_ref_key(&key) else {
                keys_blocked += 1;
                bytes_blocked_approx += row_bytes;
                if blocked_samples.len() < SAMPLE_CAP {
                    blocked_samples.push(key);
                }
                continue;
            };

            let header =
                build_storage_key(storage_prefix, &molecule_key_codec::header_key(mol_uuid));
            let mk_prefix = build_storage_key(
                storage_prefix,
                &molecule_key_codec::molecule_record_prefix(mol_uuid),
            );

            let has_header = self
                .raw()
                .exists_item(&header)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("exists mh: {e}")))?;
            let has_mk = if has_header {
                true
            } else {
                let mk_rows = self
                    .raw()
                    .inner()
                    .scan_prefix_keys(mk_prefix.as_bytes())
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("scan mk: {e}")))?;
                !mk_rows.is_empty()
            };

            if has_mk {
                keys_safe += 1;
                bytes_safe_approx += row_bytes;
                safe_to_delete.push(key);
            } else {
                keys_blocked += 1;
                bytes_blocked_approx += row_bytes;
                if blocked_samples.len() < SAMPLE_CAP {
                    blocked_samples.push(key);
                }
            }
        }

        let mut keys_deleted = 0u64;
        if !dry_run && !safe_to_delete.is_empty() {
            keys_deleted = safe_to_delete.len() as u64;
            self.raw()
                .batch_delete_keys(safe_to_delete)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("delete ref: {e}")))?;
            let _ = self.flush().await;
        }

        Ok(LegacyRefBlobPurgeReport {
            dry_run,
            collection: "legacy_blob_refs".to_string(),
            keys_found,
            bytes_found_approx,
            keys_safe,
            keys_blocked,
            bytes_safe_approx,
            bytes_blocked_approx,
            keys_deleted,
            blocked_samples,
            purge_complete: keys_blocked == 0 && (dry_run || keys_deleted == keys_safe),
        })
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
