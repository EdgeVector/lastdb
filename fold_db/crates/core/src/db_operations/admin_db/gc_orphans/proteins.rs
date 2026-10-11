//! Orphan protein GC.

use super::*;

impl AtomStore {
    /// Delete empty `protein:` rows that have no `molprot:` back-ref.
    ///
    /// A live protein is reachable through its member list and through at least
    /// one molecule back-ref. Historical client probe leaks created empty
    /// proteins, failed to bind them, and left no back-ref. Those rows carry no
    /// member state and can be reclaimed without touching fold state or tips.
    pub async fn gc_orphan_proteins(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<ProteinGcReport, SchemaError> {
        let backref_prefix = build_storage_key(storage_prefix, MEMBER_BACKREF_PREFIX);
        let backref_rows = self
            .raw()
            .inner()
            .scan_prefix(backref_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc scan molprot: {e}")))?;
        let mut referenced = HashSet::new();
        let mut report = ProteinGcReport {
            dry_run,
            molprot_backrefs_scanned: backref_rows.len() as u64,
            ..ProteinGcReport::default()
        };
        for (_, value) in backref_rows {
            if let Ok(protein_uuid) = serde_json::from_slice::<String>(&value) {
                referenced.insert(protein_uuid);
            }
        }

        let protein_prefix = build_storage_key(storage_prefix, PROTEIN_RECORD_PREFIX);
        let protein_rows = self
            .raw()
            .inner()
            .scan_prefix(protein_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc scan protein: {e}")))?;

        let mut to_delete = Vec::new();
        for (key, value) in protein_rows {
            report.proteins_scanned += 1;
            let protein: Protein = serde_json::from_slice(&value).map_err(|e| {
                SchemaError::InvalidData(format!(
                    "gc decode protein {}: {e}",
                    String::from_utf8_lossy(&key)
                ))
            })?;
            if !protein.members.is_empty() {
                report.proteins_with_members += 1;
                continue;
            }
            if referenced.contains(&protein.uuid) {
                report.proteins_referenced_by_backref += 1;
                continue;
            }
            report.orphan_proteins += 1;
            report.bytes_freed_approx += key.len() as u64 + value.len() as u64;
            to_delete.push(String::from_utf8_lossy(&key).into_owned());
        }

        if !dry_run && !to_delete.is_empty() {
            const CHUNK: usize = 2000;
            for chunk in to_delete.chunks(CHUNK) {
                self.raw()
                    .batch_delete_keys(chunk.to_vec())
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("gc delete proteins: {e}")))?;
            }
            report.proteins_deleted = report.orphan_proteins;
            let _ = self.flush().await;
        }

        Ok(report)
    }
}
