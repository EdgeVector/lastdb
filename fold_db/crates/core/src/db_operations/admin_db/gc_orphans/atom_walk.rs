//! Delete-ledger and current reference helpers for orphan atom GC.

use super::*;

impl AtomStore {
    pub(super) async fn open_gc_atoms_ledger(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        scan_started_at: DateTime<Utc>,
    ) -> Result<Option<DeleteLedgerHandle>, SchemaError> {
        if dry_run {
            return Ok(None);
        }
        let entry = AtomDeleteLedgerEntry::gc_atoms("gc-atoms", &scan_started_at.to_rfc3339());
        self.begin_delete_ledger_row(storage_prefix, entry)
            .await
            .map(Some)
    }

    pub(super) fn gc_atoms_initial_report(
        dry_run: bool,
        source_schema_name: Option<&str>,
        scan_started_at: DateTime<Utc>,
    ) -> AtomGcReport {
        AtomGcReport {
            dry_run,
            schema: source_schema_name.map(str::to_string),
            scan_started_at: scan_started_at.to_rfc3339(),
            ..Default::default()
        }
    }

    pub(super) async fn collect_gc_atoms_roots(
        &self,
        storage_prefix: Option<&str>,
        dry_run: bool,
        extra_roots: &HashSet<String>,
    ) -> Result<HashSet<String>, SchemaError> {
        let mut referenced = self
            .collect_referenced_atom_uuids(storage_prefix, dry_run)
            .await?;
        referenced.extend(extra_roots.iter().cloned());
        Ok(referenced)
    }

    pub(super) async fn finish_gc_atoms_ledger(
        &self,
        handle: DeleteLedgerHandle,
        pending: &mut Vec<String>,
        mut storage_keys_deleted: u64,
        report: &AtomGcReport,
    ) -> Result<(), SchemaError> {
        if !pending.is_empty() {
            storage_keys_deleted += self.drain_gc_deletes(pending).await?;
        }
        if storage_keys_deleted > 0 {
            let _ = self.flush().await;
        }
        let final_report = report.clone();
        self.commit_delete_ledger_row(handle, move |entry| {
            entry.atoms_deleted = final_report.atoms_deleted;
            entry.storage_keys_deleted = storage_keys_deleted;
            entry.atoms_scanned = final_report.atoms_scanned;
            entry.atoms_skipped_recent = final_report.atoms_skipped_recent;
            entry.atoms_skipped_undatable = final_report.atoms_skipped_undatable;
        })
        .await;
        Ok(())
    }
}
