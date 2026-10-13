//! Audited-candidate revalidation and post-delete flush for atom GC.

use super::{AtomGcCandidate, AtomGcReapOptions, AtomGcReapReport};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::laststore::CollectionCompactOptions;

impl AtomStore {
    /// Revalidate one queued candidate under the body and count locks, then
    /// delete its body when the audit allows it.
    pub(super) async fn reap_audited_atom_gc_candidate(
        &self,
        queued: &AtomGcCandidate,
        storage_prefix: Option<&str>,
        options: &AtomGcReapOptions,
        report: &mut AtomGcReapReport,
    ) -> Result<(), SchemaError> {
        let atom_uuid = queued.atom_uuid.clone();
        // Body writers take these locks in this order.
        let _body_guards = self
            .lock_automatic_gc_atoms(std::slice::from_ref(&atom_uuid))
            .await;
        let _count_guards = self
            .lock_atom_ref_counts(std::slice::from_ref(&atom_uuid))
            .await;
        let Some(candidate) = self
            .load_atom_gc_candidate(&atom_uuid, storage_prefix)
            .await?
        else {
            if !options.dry_run {
                self.delete_stale_atom_gc_queue_row(&queued.queue_key)
                    .await?;
                report.stale_queue_rows_deleted = report.stale_queue_rows_deleted.saturating_add(1);
            }
            return Ok(());
        };
        if candidate.zero_since_unix_nanos != queued.zero_since_unix_nanos
            || candidate.queue_key != queued.queue_key
        {
            if !options.dry_run {
                self.delete_stale_atom_gc_queue_row(&queued.queue_key)
                    .await?;
                report.stale_queue_rows_deleted = report.stale_queue_rows_deleted.saturating_add(1);
            }
            return Ok(());
        }
        if self
            .cancel_unless_exact_zero_count(&candidate, storage_prefix, options, report)
            .await?
        {
            return Ok(());
        }
        // A pre-count atom can have a false zero. Verify the authoritative
        // reverse-edge partition while both writer locks are held.
        if self
            .has_active_atom_refs(&atom_uuid, storage_prefix)
            .await?
        {
            report.cancelled_positive_count = report.cancelled_positive_count.saturating_add(1);
            if !options.dry_run {
                self.delete_atom_gc_candidate(&candidate, storage_prefix)
                    .await?;
            }
            return Ok(());
        }
        if self
            .has_any_pending_atom_refs(&atom_uuid, storage_prefix)
            .await?
        {
            report.retained_pending_reference = report.retained_pending_reference.saturating_add(1);
            return Ok(());
        }
        match candidate.audit.as_ref() {
            None => {
                report.retained_without_audit = report.retained_without_audit.saturating_add(1);
                return Ok(());
            }
            Some(audit) if audit.reachable => {
                report.retained_audit_reachable = report.retained_audit_reachable.saturating_add(1);
                return Ok(());
            }
            Some(_) if !candidate.audit_clears_delete() => {
                report.retained_without_audit = report.retained_without_audit.saturating_add(1);
                return Ok(());
            }
            Some(_) => {}
        }
        report.eligible = report.eligible.saturating_add(1);
        if options.dry_run {
            return Ok(());
        }
        let deleted = self
            .delete_atom_gc_candidate_body(&candidate, storage_prefix)
            .await?;
        report.atoms_deleted = report.atoms_deleted.saturating_add(u64::from(deleted.atom));
        report.storage_keys_deleted = report
            .storage_keys_deleted
            .saturating_add(deleted.storage_keys);
        report.bytes_deleted_approx = report
            .bytes_deleted_approx
            .saturating_add(deleted.bytes_approx);
        Ok(())
    }

    /// Remove the candidate and report true when the count record is absent,
    /// inexact or not zero. A candidate always travels with an explicit count
    /// record. Without one, a zero proves nothing, so the body leaks.
    async fn cancel_unless_exact_zero_count(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
        options: &AtomGcReapOptions,
        report: &mut AtomGcReapReport,
    ) -> Result<bool, SchemaError> {
        let count = self
            .atom_live_ref_count_rows(std::slice::from_ref(&candidate.atom_uuid), storage_prefix)
            .await?
            .into_iter()
            .next()
            .flatten();
        match count {
            None => {
                report.cancelled_missing_count = report.cancelled_missing_count.saturating_add(1);
            }
            Some(count) if !count.exact || count.live_refs != 0 => {
                report.cancelled_positive_count = report.cancelled_positive_count.saturating_add(1);
            }
            Some(_) => return Ok(false),
        }
        if !options.dry_run {
            self.delete_atom_gc_candidate(candidate, storage_prefix)
                .await?;
        }
        Ok(true)
    }

    /// Flush the deletes and optionally compact the atom plane.
    pub(super) async fn flush_and_compact_after_atom_gc(
        &self,
        options: &AtomGcReapOptions,
        report: &mut AtomGcReapReport,
    ) -> Result<(), SchemaError> {
        if !options.dry_run && report.atoms_deleted > 0 {
            self.main_store.inner().flush().await.map_err(|error| {
                SchemaError::InvalidData(format!("flush atom GC deletes: {error}"))
            })?;
            if options.compact_after_delete {
                let namespaced = self.namespaced_store.as_ref().ok_or_else(|| {
                    SchemaError::InvalidData(
                        "atom GC physical delete requires a namespaced store".to_string(),
                    )
                })?;
                report.compaction = Some(
                    namespaced
                        .compact_collection(CollectionCompactOptions {
                            collection: "atoms".to_string(),
                            dry_run: false,
                            seed_committed_history: false,
                        })
                        .await
                        .map_err(|error| {
                            SchemaError::InvalidData(format!(
                                "compact atom segments after audited GC: {error}"
                            ))
                        })?,
                );
            }
        }
        Ok(())
    }
}
