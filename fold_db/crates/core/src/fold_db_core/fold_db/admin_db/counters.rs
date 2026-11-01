//! The verifier and the blob counter report.

use super::*;

impl FoldDB {
    /// The verifier summary for `lastdb status`. A read of in-memory state; no
    /// store access.
    #[must_use]
    pub fn atom_counter_report(&self) -> crate::db_operations::AtomCounterReport {
        crate::db_operations::AtomCounterReport {
            verify_refs: Some(self.db_ops().records().verify_refs_summary()),
        }
    }

    /// Run one step of the counter verifier (`lastdb db verify-refs`). It
    /// reads and reports; it writes nothing and takes no write gate.
    pub async fn verify_refs_step(
        &self,
        request: crate::db_operations::VerifyRequest,
    ) -> Result<crate::db_operations::VerifyReport, FoldDbError> {
        self.db_ops()
            .records()
            .verify_refs_step(request)
            .await
            .map_err(|error| admin_op_error("verify_refs", error))
    }

    /// The last complete verify run and the run in progress, from memory.
    #[must_use]
    pub fn verify_refs_summary(&self) -> crate::db_operations::VerifySummary {
        self.db_ops().records().verify_refs_summary()
    }

    /// The storage scope prefixes that the database catalog names, besides the
    /// personal scope. The catalog is small, so listing it is not a scan of the
    /// data. A scope that holds data and has no catalog entry is not listed.
    pub async fn atom_counter_scopes(&self) -> Result<Vec<String>, FoldDbError> {
        use crate::db_operations::db_catalog_store::{
            referenced_instance_ids, UNPREFIXED_INSTANCE_ID,
        };
        let entries = self
            .db_ops()
            .db_catalog()
            .list_entries()
            .await
            .map_err(|error| {
                FoldDbError::Database(format!("list the database catalog: {error}"))
            })?;
        let mut scopes: Vec<String> = referenced_instance_ids(&entries)
            .into_iter()
            .filter(|id| !id.is_empty() && id != UNPREFIXED_INSTANCE_ID)
            .collect();
        scopes.sort();
        Ok(scopes)
    }

    /// Totals of the blob counters since this process started, and the size of
    /// the dwell queue: the buckets by the directory, and the entries counted up
    /// to 1,000 with bounded reads (`queue_depth_capped`).
    pub async fn blob_counter_report(&self) -> crate::db_operations::BlobCounterReport {
        self.db_ops().records().blob_counter_report_full().await
    }

    /// The retention window of kept conflict records, the janitor's last run,
    /// and the size of the queue: the buckets by the directory, and the entries
    /// counted up to 1,000 with bounded reads. A bucket that cannot be read
    /// makes the entry count a lower bound (`queue_depth_capped`). Only a
    /// failure to read the directory leaves `queue_depth` empty.
    pub async fn conflict_retention_report(&self) -> crate::db_operations::ConflictRetentionReport {
        let records = self.db_ops().records();
        let mut report = records.retention_state().snapshot();
        let queue = records.retention_state().queue();
        if let Ok(stats) = records
            .time_queue_stats(&queue, CONFLICT_QUEUE_DEPTH_CAP)
            .await
        {
            report.queue_depth = Some(stats.entries);
            report.queue_depth_capped = stats.capped;
            report.queue_buckets = Some(stats.buckets);
        }
        report
    }
}

/// The queue depth that the status counts up to.
const CONFLICT_QUEUE_DEPTH_CAP: u64 = 1_000;
