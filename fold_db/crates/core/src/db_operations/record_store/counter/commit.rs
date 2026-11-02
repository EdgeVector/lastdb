//! The row commit: one native batch of rows, deletes, guarded deletes, and the
//! release of the conflict rows of a home.
//!
//! The retention janitor, the queue repairs, the blob queue repair and the
//! field history expiry write through [`RecordStore::commit_rows`]. It runs inside
//! the native write request scope, so every read sees the committed root, the
//! accepted batches and the pending batch of the request, and the one
//! `batch_mutate` at the end joins that pending batch.
//!
//! A release names a conflict row (`rcf:`). The row holds no record or file blob
//! reference, so a release only deletes: the row, its queue rows, and the marker
//! of the record when no live entry of it is left (`record_conflicts/release.rs`).

use crate::db_operations::record_store::record_conflicts::is_record_conflict_row_key;
use crate::db_operations::record_store::record_conflicts::RecordReleases;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde_json::Value;
use std::collections::BTreeSet;

/// One kept conflict row to release.
pub(crate) struct ReleaseRequest {
    /// The full storage key of the conflict row.
    pub history: String,
    /// The other rows of the record (the queue entry). They go with the row, or
    /// when it is already gone.
    pub deletes: Vec<Vec<u8>>,
}

/// A delete that runs only while the key still holds the value the caller read.
/// The commit reads the key in the write gate. A key that holds other bytes, or
/// none, is left alone.
pub(crate) struct GuardedDelete {
    pub key: Vec<u8>,
    pub expected: Vec<u8>,
}

/// Everything one row commit writes.
#[derive(Default)]
pub(crate) struct RowChanges {
    /// Conflict rows this batch releases at the end of their window.
    pub releases: Vec<ReleaseRequest>,
    /// Rows the batch puts: queue rows, bucket rows, the retention window.
    pub rows: Vec<(String, Value)>,
    /// Keys the batch deletes.
    pub deletes: Vec<Vec<u8>>,
    /// Keys that the batch deletes only if they still hold the bytes read
    /// before (the retirement of a queue bucket).
    pub guarded_deletes: Vec<GuardedDelete>,
}

/// What one row commit did.
#[derive(Debug, Default)]
pub(crate) struct RowCommit {
    /// Conflict rows released, and releases whose row was already gone.
    pub records_released: u64,
    /// Guarded deletes that found the expected bytes and deleted the key.
    pub guarded_deleted: u64,
}

impl RecordStore {
    /// Write one batch of rows and deletes.
    ///
    /// The caller holds the native write gate through a request scope. A call
    /// outside a scope on a native store is a defect and fails here, because the
    /// reads of the releases and the guarded deletes must see the batch they
    /// join.
    pub(crate) async fn commit_rows(&self, changes: RowChanges) -> Result<RowCommit, SchemaError> {
        self.require_write_scope()?;
        let RowChanges {
            releases,
            rows,
            mut deletes,
            guarded_deletes,
        } = changes;
        let released = self.release_conflict_rows(releases).await?;
        deletes.extend(released.deletes);
        let guarded = self.guarded_delete_keys(guarded_deletes).await?;
        let commit = RowCommit {
            records_released: released.released + released.gone,
            guarded_deleted: guarded.len() as u64,
        };
        deletes.extend(guarded);
        let mut mutations = Vec::with_capacity(rows.len() + deletes.len());
        for (key, value) in rows {
            let bytes = serde_json::to_vec(&value)
                .map_err(|error| SchemaError::InvalidData(format!("serialize row: {error}")))?;
            mutations.push(KvMutation::put(key.into_bytes(), bytes));
        }
        mutations.extend(deletes.into_iter().map(KvMutation::delete));
        if !mutations.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("commit rows: {error}")))?;
        }
        Ok(commit)
    }

    /// Release each conflict row once. A release that names another row is a
    /// defect of its caller: it is logged and released nothing.
    async fn release_conflict_rows(
        &self,
        releases: Vec<ReleaseRequest>,
    ) -> Result<RecordReleases, SchemaError> {
        let mut seen = BTreeSet::new();
        let releases: Vec<ReleaseRequest> = releases
            .into_iter()
            .filter(|release| seen.insert(release.history.clone()))
            .filter(|release| {
                let row = is_record_conflict_row_key(&release.history);
                if !row {
                    tracing::error!(history = %release.history, "release names a row that is not a conflict row; nothing released");
                }
                row
            })
            .collect();
        self.release_record_conflicts(&releases).await
    }

    /// A native writer needs a request scope.
    pub(in crate::db_operations::record_store) fn require_write_scope(
        &self,
    ) -> Result<(), SchemaError> {
        if crate::storage::disk_pack::transaction::publication::active(
            &self.namespaced_store.disk_pack_runtime(),
        ) {
            return Ok(());
        }
        Err(SchemaError::InvalidData(
            "row commits require a complete native write scope".into(),
        ))
    }
}
