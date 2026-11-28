//! Entries of this device's stream that are not record or schema writes, and the
//! signed-log rows the node keeps beside them.
//!
//! The node signs these entries itself (a grant, a lease, a heads entry, a proposal). The
//! store gives each one its place in the stream and its order value, puts it in the spool,
//! and moves the chain row, all in one commit with the rows the caller stages.

use super::MutationManager;
use crate::schema::types::LogRow;
use crate::schema::SchemaError;
use crate::storage::disk_backup::accepted::{self, LocalEntry, SignedLocal};
use crate::storage::disk_pack::transaction;
use std::sync::Arc;

impl MutationManager {
    /// Append one entry of kind `kind` to this store's device stream, in one commit with
    /// `rows`. Its order value is at least `at_least_order` and above every earlier entry
    /// of the stream. `sign` gets the entry before it (none for the first entry) and the
    /// order value, and returns the signed entry. Refused when the store does not capture
    /// its writes, or inside the apply of another stream's entry.
    pub async fn append_log_entry<F>(
        &self,
        kind: &str,
        at_least_order: u64,
        sign: F,
        rows: &[LogRow],
    ) -> Result<LocalEntry, SchemaError>
    where
        F: FnOnce(Option<&LocalEntry>, u64) -> Result<SignedLocal, String> + Send,
    {
        let runtime = self.db_ops.native_runtime();
        if !accepted::capture_enabled(&runtime) {
            return Err(SchemaError::InvalidData(
                "this store does not capture its writes".into(),
            ));
        }
        let metadata = self.db_ops.metadata();
        transaction::scope(Arc::clone(&runtime), async {
            let previous = metadata.local_replay_head(&self.writer_id).await?;
            // The clock gives a millisecond above this floor, so the order value is above
            // the entry before and at least `at_least_order`.
            let floor = previous
                .as_ref()
                .map_or(0, |held| held.order)
                .max(at_least_order.saturating_sub(1));
            let order = metadata
                .stage_local_author_ms(&self.writer_id, Some(floor))
                .await?;
            let head = accepted::stage_log_entry(&runtime, kind, previous.as_ref(), order, sign)?;
            metadata.stage_signed_log_rows(rows).await?;
            metadata.stage_local_replay_head(&head).await?;
            Ok::<_, SchemaError>(head)
        })
        .await
    }

    /// The value of one signed-log row. Inside an open transaction (such as the apply of
    /// [`Self::write_replay_at_head`]) it reads what that transaction staged.
    pub async fn signed_log_row(&self, key: &str) -> Result<Option<Vec<u8>>, SchemaError> {
        let metadata = self.db_ops.metadata();
        transaction::scope(self.db_ops.native_runtime(), metadata.signed_log_row(key)).await
    }

    /// One page of the signed-log rows whose key starts with `prefix`, in key order,
    /// after the key `after`, at most `limit` (up to `SIGNED_LOG_PAGE_MAX`). Keys come
    /// back as the caller gave them.
    pub async fn signed_log_rows(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, SchemaError> {
        let metadata = self.db_ops.metadata();
        transaction::scope(
            self.db_ops.native_runtime(),
            metadata.signed_log_rows(prefix, after, limit),
        )
        .await
    }

    /// Put or delete signed-log rows. Inside an open transaction the rows commit with
    /// it. Otherwise they commit on their own.
    pub async fn stage_signed_log_rows(&self, rows: &[LogRow]) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        transaction::scope(
            self.db_ops.native_runtime(),
            metadata.stage_signed_log_rows(rows),
        )
        .await
    }
}
