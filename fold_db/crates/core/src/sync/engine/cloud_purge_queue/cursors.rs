//! Archive and reset only the retired targets' download cursors.

use super::*;

impl SyncEngine {
    pub(super) async fn detach_generation_cursors(
        &self,
        context: &DetachContext<'_>,
        receipt: &mut CloudPurgeQueueDetachReceipt,
    ) -> SyncResult<()> {
        // The old allocation floor remains, so a new write cannot reuse a retired seq.
        let cursors = self.cursor_store.open_namespace("sync_cursors").await?;
        let cursor_keys: Vec<_> = context
            .retired_prefixes
            .iter()
            .map(|prefix| format!("cursor:{prefix}").into_bytes())
            .collect();
        let old = cursors.get_many(cursor_keys.clone()).await?;
        let archived: Vec<_> = cursor_keys
            .iter()
            .cloned()
            .zip(old)
            .filter_map(|(key, old)| old.map(|old| (key, old)))
            .collect();
        if !archived.is_empty() {
            self.archive_detach_page(context, "sync_cursors", &archived, receipt)?;
        }
        cursors
            .batch_put(
                cursor_keys
                    .into_iter()
                    .map(|key| Ok((key, self.encode_cursor(0).map_err(SyncError::Storage)?)))
                    .collect::<SyncResult<Vec<_>>>()?,
            )
            .await?;
        cursors.flush().await?;
        self.download_cursors
            .lock()
            .await
            .retain(|prefix, _| !context.retired_prefixes.contains(prefix));
        Ok(())
    }
}
