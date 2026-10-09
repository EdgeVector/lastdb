//! Download cursor persistence.

use super::super::super::*;

impl SyncEngine {
    /// Persist a download cursor to Sled.
    pub(crate) async fn save_download_cursor(&self, prefix: &str, seq: u64) {
        let cursor_key = format!("cursor:{prefix}");
        let value =
            match self.encode_cursor(seq) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(
                    "failed to seal download cursor for '{}' at seq {}: {} — cursor not persisted",
                    prefix, seq, e
                );
                    return;
                }
            };
        match self.cursor_store.open_namespace("sync_cursors").await {
            Ok(kv) => {
                if let Err(e) = kv.put(cursor_key.as_bytes(), value).await {
                    tracing::error!(
                        "failed to save download cursor for '{}' at seq {}: {} — next restart will re-download from last saved cursor",
                        prefix, seq, e
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    "failed to open sync_cursors namespace for '{}': {} — cursor not persisted",
                    prefix,
                    e
                );
            }
        }
    }
}
