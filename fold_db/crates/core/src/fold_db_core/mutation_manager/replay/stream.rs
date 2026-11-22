//! Per-stream progress of a signed log, and the one-transaction apply of an entry.

use super::MutationManager;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::ReplayStreamHead;
use std::future::Future;
use std::sync::Arc;

impl MutationManager {
    /// The head row of each stream, in the order asked. A stream this node has
    /// not applied an entry of has none.
    pub async fn replay_stream_heads(
        &self,
        streams: &[String],
    ) -> Result<Vec<Option<ReplayStreamHead>>, SchemaError> {
        let metadata = self.db_ops.metadata();
        transaction::scope(self.db_ops.native_runtime(), async {
            let mut heads = Vec::with_capacity(streams.len());
            for stream in streams {
                heads.push(metadata.replay_stream_head(stream).await?);
            }
            Ok::<_, SchemaError>(heads)
        })
        .await
    }

    /// Write the head row of a stream with no entry applied: the own stream
    /// after an acknowledgement, or a stream that failed verification.
    pub async fn set_replay_stream_head(
        &self,
        stream: &str,
        head: &ReplayStreamHead,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        transaction::scope(self.db_ops.native_runtime(), async {
            metadata.stage_replay_stream_head(stream, head).await?;
            Ok::<(), SchemaError>(())
        })
        .await
    }

    /// Apply the next entry of a stream. `replay` and the new head row commit in
    /// one transaction, and the write is not captured for upload again.
    ///
    /// `Ok(None)` means the entry was applied before: the held head is at or
    /// past `next`. An entry that skips a sequence number, or a frozen stream,
    /// is an error and nothing is written.
    pub async fn write_replay_at_head<T>(
        &self,
        stream: &str,
        next: &ReplayStreamHead,
        replay: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<Option<T>, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        let metadata = self.db_ops.metadata();
        runtime
            .replay_accepted(transaction::scope(Arc::clone(&runtime), async {
                let held = metadata.replay_stream_head(stream).await?;
                if held.as_ref().is_some_and(|head| head.frozen) {
                    return Err(SchemaError::Blocked("the stream is frozen".into()));
                }
                let held_seq = held.map_or(0, |head| head.seq);
                if next.seq <= held_seq {
                    return Ok(None);
                }
                if next.seq != held_seq + 1 {
                    return Err(SchemaError::InvalidData(
                        "a stream entry skips a sequence number".into(),
                    ));
                }
                let applied = replay.await?;
                metadata.stage_replay_stream_head(stream, next).await?;
                Ok(Some(applied))
            }))
            .await
    }

    /// Run `replay` in one transaction that is not captured for upload. A local
    /// write that has been signed settles its pending marker this way.
    pub async fn write_replay<T>(
        &self,
        replay: impl Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(transaction::scope(Arc::clone(&runtime), replay))
            .await
    }
}
