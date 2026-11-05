//! Read the shared cut from the same committed root as its record inventory.

use super::{inbox, invalid, JournalSchema, Metadata, PROGRESS_PREFIX};
use crate::storage::error::StorageResult;
use cloud_backup_protocol::shared::{LogHead, WriterStream};
use lastdb_model::portable_mutation::{StreamId, StreamProgress};
use std::collections::BTreeMap;

pub(crate) struct CheckpointDirectory {
    pub(crate) writer: WriterStream,
    pub(crate) schemas: BTreeMap<String, JournalSchema>,
    pub(crate) local_head: Option<LogHead>,
    pub(crate) progress: BTreeMap<StreamId, StreamProgress>,
    pub(crate) delete_reclaimed_through_ms: Option<u64>,
}

impl Metadata {
    pub(crate) async fn portable_checkpoint_directory(&self) -> StorageResult<CheckpointDirectory> {
        let shared = self
            .local_author()
            .await?
            .shared
            .ok_or_else(|| invalid("checkpoint requires a bound shared writer"))?;
        let replay = self.replay_state().await?;
        if replay.local_stream_id.as_str() != shared.scope.source_stream.as_str() {
            return Err(invalid(
                "checkpoint writer differs from its replay directory",
            ));
        }
        let keys = replay
            .participating_streams
            .iter()
            .map(|stream| format!("{PROGRESS_PREFIX}{stream}").into_bytes())
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != replay.participating_streams.len() {
            return Err(invalid("incomplete checkpoint stream directory"));
        }
        let progress: BTreeMap<_, _> = replay
            .participating_streams
            .into_iter()
            .zip(rows)
            .map(|(stream, bytes)| Ok((stream, inbox::decode_cursor(bytes.as_deref())?)))
            .collect::<StorageResult<_>>()?;
        let mut local = shared.progress;
        if local.uploaded_through.unwrap_or_default() != local.applied_through {
            return Err(invalid("checkpoint has an unaccepted local prefix"));
        }
        local.uploaded_through = None;
        if progress.get(&replay.local_stream_id) != Some(&local) {
            return Err(invalid("checkpoint author differs from its applied cursor"));
        }
        Ok(CheckpointDirectory {
            writer: shared.scope,
            schemas: shared.schemas,
            local_head: shared.uploaded_head,
            progress,
            delete_reclaimed_through_ms: replay.delete_reclaimed_through_ms,
        })
    }
}
