//! Use the full cloud directory and durable local cursors for delete cleanup.

use super::{inbox::decode_cursor, inbox::progress_key, invalid, Metadata};
use crate::storage::error::StorageResult;
use cloud_backup_protocol::shared::peer::{PeerHeads, PeerHeadsRequest};
use lastdb_model::portable_mutation::StreamId;

impl Metadata {
    pub(crate) async fn portable_delete_cutoff(
        &self,
        directory: &PeerHeads,
    ) -> StorageResult<Option<u64>> {
        self.require_scope()?;
        let author = self.local_author().await?;
        let shared = author
            .shared
            .as_ref()
            .ok_or_else(|| invalid("delete cleanup requires a bound writer"))?;
        directory
            .validate_for(&PeerHeadsRequest {
                reader: shared.scope.clone(),
            })
            .map_err(invalid)?;
        let state = self.replay_state().await?;
        if state.local_stream_id.as_str() != shared.scope.source_stream.as_str() {
            return Err(invalid(
                "delete cleanup writer differs from its replay directory",
            ));
        }
        let peers = directory
            .streams
            .iter()
            .map(|cut| {
                cut.source_stream
                    .as_str()
                    .parse::<StreamId>()
                    .map_err(invalid)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        // A directory change holds cleanup while ordinary peer catch-up continues.
        if state.participating_streams.len() != peers.len() + 1
            || peers
                .iter()
                .any(|stream| !state.participating_streams.contains(stream))
        {
            return Ok(None);
        }
        let Some(mut cutoff) = self.portable_uploaded_time(shared).await? else {
            return Ok(None);
        };
        let keys = peers
            .iter()
            .map(|stream| progress_key(stream).into_bytes())
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != peers.len() {
            return Err(invalid("incomplete delete cleanup stream directory"));
        }
        for (cut, bytes) in directory.streams.iter().zip(rows) {
            let cursor = decode_cursor(bytes.as_deref())?;
            let Some(head) = &cut.head else {
                return Ok(None);
            };
            if cursor.applied_through > head.sequence.get()
                || cursor.applied_through < cut.reclaimed_through.get()
            {
                return Ok(None);
            }
            let Some(time) = cursor.applied_through_ms else {
                return Ok(None);
            };
            cutoff = cutoff.min(time);
        }
        Ok(Some(cutoff))
    }
}
