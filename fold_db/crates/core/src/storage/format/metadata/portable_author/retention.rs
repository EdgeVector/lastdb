//! Retire historical mutation-log entries already covered by both cursors.

use super::{invalid, outgoing::entry_key, Metadata};
use crate::storage::error::StorageResult;
use lastdb_model::portable_mutation::StreamId;

impl Metadata {
    pub(crate) async fn portable_writer_caught_up(&self) -> StorageResult<bool> {
        let state = self.local_author().await?;
        Ok(state.shared.is_some_and(|shared| {
            shared.progress.uploaded_through.unwrap_or_default() == shared.progress.applied_through
        }))
    }

    pub(crate) async fn stage_legacy_log_cleanup(
        &self,
        after: Vec<u8>,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.require_scope()?;
        let mut state = self.local_author().await?;
        let shared = state
            .shared
            .as_mut()
            .ok_or_else(|| invalid("mutation log is not bound"))?;
        let Some(uploaded) = shared.progress.uploaded_through else {
            return Ok(None);
        };
        let covered = uploaded.min(shared.progress.applied_through);
        if covered == 0 {
            return Ok(None);
        }
        let stream: StreamId = shared
            .scope
            .source_stream
            .as_str()
            .parse()
            .map_err(invalid)?;
        let first = entry_key(&stream, 1);
        let start = if after.is_empty() {
            first.into_bytes()
        } else if after.len() == first.len()
            && after.starts_with(&first.as_bytes()[..first.len() - 20])
        {
            [after, vec![0]].concat()
        } else {
            return Err(invalid(
                "mutation log cleanup cursor differs from its stream",
            ));
        };
        let mut end = entry_key(&stream, covered).into_bytes();
        end.push(0);
        let rows = self
            .metadata_store
            .scan_range_paged(&start, &end, 64)
            .await?;
        let Some((last, _)) = rows.last() else {
            return Ok(None);
        };
        let last = last.clone();
        if shared.uploaded_through_ms.is_none() {
            shared.uploaded_through_ms = self.portable_uploaded_time(shared).await?;
            self.stage_local_author(&state).await?;
        }
        self.stage_deletes(rows.into_iter().map(|(key, _)| key).collect())
            .await?;
        Ok(Some(last))
    }
}
