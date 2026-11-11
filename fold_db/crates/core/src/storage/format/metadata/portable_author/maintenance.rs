//! Portable checkpoint directory, delete cutoff, log retention, and writer rejoin.

use std::collections::BTreeMap;

use cloud_backup_protocol::shared::peer::{PeerHeads, PeerHeadsRequest};
use cloud_backup_protocol::shared::{LogHead, WriterStream, MAX_WRITERS};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{HomeReplayState, StreamId, StreamProgress};

use super::{
    author_row, inbox, invalid, outgoing::entry_key, EncodedMetadata, JournalSchema, LocalAuthor,
    Metadata, RestoreOwner, PROGRESS_PREFIX, REPLAY_STATE_KEY, RESTORE_OWNER_KEY,
};
use crate::storage::error::StorageResult;

pub(crate) struct CheckpointDirectory {
    pub(crate) writer: WriterStream,
    pub(crate) schemas: BTreeMap<String, JournalSchema>,
    pub(crate) local_head: Option<LogHead>,
    pub(crate) progress: BTreeMap<StreamId, StreamProgress>,
    pub(crate) delete_reclaimed_through_ms: Option<u64>,
}

impl Metadata {
    pub(crate) async fn portable_checkpoint_directory(
        &self,
    ) -> StorageResult<Option<CheckpointDirectory>> {
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
            return Ok(None);
        }
        local.uploaded_through = None;
        if progress.get(&replay.local_stream_id) != Some(&local) {
            return Err(invalid("checkpoint author differs from its applied cursor"));
        }
        Ok(Some(CheckpointDirectory {
            writer: shared.scope,
            schemas: shared.schemas,
            local_head: shared.uploaded_head,
            progress,
            delete_reclaimed_through_ms: replay.delete_reclaimed_through_ms,
        }))
    }

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
            .map(|stream| inbox::progress_key(stream).into_bytes())
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != peers.len() {
            return Err(invalid("incomplete delete cleanup stream directory"));
        }
        for (cut, bytes) in directory.streams.iter().zip(rows) {
            let cursor = inbox::decode_cursor(bytes.as_deref())?;
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

    pub(crate) async fn stage_portable_rejoin(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        writer: &WriterStream,
        device: &Id,
        expected: Option<&LogHead>,
        schemas: &BTreeMap<String, JournalSchema>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let stream: StreamId = writer.source_stream.as_str().parse().map_err(invalid)?;
        if progress.len() > MAX_WRITERS
            || (progress.len() == MAX_WRITERS && !progress.contains_key(&stream))
        {
            return Err(invalid(
                "returning writer exceeds the cloud directory capacity",
            ));
        }
        if let Some(head) = expected {
            head.validate().map_err(invalid)?;
        }
        let owner = RestoreOwner {
            database: writer.database_id.clone(),
            device: device.clone(),
        };
        if let Some(bytes) = self
            .metadata_store
            .get(RESTORE_OWNER_KEY.as_bytes())
            .await?
        {
            let prior: RestoreOwner = serde_json::from_slice(&bytes).map_err(invalid)?;
            if prior != owner {
                return Err(invalid(
                    "returning writer belongs to another database or device",
                ));
            }
        }

        let mut author = self.local_author().await?;
        if author.stream == stream {
            return Err(invalid("returning writer shares its device-only stream"));
        }
        let shared = author
            .shared
            .as_mut()
            .ok_or_else(|| invalid("returning writer is not bound"))?;
        if &shared.scope != writer || &shared.schemas != schemas {
            return Err(invalid(
                "returning writer differs from its bound database or schemas",
            ));
        }
        let own = progress.get(&stream).copied().unwrap_or_default();
        let expected_sequence = expected.map(|head| head.sequence.get());
        let mut local = shared.progress;
        local.uploaded_through = None;
        if local != own
            || shared.progress.applied_through != expected_sequence.unwrap_or_default()
            || shared.progress.uploaded_through != expected_sequence
            || shared.uploaded_head.as_ref() != expected
            || shared
                .uploaded_through_ms
                .is_some_and(|time| Some(time) != own.applied_through_ms)
        {
            return Err(invalid(
                "returning writer is not drained at the held cloud head",
            ));
        }
        shared.uploaded_through_ms = own.applied_through_ms;
        author.clock_floor_ms = author
            .clock_floor_ms
            .max(restored_clock_floor(progress, cutoff)?);
        self.stage_restored_author(progress, cutoff, &stream, &author, owner)
            .await
    }

    pub(super) async fn stage_restored_author(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        local_stream: &StreamId,
        author: &LocalAuthor,
        owner: RestoreOwner,
    ) -> StorageResult<()> {
        let state = HomeReplayState {
            local_stream_id: local_stream.clone(),
            participating_streams: progress
                .keys()
                .cloned()
                .chain([local_stream.clone()])
                .collect(),
            delete_reclaimed_through_ms: cutoff,
        };
        let mut rows = Vec::with_capacity(progress.len() + 3);
        for (stream, cursor) in progress {
            cursor.validate().map_err(invalid)?;
            if cursor.uploaded_through.is_some() {
                return Err(invalid("restored peer cursor has local upload state"));
            }
            rows.push(EncodedMetadata {
                key: inbox::progress_key(stream),
                bytes: serde_json::to_vec(cursor).map_err(invalid)?,
            });
        }
        rows.push(EncodedMetadata {
            key: REPLAY_STATE_KEY.into(),
            bytes: serde_json::to_vec(&state).map_err(invalid)?,
        });
        rows.push(author_row(author)?);
        rows.push(EncodedMetadata {
            key: RESTORE_OWNER_KEY.into(),
            bytes: serde_json::to_vec(&owner).map_err(invalid)?,
        });
        self.stage_batch(rows).await
    }

    pub(crate) async fn stage_reset_rejoin_inbox(
        &self,
        after: Option<&[u8]>,
    ) -> StorageResult<Option<Vec<u8>>> {
        let page = self.rejoin_prefix_page(inbox::INDEX_PREFIX, after).await?;
        let mut deletes = Vec::new();
        for (key, bytes) in page.rows {
            let stream: StreamId = std::str::from_utf8(&key)
                .map_err(invalid)?
                .strip_prefix(inbox::INDEX_PREFIX)
                .ok_or_else(|| invalid("invalid returning inbox index key"))?
                .parse()
                .map_err(invalid)?;
            for range in inbox::decode_index(&stream, Some(&bytes))? {
                deletes.push(inbox::range_key(&range).into_bytes());
            }
            deletes.push(key);
        }
        if after.is_none()
            && self
                .metadata_store
                .exists(inbox::LAST_APPLIED_PEER_KEY.as_bytes())
                .await?
        {
            deletes.push(inbox::LAST_APPLIED_PEER_KEY.as_bytes().to_vec());
        }
        if !deletes.is_empty() {
            self.stage_deletes(deletes).await?;
        }
        Ok(page.resume)
    }
}

pub(super) fn restored_clock_floor(
    progress: &BTreeMap<StreamId, StreamProgress>,
    cutoff: Option<u64>,
) -> StorageResult<Option<u64>> {
    progress
        .values()
        .filter_map(|cursor| cursor.applied_through_ms)
        .chain(cutoff)
        .max()
        .map(|ms| {
            ms.checked_add(1)
                .ok_or_else(|| invalid("delete boundary exhausted"))
        })
        .transpose()
}
