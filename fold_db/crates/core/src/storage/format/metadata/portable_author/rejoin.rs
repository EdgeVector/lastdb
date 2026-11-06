//! Seal the cloud recovery cut while retaining the returning device's author.

use super::{
    author_row, inbox, invalid, EncodedMetadata, JournalSchema, LocalAuthor, Metadata,
    RestoreOwner, REPLAY_STATE_KEY, RESTORE_OWNER_KEY,
};
use crate::storage::error::StorageResult;
use cloud_backup_protocol::shared::{LogHead, WriterStream, MAX_WRITERS};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{HomeReplayState, StreamId, StreamProgress};
use std::collections::BTreeMap;

impl Metadata {
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

    /// Cloud recovery already verified every retained peer range up to the cut.
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
