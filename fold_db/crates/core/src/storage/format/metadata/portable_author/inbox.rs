//! Complete authenticated peer ranges wait durably for their missing prefix.

use super::{invalid, EncodedMetadata, Metadata, PROGRESS_PREFIX, REPLAY_STATE_KEY};
use crate::storage::error::StorageResult;
use lastdb_model::portable_mutation::{HomeReplayState, SchemaIdentity, StreamId, StreamProgress};
use lastdb_model::portable_replay::{
    LogRangeId, VerifiedLogRange, MAX_LOG_ENTRIES, MAX_REPLAY_BYTES,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const INDEX_PREFIX: &str = "portable-inbox-index:";
const RANGE_PREFIX: &str = "portable-inbox:";
const MAX_QUEUED_RANGES: usize = 4;
// A complete accepted range has its own aggregate sealed-file allowance.
const MAX_APPLIED_RANGES: usize = 1;
const MAX_SELECTED_STREAMS: usize = 4;
const MAX_SEQUENCE_AHEAD: u64 = 8192;

pub(crate) struct InboxWindow {
    pub(crate) progress: BTreeMap<StreamId, StreamProgress>,
    pub(crate) cutoff: Option<u64>,
    pub(crate) ranges: Vec<VerifiedLogRange>,
    indexes: BTreeMap<StreamId, Vec<String>>,
}

impl Metadata {
    pub(crate) async fn stage_portable_peer(
        &self,
        stream: &StreamId,
        first_authored_at_ms: u64,
    ) -> StorageResult<()> {
        self.require_scope()?;
        cloud_backup_protocol::Id::try_from(stream.as_str().to_owned()).map_err(invalid)?;
        let mut state = self.replay_state().await?;
        if stream == &state.local_stream_id {
            return Err(invalid("the local writer is not a peer"));
        }
        if state.participating_streams.contains(stream) {
            return Ok(());
        }
        if state.participating_streams.len() >= cloud_backup_protocol::shared::MAX_WRITERS
            || state
                .delete_reclaimed_through_ms
                .is_some_and(|cutoff| first_authored_at_ms <= cutoff)
        {
            return Err(invalid(
                "peer stream is outside the admitted replay boundary",
            ));
        }
        state.participating_streams.insert(stream.clone());
        self.stage(encoded(REPLAY_STATE_KEY.into(), &state)?).await
    }

    pub(crate) async fn stage_portable_range(
        &self,
        id: LogRangeId,
        bytes: Vec<u8>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let range = VerifiedLogRange::decode(id, &bytes).map_err(invalid)?;
        if range
            .entries()
            .iter()
            .any(|entry| matches!(&entry.schema, SchemaIdentity::Local(_)))
        {
            return Err(invalid("peer range contains a local-only schema"));
        }
        let id = range.id();
        let stream = id.source_stream();
        let names = [
            REPLAY_STATE_KEY.into(),
            progress_key(stream),
            index_key(stream),
        ];
        let rows = self
            .metadata_store
            .get_many(names.map(String::into_bytes).into())
            .await?;
        let [state, cursor, index]: [_; 3] = rows
            .try_into()
            .map_err(|_| invalid("incomplete portable inbox state"))?;
        let state = decode_state(state.as_deref())?;
        require_peer(&state, stream)?;
        let cursor = decode_cursor(cursor.as_deref())?;
        if id.last_sequence() <= cursor.applied_through {
            return Ok(());
        }
        let mut index = decode_index(stream, index.as_deref())?;
        let name = id.to_string();
        let key = range_key(&name);
        if index.contains(&name) {
            if self.metadata_store.get(key.as_bytes()).await?.as_deref() != Some(bytes.as_slice()) {
                return Err(invalid("conflicting bytes for an immutable inbox range"));
            }
            return Ok(());
        }
        for prior in &index {
            let prior: LogRangeId = prior.parse().map_err(invalid)?;
            if prior.first_sequence() <= id.last_sequence()
                && id.first_sequence() <= prior.last_sequence()
            {
                return Err(invalid("authenticated peer ranges overlap"));
            }
        }
        let next = cursor.applied_through + 1;
        if id.first_sequence() > next.saturating_add(MAX_SEQUENCE_AHEAD)
            || index.len() >= MAX_QUEUED_RANGES
            || (index.len() + 1 == MAX_QUEUED_RANGES && id.first_sequence() > next)
        {
            return Err(invalid("peer range is outside the bounded inbox window"));
        }
        index.push(name);
        index.sort_unstable();
        self.stage_batch(vec![
            EncodedMetadata { key, bytes },
            encoded(index_key(stream), &index)?,
        ])
        .await
    }

    pub(crate) async fn portable_peer_progress(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> StorageResult<BTreeMap<StreamId, StreamProgress>> {
        if streams.len() > MAX_SELECTED_STREAMS {
            return Err(invalid("select a bounded set of peer streams"));
        }
        let names = std::iter::once(REPLAY_STATE_KEY.as_bytes().to_vec())
            .chain(
                streams
                    .iter()
                    .map(|stream| progress_key(stream).into_bytes()),
            )
            .collect();
        let rows = self.metadata_store.get_many(names).await?;
        if rows.len() != streams.len() + 1 {
            return Err(invalid("incomplete peer replay cursors"));
        }
        let state = decode_state(rows[0].as_deref())?;
        if streams.contains(&state.local_stream_id) {
            return Err(invalid("the local writer is not a peer"));
        }
        streams
            .iter()
            .zip(&rows[1..])
            .filter(|(stream, _)| state.participating_streams.contains(*stream))
            .map(|(stream, row)| Ok((stream.clone(), decode_cursor(row.as_deref())?)))
            .collect()
    }

    pub(crate) async fn portable_inbox(
        &self,
        streams: &BTreeSet<StreamId>,
    ) -> StorageResult<InboxWindow> {
        if streams.is_empty() || streams.len() > MAX_SELECTED_STREAMS {
            return Err(invalid("select a bounded set of peer streams"));
        }
        let names = std::iter::once(REPLAY_STATE_KEY.as_bytes().to_vec())
            .chain(streams.iter().flat_map(|stream| {
                [
                    progress_key(stream).into_bytes(),
                    index_key(stream).into_bytes(),
                ]
            }))
            .collect();
        let rows = self.metadata_store.get_many(names).await?;
        if rows.len() != 1 + streams.len() * 2 {
            return Err(invalid("incomplete peer replay window"));
        }
        let state = decode_state(rows[0].as_deref())?;
        let mut window = InboxWindow {
            progress: BTreeMap::new(),
            cutoff: state.delete_reclaimed_through_ms,
            ranges: Vec::new(),
            indexes: BTreeMap::new(),
        };
        let mut selected = Vec::new();
        let mut count = 0;
        for (stream, pair) in streams.iter().zip(rows[1..].chunks_exact(2)) {
            require_peer(&state, stream)?;
            let cursor = decode_cursor(pair[0].as_deref())?;
            let index = decode_index(stream, pair[1].as_deref())?;
            let mut through = cursor.applied_through;
            for name in &index {
                let id: LogRangeId = name.parse().map_err(invalid)?;
                let entries = (id.last_sequence() - id.first_sequence() + 1) as usize;
                if id.first_sequence() > through.saturating_add(1)
                    || count + entries > MAX_LOG_ENTRIES
                    || selected.len() == MAX_APPLIED_RANGES
                {
                    break;
                }
                count += entries;
                through = through.max(id.last_sequence());
                selected.push(id);
            }
            window.progress.insert(stream.clone(), cursor);
            window.indexes.insert(stream.clone(), index);
        }
        self.read_inbox_ranges(selected, &mut window).await?;
        Ok(window)
    }

    async fn read_inbox_ranges(
        &self,
        selected: Vec<LogRangeId>,
        window: &mut InboxWindow,
    ) -> StorageResult<()> {
        let rows = self
            .metadata_store
            .get_many(
                selected
                    .iter()
                    .map(|id| range_key(&id.to_string()).into_bytes())
                    .collect(),
            )
            .await?;
        if rows.len() != selected.len() {
            return Err(invalid("incomplete inbox file read"));
        }
        let mut total = 0;
        for (id, row) in selected.into_iter().zip(rows) {
            let bytes = row.ok_or_else(|| invalid("indexed inbox file is missing"))?;
            total += bytes.len();
            if total > MAX_REPLAY_BYTES {
                return Err(invalid("peer replay window exceeds its byte bound"));
            }
            window
                .ranges
                .push(VerifiedLogRange::decode(id, &bytes).map_err(invalid)?);
        }
        Ok(())
    }

    pub(crate) async fn stage_portable_inbox_progress(
        &self,
        mut window: InboxWindow,
        progress: BTreeMap<StreamId, StreamProgress>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut rows = Vec::new();
        let mut deletes = Vec::new();
        let mut changed_indexes = BTreeSet::new();
        for range in &window.ranges {
            let id = range.id();
            let stream = id.source_stream();
            let cursor = progress.get(stream).unwrap_or(&window.progress[stream]);
            if id.last_sequence() <= cursor.applied_through {
                let name = id.to_string();
                let index = window
                    .indexes
                    .get_mut(stream)
                    .ok_or_else(|| invalid("inbox index missing"))?;
                index.retain(|entry| entry != &name);
                changed_indexes.insert(stream);
                deletes.push(range_key(&name).into_bytes());
            }
        }
        for stream in changed_indexes {
            rows.push(encoded(index_key(stream), &window.indexes[stream])?);
        }
        for (stream, cursor) in progress {
            rows.push(encoded(progress_key(&stream), &cursor)?);
        }
        if !rows.is_empty() {
            self.stage_batch(rows).await?;
        }
        if !deletes.is_empty() {
            self.stage_deletes(deletes).await?;
        }
        Ok(())
    }

    pub(super) async fn replay_state(&self) -> StorageResult<HomeReplayState> {
        let row = self.metadata_store.get(REPLAY_STATE_KEY.as_bytes()).await?;
        decode_state(row.as_deref())
    }

    pub(crate) async fn portable_delete_cutoff(
        &self,
        participants: &BTreeSet<StreamId>,
    ) -> StorageResult<Option<u64>> {
        self.require_scope()?;
        let state = self.replay_state().await?;
        if &state.participating_streams != participants {
            return Err(invalid(
                "delete cleanup requires the complete admitted writer directory",
            ));
        }
        let keys = state
            .participating_streams
            .iter()
            .map(|stream| progress_key(stream).into_bytes())
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != state.participating_streams.len() {
            return Err(invalid("incomplete delete cleanup stream directory"));
        }
        let mut cutoff = u64::MAX;
        for bytes in rows {
            let cursor = decode_cursor(bytes.as_deref())?;
            let Some(time) = cursor.applied_through_ms else {
                return Ok(None);
            };
            cutoff = cutoff.min(time);
        }
        Ok(Some(cutoff))
    }

    pub(crate) async fn stage_delete_reclaimed(
        &self,
        through_ms: u64,
        cutoff_ms: u64,
    ) -> StorageResult<()> {
        self.require_scope()?;
        if through_ms >= cutoff_ms {
            return Err(invalid("delete cleanup crossed its stream cutoff"));
        }
        let mut state = self.replay_state().await?;
        if state
            .delete_reclaimed_through_ms
            .is_none_or(|prior| through_ms > prior)
        {
            state.delete_reclaimed_through_ms = Some(through_ms);
            self.stage(encoded(REPLAY_STATE_KEY.into(), &state)?)
                .await?;
        }
        Ok(())
    }
}

fn encoded(key: String, value: &impl Serialize) -> StorageResult<EncodedMetadata> {
    Ok(EncodedMetadata {
        key,
        bytes: serde_json::to_vec(value).map_err(invalid)?,
    })
}

pub(super) fn decode_state(bytes: Option<&[u8]>) -> StorageResult<HomeReplayState> {
    let state: HomeReplayState =
        serde_json::from_slice(bytes.ok_or_else(|| invalid("shared replay state is missing"))?)
            .map_err(invalid)?;
    if !state.participating_streams.contains(&state.local_stream_id)
        || state.participating_streams.len() > cloud_backup_protocol::shared::MAX_WRITERS
    {
        return Err(invalid("invalid shared replay participants"));
    }
    Ok(state)
}

fn decode_cursor(bytes: Option<&[u8]>) -> StorageResult<StreamProgress> {
    let cursor: StreamProgress = bytes
        .map(serde_json::from_slice)
        .transpose()
        .map_err(invalid)?
        .unwrap_or_default();
    cursor.validate().map_err(invalid)?;
    if cursor.uploaded_through.is_some() {
        return Err(invalid("peer cursor contains local upload progress"));
    }
    Ok(cursor)
}

fn decode_index(stream: &StreamId, bytes: Option<&[u8]>) -> StorageResult<Vec<String>> {
    let index: Vec<String> = bytes
        .map(serde_json::from_slice)
        .transpose()
        .map_err(invalid)?
        .unwrap_or_default();
    if index.len() > MAX_QUEUED_RANGES || index.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("invalid inbox range index"));
    }
    for name in &index {
        let id: LogRangeId = name.parse().map_err(invalid)?;
        if id.source_stream() != stream {
            return Err(invalid("inbox index points to a different peer"));
        }
    }
    Ok(index)
}

fn require_peer(state: &HomeReplayState, stream: &StreamId) -> StorageResult<()> {
    if stream == &state.local_stream_id || !state.participating_streams.contains(stream) {
        return Err(invalid("range belongs to an unadmitted peer"));
    }
    Ok(())
}

fn progress_key(stream: &StreamId) -> String {
    format!("{PROGRESS_PREFIX}{stream}")
}
fn index_key(stream: &StreamId) -> String {
    format!("{INDEX_PREFIX}{stream}")
}
fn range_key(name: &str) -> String {
    format!("{RANGE_PREFIX}{name}")
}
