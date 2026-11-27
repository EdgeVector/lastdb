//! Log segments on the relay (`docs/designs/relay-protocol.md`, "Log segments"): the append of
//! one, and a `log.read` whose segments are fetched and opened in place.

use mutation_log::SignedEntry;
use relay_protocol::{
    LogAppend, LogAppendRequest, LogRead, LogReadAnswer, LogReadRequest, ReadItem, SegmentAppend,
    SegmentRef,
};

use futures::{StreamExt, TryStreamExt};

use super::{append_counts, RelayClient, BEHIND_THE_TRIM};

/// Segments of one page fetched at once. Each fetch opens a fresh connection to the store, so a
/// page of one-entry segments fetched one after another costs a round trip and a handshake per
/// entry; a sweep that reads the entries after an old snapshot reads thousands of them.
const SEGMENT_FETCHES: usize = 8;

/// One page of a `log.read`.
pub(in crate::log_sync) struct Page {
    /// The entries in stream order, those of a segment in its place.
    pub(in crate::log_sync) entries: Vec<SignedEntry>,
    pub(in crate::log_sync) cursor: Option<String>,
    /// For each entry, whether a segment held it (its bytes are the segment blob's).
    pub(in crate::log_sync) in_segment: Vec<bool>,
    /// The digests of the segment blobs that held entries of this page.
    pub(in crate::log_sync) segments: Vec<String>,
}

impl RelayClient {
    /// Append a log segment of `entries`, consecutive entries of this node's stream, whose bytes
    /// the caller put on the relay as the blob the segment names. Answers `(stored, duplicate)`
    /// in entries, like [`Self::append`].
    pub(in crate::log_sync) async fn append_segment(
        &self,
        database_id: &str,
        segment: &SegmentAppend,
        entries: &[&SignedEntry],
    ) -> Result<(u64, u64), String> {
        let request = LogAppendRequest::segment(database_id.to_owned(), segment.clone());
        let answer = self.call::<LogAppend>(&request).await?;
        let (stored, duplicate) = append_counts(answer, entries.len() as u64);
        if let Some(usage) = &self.usage {
            usage.entries_accepted(database_id, entries, stored.saturating_add(duplicate), true)?;
        }
        Ok((stored, duplicate))
    }

    /// Count `entries`, which the relay holds already in a segment, in the usage ledger. An entry
    /// the ledger counted already is skipped, so a repeat changes nothing.
    pub(in crate::log_sync) fn segment_entries_held(
        &self,
        database_id: &str,
        entries: &[&SignedEntry],
    ) -> Result<(), String> {
        match &self.usage {
            Some(usage) => usage.entries_accepted(database_id, entries, entries.len() as u64, true),
            None => Ok(()),
        }
    }

    /// [`Self::read`], with the digests of the log segments that held the entries. A segment
    /// is fetched and opened here, so a caller sees the entries it holds as if they were stored
    /// one by one. On a read of one device from `from_seq`, the entries of the first segment
    /// below `from_seq` are left out.
    pub(in crate::log_sync) async fn read_page(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: Option<&str>,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Page, String> {
        let request = match cursor {
            Some(cursor) => LogReadRequest {
                database_id: database_id.to_owned(),
                cursor: Some(cursor.to_owned()),
                limit: Some(limit),
                ..LogReadRequest::default()
            },
            None => LogReadRequest {
                database_id: database_id.to_owned(),
                writer_key: Some(writer_key.to_owned()),
                device_id: device_id.map(str::to_owned),
                from_seq: Some(from_seq),
                cursor: None,
                limit: Some(limit),
            },
        };
        let LogReadAnswer {
            entries: items,
            cursor: next,
        } = self
            .call_tracked::<LogRead>(&request)
            .await
            .map(|(answer, _)| answer)
            .map_err(|error| {
                if error.status == Some(410) {
                    format!("{BEHIND_THE_TRIM}: {}", error.message)
                } else {
                    error.message
                }
            })?;
        const BAD: &str = "relay log.read returned invalid entries";
        let skip_below = (cursor.is_none() && device_id.is_some()).then_some(from_seq);
        // The segments of the page are fetched a few at a time, and opened in their place.
        let opened: Vec<Option<Vec<SignedEntry>>> =
            futures::stream::iter(items.iter().map(|item| async move {
                match item {
                    ReadItem::Segment { segment } => self.open_segment(segment).await.map(Some),
                    ReadItem::Entry(_) => Ok(None),
                }
            }))
            .buffered(SEGMENT_FETCHES)
            .try_collect()
            .await?;
        let mut page = Page {
            entries: Vec::with_capacity(items.len()),
            cursor: next,
            in_segment: Vec::with_capacity(items.len()),
            segments: Vec::new(),
        };
        for (index, (item, opened)) in items.into_iter().zip(opened).enumerate() {
            match item {
                ReadItem::Entry(value) => {
                    page.entries
                        .push(serde_json::from_value(value).map_err(|_| BAD.to_owned())?);
                    page.in_segment.push(false);
                }
                ReadItem::Segment { segment } => {
                    let entries = opened.unwrap_or_default();
                    let from = if index == 0 {
                        skip_below.unwrap_or(0)
                    } else {
                        0
                    };
                    for entry in entries {
                        if entry.header().writer_seq >= from {
                            page.entries.push(entry);
                            page.in_segment.push(true);
                        }
                    }
                    page.segments.push(segment.sha256);
                }
            }
        }
        Ok(page)
    }

    /// The entries of a log segment, after a check that the blob holds exactly the entries the
    /// relay named: that stream, `first_seq` through `last_seq` in order, the last with hash
    /// `last_hash`. The signatures and the chain are the caller's to check, as for any entry.
    async fn open_segment(&self, segment: &SegmentRef) -> Result<Vec<SignedEntry>, String> {
        let bytes = self
            .fetch_bytes(
                &segment.url,
                &segment.headers,
                &segment.sha256,
                segment.bytes,
            )
            .await?;
        let bad = |what: &str| format!("log segment {}: {what}", segment.sha256);
        if segment.last_seq < segment.first_seq || bytes.last() != Some(&b'\n') {
            return Err(bad("not a list of entries"));
        }
        let mut entries = Vec::new();
        for (offset, line) in bytes[..bytes.len() - 1]
            .split(|byte| *byte == b'\n')
            .enumerate()
        {
            let entry: SignedEntry =
                serde_json::from_slice(line).map_err(|_| bad("holds an invalid entry"))?;
            let header = entry.header();
            if header.node_key.to_hex() != segment.node_key
                || header.device_id.to_hex() != segment.device_id
                || Some(header.writer_seq) != segment.first_seq.checked_add(offset as u64)
            {
                return Err(bad("holds another entry than the relay named"));
            }
            entries.push(entry);
        }
        let last = entries.last().ok_or_else(|| bad("is empty"))?;
        if last.header().writer_seq != segment.last_seq
            || mutation_log::entry_hash(last)
                .map(|hash| hash.to_hex())
                .ok()
                != Some(segment.last_hash.clone())
        {
            return Err(bad("does not end at the entry the relay named"));
        }
        Ok(entries)
    }
}
