//! Numbered list of changed record keys, staged with the records it names.
//!
//! Every record write already takes the next number of the local author
//! stream. The list reuses that number: one entry row per changed record, in
//! the same staged batch as the stream row, and no row of its own to count.
//! A reader keeps the last number it handled and asks for the entries after
//! it. The list holds keys only; a reader gets values with a read by key.

use super::{decode_author, read_err, Metadata, LOCAL_AUTHOR_KEY};
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::hex::sha256_hex;
use lastdb_model::portable_mutation::{Stamp, StreamId};
use serde::{Deserialize, Serialize};

/// Newest entries kept. The write that adds entry N deletes entry N - KEEP.
pub const CHANGES_KEEP: u64 = 250_000;
/// Most entries one read examines.
pub const CHANGES_PAGE: usize = 500;

/// One record a write changes: its schema, its key, and whether the write deletes it.
pub(crate) type ChangedKey<'a> = (&'a str, &'a KeyValue, bool);

/// One changed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Rises by one per changed record, in commit order.
    pub seq: u64,
    pub schema: String,
    pub hash: Option<String>,
    pub range: Option<String>,
    pub deleted: bool,
}

/// One read of the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePage {
    /// Changes when a restore replaces this database. A reader that saved a
    /// different value rebuilds.
    pub id: String,
    /// Newest number in the list. Zero means no entry.
    pub head: u64,
    /// Last number this read examined. The reader saves it as its position.
    pub through: u64,
    /// The position is older than the oldest kept entry, or newer than `head`.
    pub gap: bool,
    pub changes: Vec<Change>,
}

/// The stored row. The key carries the number.
#[derive(Serialize, Deserialize)]
struct Row {
    s: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    h: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    r: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    d: bool,
}

// A restore starts a new stream whose numbers start again, so the stream is
// part of the key. Entries of an earlier stream are never read.
fn entry_key(stream: &StreamId, seq: u64) -> String {
    format!("changes:{stream}:{seq:020}")
}

/// Add one entry per changed record, numbered by its stamp, and the trim of
/// the entry that leaves the kept window. The caller stages `rows` in the
/// records' native write scope with the stream row.
///
/// The trim is a delete without an existence read ([`KvMutation::Trim`]).
/// Every number of the stream got its entry in the commit that took it, so
/// the trimmed entry is there, except on a stream that began before this list
/// (its first numbers have no entry). There the delete leaves one tombstone,
/// which no read of the list returns.
pub(super) fn append_rows(
    stamps: &[Stamp],
    records: &[ChangedKey<'_>],
    rows: &mut Vec<KvMutation>,
) -> StorageResult<()> {
    for (stamp, &(schema, key, deleted)) in stamps.iter().zip(records) {
        let (stream, seq) = (&stamp.position.source_stream, stamp.position.sequence);
        let row = Row {
            s: schema.to_owned(),
            h: key.hash.clone(),
            r: key.range.clone(),
            d: deleted,
        };
        let bytes = serde_json::to_vec(&row)
            .map_err(|e| StorageError::SerializationError(format!("encode change: {e}")))?;
        rows.push(KvMutation::put(entry_key(stream, seq), bytes));
        if seq > CHANGES_KEEP {
            rows.push(KvMutation::trim(entry_key(stream, seq - CHANGES_KEEP)));
        }
    }
    Ok(())
}

impl Metadata {
    /// Read the entries after `after`. Call inside one read scope so the head
    /// and the entries come from the same committed view.
    pub(crate) async fn changes_after(
        &self,
        after: u64,
        limit: usize,
        schemas: &[String],
    ) -> Result<ChangePage, SchemaError> {
        let author = self
            .store
            .get(LOCAL_AUTHOR_KEY.as_bytes())
            .await
            .map_err(read_err)?;
        let author = decode_author(author.as_deref()).map_err(read_err)?;
        let (stream, head) = (author.stream, author.progress.applied_through);
        let oldest = head.saturating_sub(CHANGES_KEEP).saturating_add(1);
        let gap = after > head || after.saturating_add(1) < oldest;
        let mut page = ChangePage {
            id: sha256_hex(format!("changes:{stream}")),
            head,
            through: after,
            gap,
            changes: Vec::new(),
        };
        let limit = limit.min(CHANGES_PAGE);
        if gap || limit == 0 || after == head {
            return Ok(page);
        }
        let prefix = format!("changes:{stream}:");
        let rows = self
            .store
            .scan_items_in_range_paged::<Row>(
                &entry_key(&stream, after.saturating_add(1)),
                &format!("changes:{stream};"),
                limit,
            )
            .await
            .map_err(read_err)?;
        let full = rows.len() == limit;
        for (key, row) in rows {
            let seq = key
                .strip_prefix(&prefix)
                .and_then(|digits| digits.parse().ok())
                .ok_or_else(|| read_err("a changes entry has an invalid key"))?;
            page.through = seq;
            if schemas.is_empty() || schemas.contains(&row.s) {
                page.changes.push(Change {
                    seq,
                    schema: row.s,
                    hash: row.h,
                    range: row.r,
                    deleted: row.d,
                });
            }
        }
        // A short page reached the end of the list.
        if !full {
            page.through = head;
        }
        Ok(page)
    }
}
