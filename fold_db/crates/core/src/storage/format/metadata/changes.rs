//! Numbered list of changed record keys, staged with the records it names.
//!
//! Every record write appends one entry in its own native transaction, with
//! the same read and the same staged batch as its local stamp. A
//! reader keeps the last number it handled and asks for the entries after it.
//! The list holds keys only; a reader gets values with a read by key.

use super::{decode_author, read_err, Metadata, LOCAL_AUTHOR_KEY};
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::{error::StorageResult, StorageError};
use lastdb_model::hex::sha256_hex;
use serde::{Deserialize, Serialize};

/// Newest entries kept. The write that adds entry N deletes entry N - KEEP.
pub const CHANGES_KEEP: u64 = 250_000;
/// Most entries one read examines.
pub const CHANGES_PAGE: usize = 500;

// `-` sorts before `:`, so the head row stays outside the entry range.
pub(super) const HEAD_KEY: &str = "changes-head";
const ENTRY_PREFIX: &str = "changes:";
const ENTRY_END: &str = "changes;";

/// One changed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// Starts at 1 and rises by one per changed record, in commit order.
    pub seq: u64,
    pub schema: String,
    pub hash: Option<String>,
    pub range: Option<String>,
    pub deleted: bool,
}

/// One read of the list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

fn entry_key(seq: u64) -> String {
    format!("{ENTRY_PREFIX}{seq:020}")
}

/// One record a write changes: its schema, its key, and whether the write deletes it.
pub(crate) type ChangedKey<'a> = (&'a str, &'a KeyValue, bool);

pub(super) fn parse_head(bytes: Option<&[u8]>) -> StorageResult<u64> {
    let Some(bytes) = bytes else {
        return Ok(0);
    };
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| StorageError::invalid("the changes head is not a number"))
}

/// Add one entry per changed record after `head`, the trim of the entry that
/// leaves the kept window, and the new head. The caller stages `rows` in the
/// records' native write scope, which holds the writer gate, so numbers
/// follow commit order.
pub(super) fn append_rows(
    mut head: u64,
    records: &[ChangedKey<'_>],
    rows: &mut Vec<(String, Option<Vec<u8>>)>,
) -> StorageResult<()> {
    for &(schema, key, deleted) in records {
        head = head
            .checked_add(1)
            .ok_or_else(|| StorageError::invalid("the changes list is out of numbers"))?;
        let change = Change {
            seq: head,
            schema: schema.to_owned(),
            hash: key.hash.clone(),
            range: key.range.clone(),
            deleted,
        };
        let bytes = serde_json::to_vec(&change)
            .map_err(|e| StorageError::SerializationError(format!("encode change: {e}")))?;
        rows.push((entry_key(head), Some(bytes)));
        if head > CHANGES_KEEP {
            rows.push((entry_key(head - CHANGES_KEEP), None));
        }
    }
    rows.push((HEAD_KEY.to_owned(), Some(head.to_string().into_bytes())));
    Ok(())
}

impl Metadata {
    async fn changes_head(&self) -> Result<u64, SchemaError> {
        let bytes = self
            .store
            .get(HEAD_KEY.as_bytes())
            .await
            .map_err(read_err)?;
        parse_head(bytes.as_deref()).map_err(read_err)
    }

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
        // A restore starts a new author stream, so the id changes with it.
        let stream = decode_author(author.as_deref()).map_err(read_err)?.stream;
        let id = sha256_hex(format!("changes:{stream}"));
        let head = self.changes_head().await?;
        let oldest = head.saturating_sub(CHANGES_KEEP).saturating_add(1);
        let gap = after > head || after.saturating_add(1) < oldest;
        let mut page = ChangePage {
            id,
            head,
            through: after,
            gap,
            changes: Vec::new(),
        };
        let limit = limit.min(CHANGES_PAGE);
        if gap || limit == 0 || after == head {
            return Ok(page);
        }
        let rows = self
            .store
            .scan_items_in_range_paged::<Change>(
                &entry_key(after.saturating_add(1)),
                ENTRY_END,
                limit,
            )
            .await
            .map_err(read_err)?;
        // A short page reached the end of the list.
        page.through = match rows.last() {
            Some((_, last)) if rows.len() == limit => last.seq,
            _ => head,
        };
        page.changes = rows
            .into_iter()
            .map(|(_, change)| change)
            .filter(|change| schemas.is_empty() || schemas.contains(&change.schema))
            .collect();
        Ok(page)
    }
}
