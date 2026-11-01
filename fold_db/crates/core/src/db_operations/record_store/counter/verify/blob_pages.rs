//! The page reads of the blob checks: counters, queue entries, and the rows of
//! the blobs that hold references. Every read of a page runs in one read scope,
//! so all of them see one committed root.

use super::blobs::{BlobEntry, BlobsPage, QueueEntry, QueuePage, UnlistedBucket};
use super::pages::Request;
use crate::db_operations::record_store::blob_counter::{blob_counter_key_of_id, BlobRead};
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// The counter id and the counter key of a blob counter row, when the key has the
/// current shape.
fn id_of_counter_key(key: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(key).ok()?;
    let id = text.strip_prefix("bref:v3:c:")?.strip_suffix("\0c")?;
    (id.len() == 64).then(|| id.to_string())
}

impl RecordStore {
    async fn read_blob_counter_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.raw()
            .inner()
            .get_many(keys)
            .await
            .map_err(SchemaError::from)
    }

    /// One page of blob counters, and whether the queue holds the entry of each.
    pub(super) async fn read_blobs_page(
        &self,
        (start, end, asked): Request,
    ) -> Result<BlobsPage, SchemaError> {
        let queue = self.blob_events.queue();
        self.atom_read_scope(async {
            let scan = self.read_verify_page(&start, &end, asked).await?;
            let (rows, resume) = (scan.rows, scan.resume);
            let counters: Vec<BlobRead> = rows
                .iter()
                .map(|(_, value)| BlobRead::parse(Some(value)))
                .collect();
            let queue_keys: Vec<Vec<u8>> = counters
                .iter()
                .filter_map(|counter| match counter {
                    BlobRead::Present(counter) => counter
                        .zero_since
                        .map(|at| queue.entry_key(at, &counter.blob).into_bytes()),
                    _ => None,
                })
                .collect();
            let found = if queue_keys.is_empty() {
                Vec::new()
            } else {
                self.raw()
                    .inner()
                    .exists_many(queue_keys)
                    .await
                    .map_err(SchemaError::from)?
            };
            let mut found = found.into_iter();
            // The rows of the blobs that hold references, read in the same scope.
            let referenced: Vec<String> = counters
                .iter()
                .filter_map(BlobRead::counter)
                .filter(|counter| counter.refs > 0)
                .map(|counter| counter.blob.clone())
                .collect();
            let missing: std::collections::HashSet<&str> = if referenced.is_empty() {
                std::collections::HashSet::new()
            } else {
                self.blob_rows_exist(&referenced)
                    .await?
                    .map(|rows| {
                        referenced
                            .iter()
                            .zip(rows)
                            .filter(|(_, row)| !*row)
                            .map(|(blob, _)| blob.as_str())
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let mut entries = Vec::with_capacity(rows.len());
            for ((key, _), counter) in rows.iter().zip(counters) {
                let queued = match &counter {
                    BlobRead::Present(c) if c.zero_since.is_some() => found.next().unwrap_or(false),
                    _ => false,
                };
                let row_missing = counter
                    .counter()
                    .is_some_and(|counter| missing.contains(counter.blob.as_str()));
                entries.push(BlobEntry {
                    id: id_of_counter_key(key).unwrap_or_default(),
                    counter,
                    queued,
                    row_missing,
                });
            }
            Ok(BlobsPage {
                rows: rows.len(),
                resume,
                entries,
            })
        })
        .await
    }

    /// One page of queue entries, and the counter each names.
    pub(super) async fn read_queue_page(
        &self,
        (start, end, asked): Request,
    ) -> Result<QueuePage, SchemaError> {
        let queue = self.blob_events.queue();
        self.atom_read_scope(async {
            let scan = self.read_verify_page(&start, &end, asked).await?;
            let (rows, resume) = (scan.rows, scan.resume);
            // The buckets of the page against the directory, at the same root.
            let mut buckets: BTreeMap<u64, (u64, String)> = BTreeMap::new();
            for (key, _) in &rows {
                let key = String::from_utf8_lossy(key).into_owned();
                if let Some(bucket) = queue.layout().entry_bucket(&key) {
                    let held = buckets.entry(bucket).or_insert((0, key));
                    held.0 += 1;
                }
            }
            let directory: Vec<Vec<u8>> = buckets
                .keys()
                .map(|bucket| queue.layout().directory_key(*bucket).into_bytes())
                .collect();
            let listed = if directory.is_empty() {
                Vec::new()
            } else {
                self.raw()
                    .inner()
                    .exists_many(directory)
                    .await
                    .map_err(SchemaError::from)?
            };
            let unlisted: Vec<UnlistedBucket> = buckets
                .into_iter()
                .zip(listed)
                .filter(|(_, listed)| !*listed)
                .map(|((bucket, (entries, sample)), _)| UnlistedBucket {
                    bucket,
                    entries,
                    sample,
                })
                .collect();
            let parsed: Vec<(u64, String)> = rows
                .iter()
                .filter_map(|(key, _)| queue.parse_entry(key))
                .collect();
            let keys = parsed
                .iter()
                .map(|(_, id)| blob_counter_key_of_id(id).into_bytes())
                .collect();
            let bytes = self.read_blob_counter_bytes(keys).await?;
            let entries = parsed
                .into_iter()
                .zip(bytes)
                .map(|((zero_since, id), bytes)| QueueEntry {
                    zero_since,
                    id,
                    counter: BlobRead::parse(bytes.as_deref()),
                })
                .collect();
            Ok(QueuePage {
                rows: rows.len(),
                resume,
                entries,
                unlisted,
            })
        })
        .await
    }
}
