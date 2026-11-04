//! The release of the stored rows of a dropped schema.
//!
//! A drop removes the schema record from the catalog. Before that, it releases
//! what the schema stored: the records and their tombstones
//! (`rk:{S}:`), the field history entries (`rh:{S}:`), and the entries of each
//! declared alternate key (`ak:{F}:`). Each family is one exact key prefix of
//! this schema, so the release reads only the keyed ranges of this schema and
//! never a row of another schema.
//!
//! The release goes in bounded batches. A walk step reads at most
//! [`RELEASE_ROWS`] rows outside the write gate. One native write scope then
//! reads those keys again and deletes the rows that are still there, with
//! the files of the records, in one batch. A row that a writer removed
//! between the two reads is not released twice. A drop that stops between two
//! batches (an error, a timeout) leaves the schema installed, and the next drop
//! goes on: it passes the deleted keys and releases the rest.
//!
//! - A record's file path (`fb:{S}:`, in `cas_blobs`) goes with the record.
//!   The path comes from the record key (`record_files::dropped_file_paths`).
//!   A record row that does not decode fails the drop.
//! - A field history entry goes with its queue entry and the entry count of
//!   its record (`release_dropped_history`).
//! - An alternate entry holds no file.
//!
//! No write lands behind the walk. The drop stores the state `Blocked` before
//! the first batch (`DbOperations::drop_schema`), and every write reads the
//! state inside the native write gate, so a write during the drop is refused.
//!
//! The alternate entries are the entries of the current alternate revision
//! (`alternate_field_key_id`). The schema keeps no history of revisions, and
//! needs none: an installed schema cannot change its alternate keys. A refresh
//! that changes them is refused (`schema/core/load/rules.rs`), and so is an
//! owned declaration (`exec/schema_declare/owned.rs`). So the current revision
//! is the only revision under which this install wrote entries. A new
//! alternate-key change command must add a durable list of the revisions that
//! the schema had, and this release must then read the `ak:{F}:` prefix of each.

use super::record_files::dropped_file_paths;
use super::{RangeWalk, RecordStore};
use crate::records::field_history::RECORD_HISTORY_FAMILY;
use crate::schema::{Schema, SchemaError};
use crate::storage::KvMutation;
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};

/// Rows that one release batch reads and deletes, at most.
const RELEASE_ROWS: usize = 256;

/// The rows that the drop of one schema released.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) history: u64,
    pub(crate) alternates: u64,
}

/// One family of rows of a schema.
#[derive(Debug, Clone, Copy)]
enum Family {
    Records,
    History,
    Alternates,
}

impl RecordStore {
    /// Release every record, field history entry and alternate entry that
    /// `schema` stored, in bounded batches. The caller
    /// holds no write scope: each batch opens its own.
    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let mut out = SchemaRelease::default();
        let history = format!("{RECORD_HISTORY_FAMILY}:{space}:");
        out.history = self.release_family(&history, Family::History).await?;
        let records = format!("{RECORD_PREFIX}{space}:");
        out.records = self.release_family(&records, Family::Records).await?;
        for name in schema.alternate_keys.keys() {
            let field_key = alternate_field_key_id(schema, name);
            let alternates = format!("{ALTERNATE_ENTRY_PREFIX}{field_key}:");
            out.alternates += self.release_family(&alternates, Family::Alternates).await?;
        }
        Ok(out)
    }

    /// Release every row under one exact prefix, one walk step at a time.
    async fn release_family(&self, prefix: &str, family: Family) -> Result<u64, SchemaError> {
        let (start, end) = exact_prefix_bounds(prefix);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), RELEASE_ROWS);
        let mut released = 0;
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            if step.rows.is_empty() {
                continue;
            }
            let keys: Vec<Vec<u8>> = step.rows.into_iter().map(|(key, _)| key).collect();
            released += self
                .in_write_scope(self.release_batch(keys, family))
                .await?;
        }
        Ok(released)
    }

    /// One batch: read `keys` again in the write scope, and delete the rows
    /// that are still there with the changes that each family needs.
    async fn release_batch(&self, keys: Vec<Vec<u8>>, family: Family) -> Result<u64, SchemaError> {
        let stored = self.raw().inner().get_many(keys.clone()).await?;
        if stored.len() != keys.len() {
            return Err(SchemaError::InvalidData(
                "schema release read batch is incomplete".into(),
            ));
        }
        let rows: Vec<(String, Vec<u8>)> = keys
            .into_iter()
            .zip(stored)
            .filter_map(|(key, value)| Some((String::from_utf8(key).ok()?, value?)))
            .collect();
        if rows.is_empty() {
            return Ok(0);
        }
        if matches!(family, Family::History) {
            return self.release_dropped_history(&rows).await;
        }
        let count = rows.len() as u64;
        let deletes: Vec<KvMutation> = rows
            .iter()
            .map(|(key, _)| KvMutation::delete(key.as_bytes().to_vec()))
            .collect();
        let mut files = Vec::new();
        if matches!(family, Family::Records) {
            files = dropped_file_paths(
                rows.iter()
                    .map(|(key, value)| (key.as_bytes(), value.as_slice())),
            )?;
        }
        self.raw().inner().batch_mutate(deletes).await?;
        self.stage_file_rows(files.into_iter().map(KvMutation::delete).collect())
            .await?;
        Ok(count)
    }
}
