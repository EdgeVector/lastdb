//! The release of the stored rows of a dropped schema.
//!
//! A drop removes the schema record from the catalog. Before that, it releases
//! what the schema stored in its record scope (`record_scope`, the personal
//! scope for a catalog schema): the records and their tombstones
//! (`rk:{S}:`), the field history entries (`rh:{S}:`), and the entries of each
//! declared alternate key (`ak:{M}:`). Each family is one exact key prefix of
//! this schema, so the release reads only the keyed ranges of this schema and
//! never a row of another schema.
//!
//! The release goes in bounded batches. A walk step reads at most
//! [`RELEASE_ROWS`] rows outside the write gate. One native write scope then
//! reads those keys again and deletes the rows that are still there, with
//! their blob counter changes, in one batch. A row that a writer removed
//! between the two reads is not released twice. A drop that stops between two
//! batches (an error, a timeout) leaves the schema installed, and the next drop
//! goes on: it passes the deleted keys and releases the rest.
//!
//! - A record lowers the blobs that it names (`BlobDeltas::scope_rows_dropped`),
//!   and its conflict rows, their queue entries and its conflict marker go in
//!   the same batch, as a purge does (`purged_record_conflicts`).
//! - A field history entry goes with its queue entry, its blob references and
//!   the entry count of its record (`release_dropped_history`).
//! - An alternate entry holds no blob reference.
//!
//! Two kinds of rows are not in the record scope. The rows of a database
//! binding live in the scope of its instance, and the catalog drop releases
//! that scope (`drop_prefixed_rows_for_instance`). The records that another
//! user sent live in a `from:` scope that belongs to the share.
//!
//! A write that lands behind the walk while the drop runs is not released.

use super::blob_counter::BlobDeltas;
use super::{record_scope, AtomStore, RangeWalk};
use crate::atom::molecule_uuid::{alternate_molecule_uuid, ALTERNATE_ENTRY_PREFIX};
use crate::records::atom_history::RECORD_HISTORY_PREFIX;
use crate::schema::types::field::build_storage_key;
use crate::schema::{Schema, SchemaError};
use crate::storage::KvMutation;
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

impl AtomStore {
    /// Release every record, field history entry and alternate entry that
    /// `schema` stored in its record scope, in bounded batches. The caller
    /// holds no write scope: each batch opens its own.
    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let scope = record_scope(schema);
        let space = record_space_uuid(&schema.name);
        let mut out = SchemaRelease::default();
        let history = build_storage_key(
            scope.as_deref(),
            &format!("{RECORD_HISTORY_PREFIX}{space}:"),
        );
        out.history = self.release_family(&history, Family::History).await?;
        let records = build_storage_key(scope.as_deref(), &format!("{RECORD_PREFIX}{space}:"));
        out.records = self.release_family(&records, Family::Records).await?;
        for name in schema.alternate_keys.keys() {
            let molecule = alternate_molecule_uuid(schema, name);
            let alternates = build_storage_key(
                scope.as_deref(),
                &format!("{ALTERNATE_ENTRY_PREFIX}{molecule}:"),
            );
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
            return self.release_dropped_history(rows).await;
        }
        let count = rows.len() as u64;
        let deletes: Vec<KvMutation> = rows
            .iter()
            .map(|(key, _)| KvMutation::delete(key.as_bytes().to_vec()))
            .collect();
        let batch = match family {
            Family::History | Family::Alternates => deletes,
            Family::Records => {
                let mut blobs = BlobDeltas::default();
                let unreadable = blobs.scope_rows_dropped(
                    rows.iter()
                        .map(|(key, value)| (key.as_bytes(), value.as_slice())),
                );
                if unreadable > 0 {
                    tracing::warn!(
                        target: "fold_node::database",
                        unreadable,
                        "records of a dropped schema do not decode; their blob counters stay \
                         too high until a recount"
                    );
                }
                let records: Vec<&str> = rows.iter().map(|(key, _)| key.as_str()).collect();
                let mut batch = self.purged_record_conflicts(&records).await?;
                batch.extend(deletes);
                batch.extend(self.blob_counter_mutations(blobs).await?);
                batch
            }
        };
        self.raw().inner().batch_mutate(batch).await?;
        Ok(count)
    }
}
