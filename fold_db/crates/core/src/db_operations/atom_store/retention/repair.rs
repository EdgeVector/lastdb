//! The repair of the retention rows of conflict records (offline recount).
//!
//! The survey (`time_queue/survey.rs`) finds a record that no queue entry names,
//! and a bucket that holds entries and has no directory row. The repair writes
//! the missing rows again through the planner (`commit_tip_changes`, rows only),
//! so it adds no writer to the writer lint.
//!
//! - A record without a queue entry gets a queue entry that expires one window
//!   from now, and the directory row of its bucket (`TimeQueue::rows`). The
//!   original expiry is not known, so a record that was past its window waits one
//!   more window: it does not wait for ever. The entry has no slot directory
//!   entry of its own (`dir` is empty), because the slot of a record cannot be
//!   derived from the record without the key codec of its molecule, which a
//!   stopped home may not have loaded. A slot directory entry that the record
//!   still has stays, and a purge of its key finds the record through it.
//! - A bucket that holds entries and has no row gets the row back
//!   (`TimeQueue::register_buckets`).
//!
//! A repair is safe to run again: a record that has its entry is not a finding.

use super::entry::{RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

/// Records whose rows one repair batch writes, at most.
const REPAIR_BATCH_RECORDS: usize = 100;

/// What a repair wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Repaired {
    /// Records that got a queue entry and a bucket row.
    pub(crate) records: u64,
    /// Buckets that got their directory row back.
    pub(crate) buckets: u64,
}

/// The conflict row of a record that the tip-conflict recorder wrote, from the
/// key of its history row: `history\0{molecule}:{ts}:conflict:{digest}` names
/// `conflict\0{molecule}:{stamp}` with the stamp the first 64 bits of the digest
/// in 20 digits. A record of another tag has no conflict row.
fn conflict_key_of(scope: Option<&str>, history: &str) -> Option<String> {
    let rest = crate::kind_partition::rest_of(history, "history")?;
    let mut parts = rest.rsplitn(3, ':');
    let digest = parts.next()?;
    if parts.next()? != "conflict" {
        return None;
    }
    let molecule = parts.next()?.split(':').next()?;
    let stamp = u64::from_str_radix(digest.get(..16)?, 16).ok()?;
    Some(build_storage_key(
        scope,
        &crate::kind_partition::anchored("conflict", &format!("{molecule}:{stamp:020}")),
    ))
}

impl AtomStore {
    /// Write a queue entry and its bucket row for each history key in
    /// `records`, and the directory row of each bucket in `buckets`. A stopped
    /// home, or a holder of a write scope: each batch is atomic.
    pub(crate) async fn repair_record_retention(
        &self,
        scope: Option<&str>,
        records: &[String],
        buckets: &[u64],
    ) -> Result<Repaired, SchemaError> {
        let queue = self.retention_state().queue();
        let mut repaired = Repaired::default();
        for chunk in records.chunks(REPAIR_BATCH_RECORDS) {
            let mut rows = Vec::with_capacity(chunk.len() * 2);
            for history in chunk {
                let id = keys::record_id(history);
                let created = self.retention_state().clock_nanos();
                let expires = created.saturating_add(self.retention_state().window_nanos());
                let entry = RetentionEntry {
                    version: ENTRY_VERSION,
                    history: history.clone(),
                    conflict: conflict_key_of(scope, history),
                    dir: String::new(),
                    queue: queue.entry_key(expires, &id),
                    slot: String::new(),
                    created,
                };
                let value = serde_json::to_value(&entry).map_err(|error| {
                    SchemaError::InvalidData(format!("serialize retention entry: {error}"))
                })?;
                rows.extend(queue.rows(expires, &id, &value));
            }
            self.write_repair_rows(rows).await?;
            repaired.records += chunk.len() as u64;
        }
        if !buckets.is_empty() {
            queue
                .register_buckets(self, buckets, self.retention_state().clock_nanos())
                .await?;
            repaired.buckets = buckets.len() as u64;
        }
        Ok(repaired)
    }

    /// One batch of the planner that writes only rows.
    async fn write_repair_rows(
        &self,
        rows: Vec<(String, serde_json::Value)>,
    ) -> Result<(), SchemaError> {
        self.in_background_scope(async {
            self.commit_tip_changes(TipChangeSet {
                rows,
                ..TipChangeSet::default()
            })
            .await
            .map(|_| ())
        })
        .await
    }
}
