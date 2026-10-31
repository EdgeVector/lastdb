//! The repair of the retention rows of conflict records (offline recount).
//!
//! The survey (`time_queue/survey.rs`) finds a record that no queue entry names,
//! and a bucket that holds entries and has no directory row. The repair writes
//! the missing rows again through the planner (`commit_tip_changes`, rows only),
//! so it adds no writer to the writer lint.
//!
//! - A record without a queue entry gets its queue entry back, and the directory
//!   row of its bucket. The slot directory entry of the record, when it still has
//!   one, holds the whole original entry: the queue key (so the expiry), the slot
//!   key and the creation time. The repair finds that entry with one walk of the
//!   slot directory (`slot_entries_of`) and puts the queue entry back at its own
//!   key, with the slot directory entry named, so the release of the record
//!   deletes the slot directory entry too. A record that is past its window is
//!   due, and the janitor releases it.
//!   A record with no slot directory entry either gets a queue entry that expires
//!   one window from now (`TimeQueue::rows`): its original expiry is not known, so
//!   it waits one more window and does not wait for ever. That entry has no slot
//!   directory entry of its own (`dir` is empty), because the slot of a record
//!   cannot be derived from the record without the key codec of its molecule,
//!   which a stopped home may not have loaded. The window is the one that the
//!   node recorded (`window.rs`), not the environment of the recount.
//! - A bucket that holds entries and has no row gets the row back
//!   (`TimeQueue::register_buckets`).
//!
//! A repair is safe to run again: a record that has its entry is not a finding.

use super::entry::{RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::time_queue::TimeQueue;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::{HashMap, HashSet};

/// Records whose rows one repair batch writes, at most.
const REPAIR_BATCH_RECORDS: usize = 100;
/// Rows of the slot directory that one read asks for.
const SLOT_WALK_ROWS: usize = 1_000;

/// The queue layout and the window that a repair writes with. The recount takes
/// them from the record of the node when the node recorded them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RepairWindow {
    pub(crate) queue: TimeQueue,
    pub(crate) window_nanos: u64,
}

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
    let rest = lastdb_model::kind_partition::rest_of(history, "history")?;
    let mut parts = rest.rsplitn(3, ':');
    let digest = parts.next()?;
    if parts.next()? != "conflict" {
        return None;
    }
    let molecule = parts.next()?.split(':').next()?;
    let stamp = u64::from_str_radix(digest.get(..16)?, 16).ok()?;
    Some(build_storage_key(
        scope,
        &lastdb_model::kind_partition::anchored("conflict", &format!("{molecule}:{stamp:020}")),
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
        window: &RepairWindow,
    ) -> Result<Repaired, SchemaError> {
        let queue = window.queue;
        let mut repaired = Repaired::default();
        let wanted: HashSet<&str> = records.iter().map(String::as_str).collect();
        let found = if wanted.is_empty() {
            HashMap::new()
        } else {
            self.slot_entries_of(scope, &wanted).await?
        };
        for chunk in records.chunks(REPAIR_BATCH_RECORDS) {
            let mut rows = Vec::with_capacity(chunk.len() * 2);
            for history in chunk {
                rows.extend(self.entry_rows(scope, history, found.get(history), window)?);
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

    /// The rows that put the queue entry of one record back. `found` is the slot
    /// directory entry of the record, when the walk met it: the entry then goes
    /// back at its own key, with the same slot, directory entry and creation time.
    fn entry_rows(
        &self,
        scope: Option<&str>,
        history: &str,
        found: Option<&RetentionEntry>,
        window: &RepairWindow,
    ) -> Result<Vec<(String, serde_json::Value)>, SchemaError> {
        let queue = window.queue;
        let id = keys::record_id(history);
        let serialize = |entry: &RetentionEntry| {
            serde_json::to_value(entry).map_err(|error| {
                SchemaError::InvalidData(format!("serialize retention entry: {error}"))
            })
        };
        if let Some(original) = found {
            let entry = RetentionEntry {
                conflict: original
                    .conflict
                    .clone()
                    .or_else(|| conflict_key_of(scope, history)),
                ..original.clone()
            };
            if let Some(rows) = queue.rows_for_key(&entry.queue, &serialize(&entry)?) {
                return Ok(rows);
            }
        }
        let created = self.retention_state().clock_nanos();
        let expires = created.saturating_add(window.window_nanos);
        let entry = RetentionEntry {
            version: ENTRY_VERSION,
            history: history.to_string(),
            conflict: conflict_key_of(scope, history),
            dir: String::new(),
            queue: queue.entry_key(expires, &id),
            slot: String::new(),
            created,
        };
        Ok(queue.rows(expires, &id, &serialize(&entry)?))
    }

    /// The slot directory entries of the records in `wanted` (history keys). One
    /// bounded walk of the slot directory of the scope, which keeps only the
    /// entries it was asked for. The directory is not read by a request.
    async fn slot_entries_of(
        &self,
        scope: Option<&str>,
        wanted: &HashSet<&str>,
    ) -> Result<HashMap<String, RetentionEntry>, SchemaError> {
        let (start, end) = keys::prefix_range(&build_storage_key(scope, "crs:v1:"));
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SLOT_WALK_ROWS);
        let mut found = HashMap::new();
        while !walk.done() && found.len() < wanted.len() {
            for (_, value) in self.walk_step(&mut walk).await?.rows {
                let Ok(entry) = serde_json::from_slice::<RetentionEntry>(&value) else {
                    continue;
                };
                if wanted.contains(entry.history.as_str()) {
                    found.insert(entry.history.clone(), entry);
                }
            }
        }
        Ok(found)
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
