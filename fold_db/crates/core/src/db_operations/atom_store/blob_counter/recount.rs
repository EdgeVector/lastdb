//! The blob step of the offline recount.
//!
//! `lastdb db recount` counts the references of each blob from the atom counters
//! and the tips of the scopes it recounted (`counter/recount/blobs.rs` does the
//! counting), and this module compares the count with the blob counters and
//! writes the difference. It runs on a **stopped home**, like the rest of the
//! recount, so it takes no write gate. Each batch is atomic, and a run that
//! stopped is safe to run again.
//!
//! The blob counters are global and the recount lists scopes. A recount that
//! missed a scope would count too low. So the default only **raises**: a counter
//! below the count rises, and a missing counter is made. Nothing lowers a
//! counter, and no blob starts a dwell. `exact` (`--blobs-exact`, with
//! `--all-scopes`) says that the listed scopes are every scope of the home. Then
//! a counter above the count falls to it, a `suspect` flag clears, and a blob that
//! nothing references starts its dwell, so the dwell pass reclaims it.
//!
//! After the counters, the queue is checked: an entry that no counter at zero
//! backs is deleted; a counter in its dwell with no entry gets its entry back; a
//! bucket whose entries stay and that has no directory row gets the row back. The
//! two repairs that write rows go through the planner (`recount_queue.rs`).

use super::{BlobCounter, BlobQueue, BlobRead};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use std::collections::{BTreeSet, HashMap};

use super::recount_report::BlobRecountReport;

/// Operations in one batch.
const BATCH_OPERATIONS: usize = 400;
/// Rows asked for in each read.
const PAGE_ROWS: usize = 1_000;
/// Blobs without a row that the report names.
const MISSING_SAMPLES: usize = 10;

/// What to write for one blob.
#[derive(Debug, PartialEq, Eq)]
struct Fix {
    counter: BlobCounter,
    unqueue: Option<u64>,
    enqueue: Option<u64>,
    kind: FixKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixKind {
    Create,
    Raise,
    Lower,
    ClearSuspect,
    CancelDwell,
    StartDwell,
}

/// Decide for one blob. `holders` is the number of atom counters and tips that
/// name it in the listed scopes. `None` means the counter is right.
fn decide(
    blob: &str,
    before: Option<&BlobCounter>,
    holders: u64,
    exact: bool,
    now: u64,
) -> Option<Fix> {
    let Some(counter) = before else {
        return (holders > 0).then(|| Fix {
            counter: BlobCounter::new(blob, holders, None),
            unqueue: None,
            enqueue: None,
            kind: FixKind::Create,
        });
    };
    if holders == 0 {
        let in_dwell = counter.refs == 0 && !counter.suspect && counter.zero_since.is_some();
        if !exact || in_dwell {
            return None;
        }
        return Some(Fix {
            counter: BlobCounter {
                refs: 0,
                suspect: false,
                zero_since: Some(now),
                ..counter.clone()
            },
            unqueue: counter.zero_since,
            enqueue: Some(now),
            kind: FixKind::StartDwell,
        });
    }
    let refs = if exact || counter.refs < holders {
        holders
    } else {
        counter.refs
    };
    let suspect = counter.suspect && !exact;
    if refs == counter.refs && suspect == counter.suspect && counter.zero_since.is_none() {
        return None;
    }
    let kind = if refs > counter.refs {
        FixKind::Raise
    } else if refs < counter.refs {
        FixKind::Lower
    } else if suspect != counter.suspect {
        FixKind::ClearSuspect
    } else {
        FixKind::CancelDwell
    };
    Some(Fix {
        counter: BlobCounter {
            refs,
            suspect,
            zero_since: None,
            ..counter.clone()
        },
        unqueue: counter.zero_since,
        enqueue: None,
        kind,
    })
}

fn count(report: &mut BlobRecountReport, fix: &Fix) {
    let slot = match fix.kind {
        FixKind::Create => &mut report.created,
        FixKind::Raise => &mut report.raised,
        FixKind::Lower => &mut report.lowered,
        FixKind::ClearSuspect => &mut report.suspect_cleared,
        FixKind::CancelDwell => &mut report.dwell_cancelled,
        FixKind::StartDwell => &mut report.dwell_started,
    };
    *slot += 1;
}

fn mutations(queue: &BlobQueue, fix: &Fix) -> Result<Vec<KvMutation>, SchemaError> {
    let blob = &fix.counter.blob;
    let mut out = Vec::with_capacity(4);
    if let Some(at) = fix.unqueue {
        out.push(KvMutation::delete(queue.entry_key(at, blob).into_bytes()));
    }
    out.push(super::commit::counter_put(&fix.counter)?);
    if let Some(at) = fix.enqueue {
        out.extend(queue.put_mutations(at, blob)?);
    }
    Ok(out)
}

impl AtomStore {
    /// Write the batch (when the recount executes) and start a new one.
    async fn flush_blob_batch(
        &self,
        pending: &mut Vec<KvMutation>,
        execute: bool,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        let batch = std::mem::take(pending);
        if batch.is_empty() {
            return Ok(());
        }
        report.operations += batch.len() as u64;
        report.batches += 1;
        if execute {
            self.raw()
                .inner()
                .batch_mutate(batch)
                .await
                .map_err(SchemaError::from)?;
        }
        Ok(())
    }

    /// Compare the blob counters with `holders` (blob reference to the number of
    /// atom counters and tips that name it) and write the difference. Offline
    /// only. See the module documentation.
    pub(crate) async fn recount_blob_counters(
        &self,
        mut holders: HashMap<String, u64>,
        exact: bool,
        execute: bool,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        report.ran = true;
        report.exact = exact;
        report.referenced_blobs = holders.len() as u64;
        let now = self.blob_clock_millis();
        let queue = self.blob_events.queue();
        let mut pending = Vec::new();
        let mut referenced: Vec<String> = Vec::new();
        // Counters in their dwell whose queue entry is missing: written again at
        // the end, through the planner (`recount_queue.rs`).
        let mut restore: Vec<(String, u64)> = Vec::new();
        let (start, end) = super::counter_range();
        let mut walk = RangeWalk::new(&start, &end, PAGE_ROWS);
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            if step.unsealed > 0 && execute {
                return Err(SchemaError::InvalidData(format!(
                    "{} read(s) dropped a blob counter with no at-rest envelope; the blob step writes nothing. \
                     Run `lastdb db reap-unsealed --collection atoms` (dry run first), then recount again",
                    step.unsealed
                )));
            }
            let mut in_dwell: Vec<(String, u64)> = Vec::new();
            for (_, value) in &step.rows {
                report.counters_read += 1;
                let Ok(counter) = serde_json::from_slice::<BlobCounter>(value) else {
                    report.unreadable_counters += 1;
                    continue;
                };
                let held = holders.remove(&counter.blob).unwrap_or(0);
                if held > 0 || counter.refs > 0 {
                    referenced.push(counter.blob.clone());
                }
                match decide(&counter.blob, Some(&counter), held, exact, now) {
                    None => {
                        report.unchanged += 1;
                        if let Some(at) = counter.zero_since {
                            in_dwell.push((counter.blob.clone(), at));
                        }
                    }
                    Some(fix) => {
                        count(report, &fix);
                        pending.extend(mutations(&queue, &fix)?);
                    }
                }
                if pending.len() >= BATCH_OPERATIONS {
                    self.flush_blob_batch(&mut pending, execute, report).await?;
                }
            }
            self.find_missing_entries(&queue, &in_dwell, &mut restore, report)
                .await?;
            self.note_missing_rows(&mut referenced, report).await?;
        }
        // Blobs that are named and have no counter.
        for (blob, held) in holders {
            if let Some(fix) = decide(&blob, None, held, exact, now) {
                count(report, &fix);
                pending.extend(mutations(&queue, &fix)?);
                referenced.push(blob);
            }
            if referenced.len() >= PAGE_ROWS {
                self.note_missing_rows(&mut referenced, report).await?;
            }
            if pending.len() >= BATCH_OPERATIONS {
                self.flush_blob_batch(&mut pending, execute, report).await?;
            }
        }
        self.flush_blob_batch(&mut pending, execute, report).await?;
        self.write_blob_queue_rows(&queue, &restore, execute)
            .await?;
        self.note_missing_rows(&mut referenced, report).await?;
        if report.rows_missing > 0 {
            report.notes.push(format!(
                "{} referenced blob(s) have no blob row: the atoms name a blob that is not in the store; the recount cannot restore the bytes",
                report.rows_missing
            ));
        }
        if report.unreadable_counters > 0 {
            report.notes.push(format!(
                "{} blob counter(s) do not decode and were not changed",
                report.unreadable_counters
            ));
        }
        self.recount_blob_queue(execute, report).await
    }

    /// Count the referenced blobs whose `cas_blobs` row is missing. Empties
    /// `blobs`.
    async fn note_missing_rows(
        &self,
        blobs: &mut Vec<String>,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        let asked = std::mem::take(blobs);
        if asked.is_empty() {
            return Ok(());
        }
        let Some(rows) = self.blob_rows_exist(&asked).await? else {
            return Ok(());
        };
        for (blob, present) in asked.into_iter().zip(rows) {
            if !present {
                report.rows_missing += 1;
                if report.missing_row_samples.len() < MISSING_SAMPLES {
                    report.missing_row_samples.push(blob);
                }
            }
        }
        Ok(())
    }

    /// A counter that keeps its `zero_since` must have its queue entry, or the
    /// dwell pass never sees the blob. Find the entries that are missing.
    async fn find_missing_entries(
        &self,
        queue: &BlobQueue,
        in_dwell: &[(String, u64)],
        restore: &mut Vec<(String, u64)>,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        if in_dwell.is_empty() {
            return Ok(());
        }
        let keys = in_dwell
            .iter()
            .map(|(blob, at)| queue.entry_key(*at, blob).into_bytes())
            .collect();
        let found = self
            .raw()
            .inner()
            .exists_many(keys)
            .await
            .map_err(SchemaError::from)?;
        for ((blob, at), present) in in_dwell.iter().zip(found) {
            if !present {
                report.queue_restored += 1;
                restore.push((blob.clone(), *at));
            }
        }
        Ok(())
    }

    /// Delete the queue entries that no counter at zero backs. In a dry run the
    /// counters are the stored ones, so a counter that the step above would fix
    /// shows its old state.
    async fn recount_blob_queue(
        &self,
        execute: bool,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        let queue = self.blob_events.queue();
        let (start, end) = BlobQueue::entries_range();
        let mut walk = RangeWalk::new(&start, &end, PAGE_ROWS);
        let mut pending = Vec::new();
        // Buckets of the entries that stay: each needs its directory row.
        let mut kept: BTreeSet<u64> = BTreeSet::new();
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            let entries: Vec<(Vec<u8>, u64, String)> = step
                .rows
                .iter()
                .map(|(key, _)| match queue.parse_entry(key) {
                    Some((at, id)) => (key.clone(), at, id),
                    None => (key.clone(), 0, String::new()),
                })
                .collect();
            let keys: Vec<String> = entries
                .iter()
                .map(|(_, _, id)| super::blob_counter_key_of_id(id))
                .collect();
            let counters: Vec<BlobRead> = self.read_blob_counters_at(&keys).await?;
            for ((key, at, _), counter) in entries.into_iter().zip(counters) {
                report.queue_read += 1;
                let backed = counter.counter().is_some_and(|counter| {
                    counter.refs == 0 && !counter.suspect && counter.zero_since == Some(at)
                });
                if backed {
                    let bucket = std::str::from_utf8(&key)
                        .ok()
                        .and_then(|key| queue.layout().entry_bucket(key));
                    kept.extend(bucket);
                } else {
                    report.queue_deleted += 1;
                    pending.push(KvMutation::delete(key));
                }
                if pending.len() >= BATCH_OPERATIONS {
                    self.flush_blob_batch(&mut pending, execute, report).await?;
                }
            }
        }
        self.flush_blob_batch(&mut pending, execute, report).await?;
        self.check_blob_directory(&queue, &kept, execute, report)
            .await
    }
}
