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
//! backs is deleted.

use super::{blob_queue_key, BlobCounter};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Operations in one batch.
const BATCH_OPERATIONS: usize = 400;
/// Rows asked for in each read.
const PAGE_ROWS: usize = 1_000;

/// What the blob step found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRecountReport {
    /// The blob step ran.
    pub ran: bool,
    /// The counters were set to the exact count (`--blobs-exact`).
    pub exact: bool,
    /// Blobs that atom counters or tips of the listed scopes name.
    pub referenced_blobs: u64,
    pub atom_counters_read: u64,
    pub tips_read: u64,
    pub counters_read: u64,
    pub unchanged: u64,
    /// A counter was missing and is made.
    pub created: u64,
    /// A counter held less than the count.
    pub raised: u64,
    /// A counter held more than the count (exact only).
    pub lowered: u64,
    /// A `suspect` flag cleared (exact only).
    pub suspect_cleared: u64,
    /// A counter with a dwell got references, so the dwell ends.
    pub dwell_cancelled: u64,
    /// A blob that nothing references starts its dwell (exact only).
    pub dwell_started: u64,
    pub unreadable_counters: u64,
    pub queue_read: u64,
    /// Queue entries that no counter at zero backs, deleted.
    pub queue_deleted: u64,
    /// A counter with a `zero_since` had no queue entry. The entry is put.
    #[serde(default)]
    pub queue_restored: u64,
    pub operations: u64,
    pub batches: u64,
    pub notes: Vec<String>,
}

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

fn mutations(fix: &Fix) -> Result<Vec<KvMutation>, SchemaError> {
    let blob = &fix.counter.blob;
    let mut out = Vec::with_capacity(3);
    if let Some(at) = fix.unqueue {
        out.push(KvMutation::delete(blob_queue_key(at, blob).into_bytes()));
    }
    out.push(super::commit::counter_put(&fix.counter)?);
    if let Some(at) = fix.enqueue {
        out.push(super::commit::queue_put(blob, at)?);
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
        let now = crate::clock::unix_millis();
        let mut pending = Vec::new();
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
                match decide(&counter.blob, Some(&counter), held, exact, now) {
                    None => {
                        report.unchanged += 1;
                        if let Some(at) = counter.zero_since {
                            in_dwell.push((counter.blob.clone(), at));
                        }
                    }
                    Some(fix) => {
                        count(report, &fix);
                        pending.extend(mutations(&fix)?);
                    }
                }
                if pending.len() >= BATCH_OPERATIONS {
                    self.flush_blob_batch(&mut pending, execute, report).await?;
                }
            }
            self.restore_queue_entries(&in_dwell, &mut pending, report)
                .await?;
        }
        // Blobs that are named and have no counter.
        for (blob, held) in holders {
            if let Some(fix) = decide(&blob, None, held, exact, now) {
                count(report, &fix);
                pending.extend(mutations(&fix)?);
            }
            if pending.len() >= BATCH_OPERATIONS {
                self.flush_blob_batch(&mut pending, execute, report).await?;
            }
        }
        self.flush_blob_batch(&mut pending, execute, report).await?;
        if report.unreadable_counters > 0 {
            report.notes.push(format!(
                "{} blob counter(s) do not decode and were not changed",
                report.unreadable_counters
            ));
        }
        self.recount_blob_queue(execute, report).await
    }

    /// A counter that keeps its `zero_since` must have its queue entry, or the
    /// dwell pass never sees the blob. Put the entry that is missing.
    async fn restore_queue_entries(
        &self,
        in_dwell: &[(String, u64)],
        pending: &mut Vec<KvMutation>,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        if in_dwell.is_empty() {
            return Ok(());
        }
        let keys = in_dwell
            .iter()
            .map(|(blob, at)| blob_queue_key(*at, blob).into_bytes())
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
                pending.push(super::commit::queue_put(blob, *at)?);
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
        let (start, end) = super::queue_range();
        let mut walk = RangeWalk::new(&start, &end, PAGE_ROWS);
        let mut pending = Vec::new();
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            let entries: Vec<(Vec<u8>, u64, String)> = step
                .rows
                .iter()
                .map(|(key, _)| match super::parse_blob_queue_key(key) {
                    Some((at, id)) => (key.clone(), at, id),
                    None => (key.clone(), 0, String::new()),
                })
                .collect();
            let keys: Vec<String> = entries
                .iter()
                .map(|(_, _, id)| super::blob_counter_key_of_id(id))
                .collect();
            let counters = self.read_blob_counters_at(&keys).await?;
            for ((key, at, _), counter) in entries.into_iter().zip(counters) {
                report.queue_read += 1;
                let backed = counter.is_some_and(|counter| {
                    counter.refs == 0 && !counter.suspect && counter.zero_since == Some(at)
                });
                if !backed {
                    report.queue_deleted += 1;
                    pending.push(KvMutation::delete(key));
                }
                if pending.len() >= BATCH_OPERATIONS {
                    self.flush_blob_batch(&mut pending, execute, report).await?;
                }
            }
        }
        self.flush_blob_batch(&mut pending, execute, report).await
    }
}
