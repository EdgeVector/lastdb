//! The store side of the blob planner: read the counters, plan, build the writes.
//!
//! [`AtomStore::blob_counter_mutations`] is called by the atom counter planner
//! (`counter::build`) inside the native write request scope. It returns the
//! mutations that join the one batch of the tip change, so a blob count and the
//! references it counts change together. [`AtomStore::register_blob_upload`] is
//! the other writer: a blob row put starts the dwell of a blob that has no
//! reference yet, in the same scope as the row.

use super::plan::{
    plan_delta, plan_unreadable, plan_upload, BlobEvent, BlobPlan, BlobWrite, PlanError,
};
use super::{blob_counter_key, BlobCounter, BlobCounterView, BlobDeltas, BlobQueue, BlobRead};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::Serialize;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

impl AtomStore {
    /// The blob counter writes of one planner run. Reads the counters by key in
    /// one batch. The caller holds the native write gate.
    pub(crate) async fn blob_counter_mutations(
        &self,
        deltas: BlobDeltas,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let changes = deltas.into_changes();
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        let now = self.blob_clock_millis();
        let blobs: Vec<&str> = changes.iter().map(|(blob, _)| blob.as_str()).collect();
        let before = self.read_blob_counters(&blobs).await?;
        self.note_late_references(&changes, &before).await?;
        let mut mutations = Vec::new();
        for ((blob, delta), read) in changes.iter().zip(&before) {
            let plan = match read {
                BlobRead::Unreadable => plan_unreadable(blob, *delta),
                read => plan_delta(blob, read.counter(), *delta, now).map_err(
                    |PlanError::Overflow| {
                        SchemaError::InvalidData(format!("blob {blob} reference count overflows"))
                    },
                )?,
            };
            self.push_blob_plan(blob, plan, &mut mutations)?;
        }
        Ok(BlobQueue::keep_last_directory_rows(mutations))
    }

    /// A reference that makes the counter of a blob whose row is gone is a late
    /// reference: the dwell deleted the blob (the counter with it), and an atom
    /// that names it arrives afterwards. The write goes through, because a blob
    /// that comes from a peer arrives after the atom that names it. The report
    /// counts it and logs the blob, `lastdb liveness explain blob` says
    /// `referenced_row_missing`, and `lastdb db verify-refs` reports
    /// `blob_counter_without_blob`. One keyed read of the rows of the blobs that
    /// have no counter; a blob that has a counter costs nothing.
    async fn note_late_references(
        &self,
        changes: &[(String, i64)],
        before: &[BlobRead],
    ) -> Result<(), SchemaError> {
        let fresh: Vec<String> = changes
            .iter()
            .zip(before)
            .filter(|((_, delta), read)| *delta > 0 && matches!(read, BlobRead::Absent))
            .map(|((blob, _), _)| blob.clone())
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let Some(rows) = self.blob_rows_exist(&fresh).await? else {
            return Ok(());
        };
        for (blob, present) in fresh.iter().zip(rows) {
            if !present {
                self.blob_events.note(blob, BlobEvent::LateReference);
            }
        }
        Ok(())
    }

    /// A blob row was put. A blob with no reference starts or renews its dwell,
    /// because the atom that names it comes after the upload. The caller holds
    /// the native write gate and has put the row in the same request scope.
    pub(crate) async fn register_blob_upload(&self, blob_ref: &str) -> Result<(), SchemaError> {
        self.require_atomic_scope()?;
        let before = self.read_blob_counters(&[blob_ref]).await?;
        let plan = match &before[0] {
            // A counter that does not decode is `suspect` once a planner run
            // touches it. An upload leaves it alone: the blob stays.
            BlobRead::Unreadable => BlobPlan::Unchanged,
            read => plan_upload(blob_ref, read.counter(), self.blob_clock_millis()),
        };
        let mut mutations = Vec::new();
        self.push_blob_plan(blob_ref, plan, &mut mutations)?;
        if !mutations.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("register blob upload: {error}"))
                })?;
        }
        Ok(())
    }

    /// The counter state of each blob, in the order given. One read by key.
    pub(crate) async fn read_blob_counters(
        &self,
        blobs: &[&str],
    ) -> Result<Vec<BlobRead>, SchemaError> {
        let keys: Vec<String> = blobs.iter().map(|blob| blob_counter_key(blob)).collect();
        self.read_blob_counters_at(&keys).await
    }

    /// The counter state at each counter key, in the order given. A record that
    /// does not decode reads as [`BlobRead::Unreadable`]. It does not fail the
    /// read, so one bad record cannot stop a chunk of the dwell pass or a write
    /// that names another blob.
    pub(super) async fn read_blob_counters_at(
        &self,
        keys: &[String],
    ) -> Result<Vec<BlobRead>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            let bytes = self
                .main_store
                .inner()
                .get_many(chunk.iter().map(|key| key.clone().into_bytes()).collect())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("read blob counters: {error}"))
                })?;
            out.extend(bytes.iter().map(|bytes| BlobRead::parse(bytes.as_deref())));
        }
        Ok(out)
    }

    /// What the counter store says about each blob, for the explain route. Three
    /// reads by key: the counters, the queue entries and the blob rows.
    pub async fn blob_counter_views(
        &self,
        blobs: &[String],
    ) -> Result<Vec<BlobCounterView>, SchemaError> {
        // The three reads see one committed root. A dwell chunk deletes the row,
        // the counter and the queue entry in one batch, so one root shows all of
        // them or none of them.
        self.atom_read_scope(self.read_blob_views(blobs)).await
    }

    async fn read_blob_views(&self, blobs: &[String]) -> Result<Vec<BlobCounterView>, SchemaError> {
        let refs: Vec<&str> = blobs.iter().map(String::as_str).collect();
        let counters = self.read_blob_counters(&refs).await?;
        let queue = self.blob_events.queue();
        let queue_keys: Vec<Vec<u8>> = blobs
            .iter()
            .zip(&counters)
            .map(|(blob, read)| {
                let at = read.counter().and_then(|counter| counter.zero_since);
                queue.entry_key(at.unwrap_or(0), blob).into_bytes()
            })
            .collect();
        let queued = self
            .main_store
            .inner()
            .exists_many(queue_keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read blob queue: {error}")))?;
        let rows = self
            .blob_rows_exist(blobs)
            .await?
            .unwrap_or_else(|| vec![false; blobs.len()]);
        Ok(blobs
            .iter()
            .zip(counters)
            .zip(queued.into_iter().zip(rows))
            .map(|((blob, read), (queued, row_present))| {
                let counter = read.counter();
                BlobCounterView {
                    blob: blob.clone(),
                    refs: counter.map(|counter| counter.refs),
                    zero_since: counter.and_then(|counter| counter.zero_since),
                    suspect: counter.is_some_and(|counter| counter.suspect),
                    queued: queued && counter.is_some_and(|c| c.zero_since.is_some()),
                    row_present,
                    unreadable: read == BlobRead::Unreadable,
                }
            })
            .collect())
    }

    /// For each blob, whether the `cas_blobs` plane holds its row. `None` when
    /// the store has no native plane to ask.
    pub(in crate::db_operations::atom_store) async fn blob_rows_exist(
        &self,
        blobs: &[String],
    ) -> Result<Option<Vec<bool>>, SchemaError> {
        let Some(store) = self.namespaced_store.as_ref() else {
            return Ok(None);
        };
        let cas = store
            .open_namespace("cas_blobs")
            .await
            .map_err(|error| SchemaError::InvalidData(format!("open cas_blobs: {error}")))?;
        let keys = blobs.iter().map(|blob| blob.as_bytes().to_vec()).collect();
        cas.exists_many(keys)
            .await
            .map(Some)
            .map_err(|error| SchemaError::InvalidData(format!("read blob rows: {error}")))
    }

    /// The counter put, with the queue entry that goes or comes. A new entry
    /// brings the directory row of its bucket.
    fn write_mutations(
        &self,
        blob: &str,
        write: &BlobWrite,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let queue = self.blob_events.queue();
        let mut out = Vec::with_capacity(4);
        if let Some(at) = write.unqueue {
            out.push(KvMutation::delete(queue.entry_key(at, blob).into_bytes()));
        }
        out.push(counter_put(&write.counter)?);
        if let Some(at) = write.enqueue {
            out.extend(queue.put_mutations(at, blob)?);
        }
        Ok(out)
    }

    /// Unix milliseconds on the clock of the blob counters: the wall clock, never
    /// more than a day ahead of the monotonic time since the store was made.
    #[must_use]
    pub fn blob_clock_millis(&self) -> u64 {
        self.blob_events.clock.now_millis()
    }

    fn push_blob_plan(
        &self,
        blob: &str,
        plan: BlobPlan,
        out: &mut Vec<KvMutation>,
    ) -> Result<(), SchemaError> {
        match plan {
            BlobPlan::Unchanged => {}
            BlobPlan::Dangling => self.blob_events.note(blob, BlobEvent::Dangling),
            BlobPlan::Write(write) => {
                let event = write.event;
                out.extend(self.write_mutations(blob, &write)?);
                if let Some(event) = event {
                    self.blob_events.note(blob, event);
                }
            }
        }
        Ok(())
    }
}

/// The put of one counter record.
pub(super) fn counter_put(counter: &BlobCounter) -> Result<KvMutation, SchemaError> {
    Ok(KvMutation::put(
        blob_counter_key(&counter.blob).into_bytes(),
        to_bytes(counter, "blob counter")?,
    ))
}

fn to_bytes<T: Serialize>(value: &T, what: &str) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(format!("serialize {what}: {error}")))
}
