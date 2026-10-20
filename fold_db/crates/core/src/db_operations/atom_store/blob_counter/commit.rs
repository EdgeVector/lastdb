//! The store side of the blob planner: read the counters, plan, build the writes.
//!
//! [`AtomStore::blob_counter_mutations`] is called by the atom counter planner
//! (`counter::build`) inside the native write request scope. It returns the
//! mutations that join the one batch of the tip change, so a blob count and the
//! references it counts change together. [`AtomStore::register_blob_upload`] is
//! the other writer: a blob row put starts the dwell of a blob that has no
//! reference yet, in the same scope as the row.

use super::plan::{plan_delta, plan_upload, BlobEvent, BlobPlan, BlobWrite, PlanError};
use super::{blob_counter_key, blob_queue_key, BlobCounter, BlobCounterView, BlobDeltas};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::Serialize;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

/// The value of a queue entry. The key holds only the hash of the blob.
#[derive(Serialize)]
struct QueueValue<'a> {
    blob: &'a str,
}

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
        let now = crate::clock::unix_millis();
        let blobs: Vec<&str> = changes.iter().map(|(blob, _)| blob.as_str()).collect();
        let before = self.read_blob_counters(&blobs).await?;
        let mut mutations = Vec::new();
        for ((blob, delta), counter) in changes.iter().zip(&before) {
            let plan = plan_delta(blob, counter.as_ref(), *delta, now).map_err(
                |PlanError::Overflow| {
                    SchemaError::InvalidData(format!("blob {blob} reference count overflows"))
                },
            )?;
            self.push_blob_plan(blob, plan, &mut mutations)?;
        }
        Ok(mutations)
    }

    /// A blob row was put. A blob with no reference starts or renews its dwell,
    /// because the atom that names it comes after the upload. The caller holds
    /// the native write gate and has put the row in the same request scope.
    pub(crate) async fn register_blob_upload(&self, blob_ref: &str) -> Result<(), SchemaError> {
        self.require_atomic_scope()?;
        let before = self.read_blob_counters(&[blob_ref]).await?;
        let plan = plan_upload(blob_ref, before[0].as_ref(), crate::clock::unix_millis());
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
    ) -> Result<Vec<Option<BlobCounter>>, SchemaError> {
        let keys: Vec<String> = blobs.iter().map(|blob| blob_counter_key(blob)).collect();
        self.read_blob_counters_at(&keys).await
    }

    /// The counter state at each counter key, in the order given.
    pub(super) async fn read_blob_counters_at(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<BlobCounter>>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            out.extend(
                self.main_store
                    .get_items::<BlobCounter>(chunk)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read blob counters: {error}"))
                    })?,
            );
        }
        Ok(out)
    }

    /// What the counter store says about each blob, for the explain route. Three
    /// reads by key: the counters, the queue entries and the blob rows.
    pub async fn blob_counter_views(
        &self,
        blobs: &[String],
    ) -> Result<Vec<BlobCounterView>, SchemaError> {
        let refs: Vec<&str> = blobs.iter().map(String::as_str).collect();
        let counters = self.read_blob_counters(&refs).await?;
        let queue_keys: Vec<Vec<u8>> = blobs
            .iter()
            .zip(&counters)
            .map(|(blob, counter)| {
                let at = counter.as_ref().and_then(|counter| counter.zero_since);
                blob_queue_key(at.unwrap_or(0), blob).into_bytes()
            })
            .collect();
        let queued = self
            .main_store
            .inner()
            .exists_many(queue_keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read blob queue: {error}")))?;
        let rows = self.blob_rows_exist(blobs).await?;
        Ok(blobs
            .iter()
            .zip(counters)
            .zip(queued.into_iter().zip(rows))
            .map(|((blob, counter), (queued, row_present))| BlobCounterView {
                blob: blob.clone(),
                refs: counter.as_ref().map(|counter| counter.refs),
                zero_since: counter.as_ref().and_then(|counter| counter.zero_since),
                suspect: counter.as_ref().is_some_and(|counter| counter.suspect),
                queued: queued && counter.as_ref().is_some_and(|c| c.zero_since.is_some()),
                row_present,
            })
            .collect())
    }

    /// For each blob, whether the `cas_blobs` plane holds its row.
    async fn blob_rows_exist(&self, blobs: &[String]) -> Result<Vec<bool>, SchemaError> {
        let Some(store) = self.namespaced_store.as_ref() else {
            return Ok(vec![false; blobs.len()]);
        };
        let cas = store
            .open_namespace("cas_blobs")
            .await
            .map_err(|error| SchemaError::InvalidData(format!("open cas_blobs: {error}")))?;
        let keys = blobs.iter().map(|blob| blob.as_bytes().to_vec()).collect();
        cas.exists_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read blob rows: {error}")))
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
                out.extend(write_mutations(blob, write)?);
                if let Some(event) = event {
                    self.blob_events.note(blob, event);
                }
            }
        }
        Ok(())
    }
}

/// The counter put, with the queue entry that goes or comes.
fn write_mutations(blob: &str, write: BlobWrite) -> Result<Vec<KvMutation>, SchemaError> {
    let mut out = Vec::with_capacity(3);
    if let Some(at) = write.unqueue {
        out.push(KvMutation::delete(blob_queue_key(at, blob).into_bytes()));
    }
    out.push(counter_put(&write.counter)?);
    if let Some(at) = write.enqueue {
        out.push(queue_put(blob, at)?);
    }
    Ok(out)
}

/// The put of one counter record.
pub(super) fn counter_put(counter: &BlobCounter) -> Result<KvMutation, SchemaError> {
    Ok(KvMutation::put(
        blob_counter_key(&counter.blob).into_bytes(),
        to_bytes(counter, "blob counter")?,
    ))
}

/// The put of the queue entry of `blob` at `zero_since`.
pub(super) fn queue_put(blob: &str, zero_since: u64) -> Result<KvMutation, SchemaError> {
    Ok(KvMutation::put(
        blob_queue_key(zero_since, blob).into_bytes(),
        to_bytes(&QueueValue { blob }, "blob queue entry")?,
    ))
}

fn to_bytes<T: Serialize>(value: &T, what: &str) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(format!("serialize {what}: {error}")))
}
