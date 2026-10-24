//! The only shared boundary installs prepared metadata after an exact root check.
use super::*;
use laststore::disk_pack::maintenance::{CompactionPublication, PreparedCompaction};
use laststore::disk_pack::snapshot::ReclaimPlan;

pub(in crate::storage::disk_pack) type MaintenanceReady =
    (MaintenanceReceipt, u64, ReclaimPlan, Current);

enum Attempt {
    Ready(Box<MaintenanceReady>),
    Queue(u64),
    Changed,
    Handoff(tokio::sync::oneshot::Receiver<StorageResult<super::handoff::HandoffResult>>),
}

impl PackRuntime {
    pub(super) async fn publish_native_pack(
        self: &Arc<Self>,
        mut output: PreparedCompaction,
    ) -> StorageResult<MaintenanceReady> {
        for retry in 0..8 {
            self.parts.progress.phase(3);
            let (next, mut staged) = self.prepare_native_publication(output, retry).await?;
            output = next;
            self.parts.progress.phase(4);
            let runtime = Arc::clone(self);
            let (next, result) =
                super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    // Join the same FIFO as clients from inside the blocking
                    // worker. Waiting owns no gate, and a busy client does not
                    // discard prepared files before the exact-root check.
                    let waiting = std::time::Instant::now();
                    let boundary = runtime.write_gate.blocking_lock();
                    let wait_ms = millis_since(waiting);
                    let started = std::time::Instant::now();
                    let mut output = Some(output);
                    let mut result = runtime.attempt_native_pack(
                        output.as_mut().expect("private compaction"),
                        &mut staged,
                        started,
                        retry,
                    );
                    if matches!(&result, Ok(Attempt::Changed)) {
                        match runtime
                            .offer_pack_merge(output.take().expect("private compaction"), retry)
                        {
                            super::handoff::Offer::Retry(next) => output = Some(*next),
                            super::handoff::Offer::Queued(receive) => {
                                result = Ok(Attempt::Handoff(receive));
                            }
                        }
                    }
                    drop(boundary);
                    tracing::debug!(target: "fold_db::compaction", retry,
                        wait_ms,
                        changed = matches!(&result, Ok(Attempt::Changed)),
                        ready = matches!(&result, Ok(Attempt::Ready(_))),
                        handoff = matches!(&result, Ok(Attempt::Handoff(..))),
                        held_ms = millis_since(started), "compaction publication attempted");
                    if let Ok(Attempt::Ready(ready)) = &mut result {
                        let held = millis_since(started);
                        ready.1 = ready.0.copy_millis.saturating_add(held);
                        runtime.parts.progress.completed(&ready.0, held);
                    }
                    // Large path maps and temporary files never drop under the gate.
                    drop(staged);
                    Ok((output, result))
                })
                .await?;
            match result? {
                Attempt::Ready(ready) => return Ok(*ready),
                Attempt::Handoff(receive) => {
                    match super::handoff::receive_handoff(receive).await? {
                        super::handoff::HandoffResult::Ready(ready) => return Ok(*ready),
                        super::handoff::HandoffResult::Retry(next) => {
                            output = *next;
                            continue;
                        }
                    }
                }
                Attempt::Queue(target) => self.wait_for_queue(target).await?,
                Attempt::Changed => tokio::task::yield_now().await,
            }
            output = next.expect("retry retains private compaction");
        }
        Err(StorageError::WriteResourceLimit {
            detail: "compaction_behind: the root or admitted prefix changed across 8 publication attempts; retry maintenance".into(),
        })
    }

    async fn prepare_native_publication(
        self: &Arc<Self>,
        mut output: PreparedCompaction,
        retry: u64,
    ) -> StorageResult<(PreparedCompaction, CompactionPublication)> {
        let runtime = Arc::clone(self);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            output.clear_merge_capture();
            runtime.capture_cleanup.release_idle();
            let capturing = std::time::Instant::now();
            // A client can own the gate before its blocking commit takes the
            // writer mutex. Capture after that already reserved publication.
            let boundary = runtime.write_gate.blocking_lock();
            let (cut, retired) = runtime
                .writer
                .lock()
                .expect("native snapshot writer")
                .capture_pack_handoff(&mut output)
                .map_err(map)?;
            drop(boundary);
            drop(retired);
            let capture_ms = millis_since(capturing);
            let prepared_from = std::time::Instant::now();
            let frontier = cut.current().frontier;
            let staged = output.catch_up_and_prepare(cut).map_err(map)?;
            tracing::debug!(target: "fold_db::compaction", retry, frontier, capture_ms,
                prepared_ms = millis_since(prepared_from),
                tier_boundary = output.tier_boundary(),
                parts = staged.dependency_parts(), "private compaction prepared");
            Ok((output, staged))
        })
        .await
    }

    pub(in crate::storage::disk_pack) fn finish_merged_pack(
        &self,
        writer: &HomeSnapshot,
        receipt: MaintenanceReceipt,
        started: std::time::Instant,
    ) -> StorageResult<MaintenanceReady> {
        let plan = writer.capture_reclaim().map_err(map)?;
        let held = millis_since(started);
        let pack_millis = receipt.copy_millis.saturating_add(held);
        self.parts.progress.completed(&receipt, held);
        self.parts.record_run(
            &parts::RunCounts {
                parts_before: receipt.parts_before,
                parts_after: receipt.parts_after,
                records: receipt.records_copied + receipt.tombstones_copied,
            },
            pack_millis,
            writer.current(),
        );
        tracing::debug!(target: "fold_db::compaction", retry = receipt.publication_retries,
            wait_ms = 0u64, changed = false, ready = true, merged = true,
            held_ms = held, "compaction publication attempted");
        Ok((receipt, pack_millis, plan, writer.current().clone()))
    }

    fn attempt_native_pack(
        &self,
        output: &mut PreparedCompaction,
        staged: &mut CompactionPublication,
        started: std::time::Instant,
        retry: u64,
    ) -> StorageResult<Attempt> {
        self.require_initialized()?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native compaction requires a recovered writer".into(),
            ));
        }
        if let Some(target) = self
            .queue
            .lock()
            .expect("native accepted queue")
            .compaction_barrier(
                staged.dependency_parts(),
                staged.changes_required_features(),
            )
        {
            return Ok(Attempt::Queue(target));
        }
        let mut writer = self.writer.lock().expect("native snapshot writer");
        let mut receipt = match writer.try_finish_pack(output, staged) {
            Ok(Some(receipt)) => receipt,
            Ok(None) => return Ok(Attempt::Changed),
            Err(error) => {
                if writer.is_poisoned() {
                    self.failed.store(true, Ordering::Release);
                }
                let error = map(error);
                self.parts.record_failure(&error);
                return Err(error);
            }
        };
        receipt.publication_retries = retry;
        let mut view = self.view.write().expect("native complete read view");
        view.current = writer.current().clone();
        let plan = writer.capture_reclaim().map_err(map)?;
        let current = view.current.clone();
        drop(view);
        let pack_millis = receipt.copy_millis.saturating_add(millis_since(started));
        self.parts.record_run(
            &parts::RunCounts {
                parts_before: receipt.parts_before,
                parts_after: receipt.parts_after,
                records: receipt.records_copied + receipt.tombstones_copied,
            },
            pack_millis,
            writer.current(),
        );
        Ok(Attempt::Ready(Box::new((
            receipt,
            pack_millis,
            plan,
            current,
        ))))
    }

    pub(in crate::storage::disk_pack) fn capture_native_cleanup(
        &self,
    ) -> StorageResult<MaintenanceReady> {
        self.require_initialized()?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native cleanup requires a recovered writer".into(),
            ));
        }
        // Commit installs its view before it releases this mutex. Accepted work
        // keeps this durable root, so capture does not flush the queue.
        let writer = self.writer.lock().expect("native snapshot writer");
        let current = writer.current().clone();
        let plan = writer.capture_reclaim().map_err(map)?;
        Ok((MaintenanceReceipt::default(), 0, plan, current))
    }
}
