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
    Busy,
}

impl PackRuntime {
    pub(super) async fn publish_native_pack(
        self: &Arc<Self>,
        mut output: PreparedCompaction,
    ) -> StorageResult<MaintenanceReady> {
        for retry in 0..8 {
            self.parts.progress.phase(3);
            let runtime = Arc::clone(self);
            let (next, mut staged) =
                super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    let cut = runtime
                        .writer
                        .lock()
                        .expect("native snapshot writer")
                        .capture_pack_root()
                        .map_err(map)?;
                    let staged = output.catch_up_and_prepare(cut).map_err(map)?;
                    Ok((output, staged))
                })
                .await?;
            output = next;
            self.parts.progress.phase(4);
            let runtime = Arc::clone(self);
            let (next, result) =
                super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    // Never hold the gate while waiting for a blocking thread.
                    let Ok(boundary) = Arc::clone(&runtime.write_gate).try_lock_owned() else {
                        drop(staged);
                        return Ok((output, Ok(Attempt::Busy)));
                    };
                    let started = std::time::Instant::now();
                    let mut result =
                        runtime.attempt_native_pack(&mut output, &mut staged, started, retry);
                    drop(boundary);
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
            output = next;
            match result? {
                Attempt::Ready(ready) => return Ok(*ready),
                Attempt::Queue(target) => self.wait_for_queue(target).await?,
                Attempt::Changed => tokio::task::yield_now().await,
                Attempt::Busy => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
        Err(StorageError::WriteResourceLimit {
            detail: "compaction_behind: publication changed or the write queue stayed busy across 8 attempts; retry maintenance".into(),
        })
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
