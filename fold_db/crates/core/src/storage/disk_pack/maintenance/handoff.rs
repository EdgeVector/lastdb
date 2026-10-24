//! One optional rewrite joins an already accepted normal commit.
use super::*;
use laststore::disk_pack::home::MutationReservation;
use laststore::disk_pack::maintenance::PreparedCompaction;
use laststore::disk_pack::PublishedGeneration;

pub(super) enum Offer {
    Retry(Box<PreparedCompaction>),
    Queued(tokio::sync::oneshot::Receiver<StorageResult<HandoffResult>>),
}

pub(super) enum HandoffResult {
    Ready(Box<publish::MaintenanceReady>),
    Retry(Box<PreparedCompaction>),
}

pub(super) async fn receive_handoff(
    receive: tokio::sync::oneshot::Receiver<StorageResult<HandoffResult>>,
) -> StorageResult<HandoffResult> {
    receive.await.map_err(|_| {
        StorageError::BackendError(
            "compaction handoff owner stopped before its durable result".into(),
        )
    })?
}

pub(in crate::storage::disk_pack) struct PackHandoff {
    output: Mutex<Option<PreparedCompaction>>,
    result: Mutex<Option<StorageResult<publish::MaintenanceReady>>>,
    complete: Mutex<Option<tokio::sync::oneshot::Sender<StorageResult<publish::MaintenanceReady>>>>,
    retry: u64,
    claimed: AtomicBool,
}

impl PackRuntime {
    pub(super) fn offer_pack_merge(
        self: &Arc<Self>,
        output: PreparedCompaction,
        retry: u64,
    ) -> Offer {
        let mut queue = self.queue.lock().expect("native compaction handoff");
        // Keep the proposal even if the current worker already took its prefix.
        // Its next normal commit can claim this slot after delivery completes.
        if queue.compaction.is_some() {
            return Offer::Retry(Box::new(output));
        }
        let (send, receive) = tokio::sync::oneshot::channel();
        let handoff = Arc::new(PackHandoff {
            output: Mutex::new(Some(output)),
            result: Mutex::new(None),
            complete: Mutex::new(Some(send)),
            retry,
            claimed: AtomicBool::new(false),
        });
        queue.compaction = Some(Arc::clone(&handoff));
        drop(queue);
        let (complete, result) = tokio::sync::oneshot::channel();
        let runtime = Arc::clone(self);
        // Registration and this independent cleanup owner contain no await.
        // Even an abandoned blocking Offer result cannot strand the slot.
        tokio::spawn(
            async move {
                let result = runtime.wait_pack_handoff(handoff, receive).await;
                let _ = complete.send(result);
                runtime.capture_cleanup.release_idle();
            }
            .in_current_span(),
        );
        Offer::Queued(result)
    }

    async fn wait_pack_handoff(
        &self,
        handoff: Arc<PackHandoff>,
        mut receive: tokio::sync::oneshot::Receiver<StorageResult<publish::MaintenanceReady>>,
    ) -> StorageResult<HandoffResult> {
        loop {
            tokio::select! {
                result = &mut receive => {
                    return result.map_err(|_| StorageError::BackendError(
                        "compaction handoff owner stopped before its durable result".into(),
                    ))?.map(|ready| HandoffResult::Ready(Box::new(ready)));
                }
                // This idle check delays only maintenance. Client admission
                // and its disk worker never wait for this timer.
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    let mut queue = self.queue.lock().expect("native compaction handoff");
                    let failed = self.failed.load(Ordering::Acquire);
                    if (queue.requests_idle() || failed) && !handoff.claimed.load(Ordering::Acquire)
                        && queue.compaction.as_ref().is_some_and(|job| Arc::ptr_eq(job, &handoff))
                    {
                        queue.compaction.take();
                        drop(queue);
                        let output = handoff.output.lock().expect("native merge output").take()
                            .expect("unclaimed compaction output");
                        if failed {
                            drop(output);
                            return Err(StorageError::BackendError(
                                "native writer failed before it claimed compaction".into(),
                            ));
                        }
                        return Ok(HandoffResult::Retry(Box::new(output)));
                    }
                }
            }
        }
    }

    /// The worker calls this after its blocking commit releases both gates.
    pub(in crate::storage::disk_pack) fn complete_pack_handoff(&self) {
        let handoff = {
            let mut queue = self.queue.lock().expect("native compaction handoff");
            if queue
                .compaction
                .as_ref()
                .is_some_and(|job| job.claimed.load(Ordering::Acquire))
            {
                queue.compaction.take()
            } else {
                None
            }
        };
        if let Some(handoff) = handoff {
            // Large private path maps and unused files drop outside the gate.
            drop(handoff.output.lock().expect("native merge output").take());
            let result = handoff
                .result
                .lock()
                .expect("native merge result")
                .take()
                .unwrap_or_else(|| {
                    Err(StorageError::BackendError(
                        "compaction handoff did not complete its normal commit".into(),
                    ))
                });
            if let Some(send) = handoff
                .complete
                .lock()
                .expect("native merge completion")
                .take()
            {
                let _ = send.send(result);
            };
        }
        self.capture_cleanup.release_idle();
    }
}

impl PackHandoff {
    pub(in crate::storage::disk_pack) fn claim(&self) {
        self.claimed.store(true, Ordering::Release);
    }

    pub(in crate::storage::disk_pack) fn commit(
        &self,
        writer: &mut HomeSnapshot,
        operations: Vec<HomeMutation>,
        reservation: MutationReservation,
    ) -> (
        laststore::Result<PublishedGeneration>,
        Option<MaintenanceReceipt>,
    ) {
        let mut output = self.output.lock().expect("native merge output");
        let output = output.as_mut().expect("accepted compaction output");
        let preparing = std::time::Instant::now();
        let timing = laststore::commit_timing::stage("compaction_merge_prepare_us");
        let merge = match writer.prepare_pack_merge(output, &reservation) {
            Ok(merge) => merge,
            Err(error) => {
                drop(timing);
                // An optional rewrite cannot consume a client's accepted quota
                // on a preflight refusal. Its normal commit remains unchanged.
                self.record(Err(map(error)));
                return (writer.commit_mutation(operations, reservation), None);
            }
        };
        drop(timing);
        tracing::debug!(target: "fold_db::compaction", prepared_ms = millis_since(preparing),
            captured_locations = merge.operations(),
            retry = self.retry, "compaction merge prepared for normal commit");
        match writer.commit_mutation_merged(operations, reservation, output, merge) {
            Ok((publication, mut receipt)) => {
                receipt.publication_retries = self.retry;
                (Ok(publication), Some(receipt))
            }
            Err(error) => {
                self.record(Err(StorageError::BackendError(error.to_string())));
                (Err(error), None)
            }
        }
    }

    pub(in crate::storage::disk_pack) fn record(
        &self,
        result: StorageResult<publish::MaintenanceReady>,
    ) {
        *self.result.lock().expect("native merge result") = Some(result);
    }
}
