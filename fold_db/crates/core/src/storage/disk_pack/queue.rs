//! Bounded complete requests share one generation-bound stored publication.

use super::*;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;

struct QueuedRequest {
    pending: Arc<transaction::PendingBatch>,
    sequence: u64,
}

#[derive(Default)]
pub(super) struct WriteQueue {
    // Success clears this list only after disk publication and delivery. A
    // failure leaves the sealed queue as the owner of every pending reservation.
    requests: Vec<QueuedRequest>,
    prepared: Option<write::PreparedCommit>,
    next_sequence: u64,
    sealed: bool,
}

impl WriteQueue {
    pub(super) fn request_count(&self) -> usize {
        self.requests.len()
    }

    pub(super) fn sequence(&self) -> u64 {
        self.next_sequence
    }
}

impl PackRuntime {
    /// A sealed prefix excludes new preparation until its durable result.
    pub(super) async fn enter_prepare(self: &Arc<Self>) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            let gate = Arc::clone(&self.prepare_gate).lock_owned().await;
            let target = {
                let queue = self.queue.lock().expect("native accepted queue");
                queue.sealed.then_some(queue.next_sequence)
            };
            let Some(target) = target else { return gate };
            if self.failed.load(Ordering::Acquire) {
                return gate;
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

    pub(super) fn preparation_prefix(&self) -> (Arc<Vec<Arc<transaction::PendingBatch>>>, bool) {
        let queue = self.queue.lock().expect("native preparation prefix");
        let hidden = !queue.requests.is_empty();
        (
            Arc::new(
                queue
                    .requests
                    .iter()
                    .map(|request| Arc::clone(&request.pending))
                    .collect(),
            ),
            hidden,
        )
    }

    /// Quote only new operations; the accepted prefix retains its owned quota.
    pub(super) fn prepare_queued(
        &self,
        incoming: Vec<HomeMutation>,
    ) -> StorageResult<Option<write::PreparedAppend>> {
        let queue = self.queue.lock().expect("native accepted queue");
        if queue.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::WriteResourceLimit {
                detail: "native accepted queue reached 1024 requests; retry after flush".into(),
            });
        }
        if incoming.is_empty() {
            return Ok(None);
        }
        self.prepare_append(incoming, queue.prepared.as_ref())
            .map(Some)
    }

    /// No await separates accepted publication from transfer to the queue owner.
    pub(super) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<transaction::PendingBatch>,
        prepared: Option<write::PreparedAppend>,
    ) -> StorageResult<u64> {
        let mut queue = self.queue.lock().expect("native accepted queue");
        let sequence = queue.next_sequence.checked_add(1).ok_or_else(|| {
            StorageError::BackendError("native accepted sequence exhausted".into())
        })?;
        let first = queue.requests.is_empty();
        if let Some(append) = &prepared {
            append.reserve_capacity(queue.prepared.as_mut())?;
        }
        queue.requests.push(QueuedRequest { pending, sequence });
        queue.next_sequence = sequence;
        if let Some(append) = prepared {
            let previous = queue.prepared.take();
            queue.prepared = Some(append.finish(previous));
        }
        queue.sealed |= queue.requests.len() == MAX_REQUESTS;
        drop(queue);
        if first {
            let runtime = Arc::clone(self);
            tokio::spawn(
                async move {
                    runtime.run_queue().await;
                }
                .in_current_span(),
            );
        }
        Ok(sequence)
    }

    pub(super) fn seal_queue(&self) -> u64 {
        let mut queue = self.queue.lock().expect("native accepted queue");
        if !queue.requests.is_empty() {
            queue.sealed = true;
        }
        queue.next_sequence
    }

    /// A flush waits for its captured prefix, not a moving queue tail.
    pub(super) async fn wait_for_queue(&self, target: u64) -> StorageResult<()> {
        let mut completed = self.completed.subscribe();
        loop {
            if self.failed.load(Ordering::Acquire) {
                return Err(StorageError::BackendError(
                    "native accepted queue requires complete recovery after publication failure"
                        .into(),
                ));
            }
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            completed.changed().await.map_err(|_| {
                StorageError::BackendError(
                    "native accepted queue owner stopped before its durable result".into(),
                )
            })?;
        }
    }

    async fn run_queue(self: Arc<Self>) {
        // FIFO admits callers already waiting ahead of this worker. Later
        // arrivals cannot extend this commit. There is no collection timer.
        let preparation = Arc::clone(&self.prepare_gate).lock_owned().await;
        self.seal_queue();
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
        let gate_from = std::time::Instant::now();
        let runtime = Arc::clone(&self);
        let result = run_blocking(move || {
            let _preparation = preparation;
            let _gate = gate;
            let result = runtime.commit_queue();
            if result.is_err() {
                runtime.failed.store(true, Ordering::Release);
            }
            result
        })
        .await;
        match result {
            Ok((sequence, requests)) => {
                let mut completion = super::timing::start();
                for request in &requests {
                    if let Err(error) = request.deliver().await {
                        super::timing::mark(&mut completion, "post_commit_delivery_us");
                        self.failed.store(true, Ordering::Release);
                        self.completed.send_modify(|_| {});
                        super::timing::mark(&mut completion, "completion_notify_us");
                        tracing::error!(%error, "native durable request post-commit delivery requires recovery");
                        super::timing::completion(completion, sequence, requests.len(), false);
                        return;
                    }
                }
                super::timing::mark(&mut completion, "post_commit_delivery_us");
                {
                    let mut queue = self.queue.lock().expect("native accepted queue");
                    let blocked = gate_from.elapsed();
                    for request in &requests {
                        request.note_flush_blocked(blocked);
                        request.release_resources();
                    }
                    queue.requests.clear();
                    queue.sealed = false;
                }
                super::timing::mark(&mut completion, "completion_cleanup_us");
                self.completed
                    .send_modify(|completed| *completed = (*completed).max(sequence));
                super::timing::mark(&mut completion, "completion_notify_us");
                super::timing::completion(completion, sequence, requests.len(), true);
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                // Notify without advancing the covered durable prefix.
                self.completed.send_modify(|_| {});
                tracing::error!(%error, "native complete accepted prefix requires recovery");
            }
        }
    }

    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<transaction::PendingBatch>>)> {
        let (result, report) = laststore::commit_timing::measure(|| self.commit_queue_inner());
        super::timing::commit(report, &result);
        result
    }

    fn commit_queue_inner(&self) -> StorageResult<(u64, Vec<Arc<transaction::PendingBatch>>)> {
        let (prepared, hidden, requests) = {
            let mut queue = self.queue.lock().expect("native accepted queue");
            queue.sealed = true;
            let hidden = queue
                .requests
                .iter()
                .flat_map(|request| request.pending.changed_coordinates())
                .collect::<Vec<_>>();
            let requests = queue
                .requests
                .iter()
                .map(|request| Arc::clone(&request.pending))
                .collect::<Vec<_>>();
            (queue.prepared.take(), hidden, requests)
        };
        if let Some(prepared) = prepared {
            self.commit_prepared_group(prepared, &hidden, &requests)?;
        } else {
            let current = self
                .view
                .read()
                .expect("native empty prefix root")
                .current
                .clone();
            self.install_committed(current, &hidden, &[], &requests)?;
        }
        let queue = self.queue.lock().expect("native accepted queue");
        for request in &queue.requests {
            request.pending.finish_publications()?;
        }
        let sequence = queue
            .requests
            .last()
            .map_or(queue.next_sequence, |request| request.sequence);
        for request in &queue.requests {
            request.pending.release_memory();
        }
        Ok((
            sequence,
            queue
                .requests
                .iter()
                .map(|request| Arc::clone(&request.pending))
                .collect(),
        ))
    }
}
