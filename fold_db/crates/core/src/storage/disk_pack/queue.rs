//! Bounded complete requests share one generation-bound stored publication.

use super::run_commit::PreparedCommit;
use super::*;
use tracing::Instrument;

const MAX_REQUESTS: usize = 1024;

/// Durable native boundary after all earlier accepted batches finish.
#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
}

pub(in crate::storage) struct WriteGuard {
    _preparation: tokio::sync::OwnedMutexGuard<()>,
    _publication: tokio::sync::OwnedMutexGuard<()>,
}

struct QueuedRequest {
    pending: Arc<transaction::PendingBatch>,
    sequence: u64,
}

#[derive(Default)]
pub(super) struct WriteQueue {
    // Success clears this list only after disk publication and delivery. A
    // failure leaves the sealed queue as the owner of every pending reservation.
    requests: Vec<QueuedRequest>,
    prepared: Option<PreparedCommit>,
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
    /// Wake consumers after a durable write. Use the actual frontier to
    /// decide whether work changed across restarts or failed writes.
    pub fn subscribe_durable_writes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.completed.subscribe()
    }

    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.flush_prefix().await?;
        let view = self.view.read().expect("native durable flush view");
        Ok(NativeFlushReceipt {
            generation: view.current.generation(),
            frontier: view.current.frontier(),
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    pub(in crate::storage::disk_pack) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        self.flush_prefix().await.map(|_| ())
    }

    async fn flush_prefix(self: &Arc<Self>) -> StorageResult<u64> {
        let target = self.seal_queue();
        self.wait_for_queue(target).await?;
        Ok(target)
    }

    pub(in crate::storage) async fn enter_write(self: &Arc<Self>) -> WriteGuard {
        loop {
            let gate = self.enter_prepare().await;
            let target = self.seal_queue();
            if target <= *self.completed.borrow() || self.failed.load(Ordering::Acquire) {
                let publication = Arc::clone(&self.write_gate).lock_owned().await;
                return WriteGuard {
                    _preparation: gate,
                    _publication: publication,
                };
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }

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

    pub(super) fn preparation_prefix(&self) -> Arc<Vec<Arc<transaction::PendingBatch>>> {
        let queue = self.queue.lock().expect("native preparation prefix");
        Arc::new(
            queue
                .requests
                .iter()
                .map(|request| Arc::clone(&request.pending))
                .collect(),
        )
    }

    /// Quote only new operations; the accepted prefix retains its owned quota.
    pub(super) fn prepare_queued(
        &self,
        incoming: Vec<HomeMutation>,
    ) -> StorageResult<Option<PreparedCommit>> {
        let queue = self.queue.lock().expect("native accepted queue");
        if queue.requests.len() >= MAX_REQUESTS {
            return Err(StorageError::WriteResourceLimit {
                detail: "native accepted queue reached 1024 requests; retry after flush".into(),
            });
        }
        if incoming.is_empty() {
            return Ok(None);
        }
        self.prepare_commit(incoming, queue.prepared.as_ref())
            .map(Some)
    }

    /// No await separates accepted publication from transfer to the queue owner.
    pub(super) fn accept_queued(
        self: &Arc<Self>,
        pending: Arc<transaction::PendingBatch>,
        prepared: Option<PreparedCommit>,
    ) -> StorageResult<u64> {
        let mut queue = self.queue.lock().expect("native accepted queue");
        let sequence = queue.next_sequence.checked_add(1).ok_or_else(|| {
            StorageError::BackendError("native accepted sequence exhausted".into())
        })?;
        let first = queue.requests.is_empty();
        if let (Some(incoming), Some(previous)) = (&prepared, queue.prepared.as_mut()) {
            previous
                .operations
                .try_reserve(incoming.operations.len())
                .map_err(|_| StorageError::WriteResourceLimit {
                    detail: "native prefix operation allocation refused".into(),
                })?;
        }
        queue.requests.push(QueuedRequest { pending, sequence });
        queue.next_sequence = sequence;
        if let Some(mut incoming) = prepared {
            if let Some(mut previous) = queue.prepared.take() {
                previous.operations.append(&mut incoming.operations);
                incoming.operations = previous.operations;
                incoming.space.absorb(previous.space);
            }
            queue.prepared = Some(incoming);
        }
        queue.sealed |= queue.requests.len() == MAX_REQUESTS;
        drop(queue);
        if first {
            let runtime = Arc::clone(self);
            tokio::spawn(async move { runtime.run_queue().await }.in_current_span());
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
            let (_preparation, _gate) = (preparation, gate);
            let result = runtime.commit_queue();
            if result.is_err() {
                runtime.failed.store(true, Ordering::Release);
            }
            result
        })
        .await;
        match result {
            Ok((sequence, requests)) => {
                for request in &requests {
                    if let Err(error) = request.deliver().await {
                        self.failed.store(true, Ordering::Release);
                        self.completed.send_modify(|_| {});
                        tracing::error!(%error, "native durable request post-commit delivery requires recovery");
                        return;
                    }
                }
                {
                    let mut queue = self.queue.lock().expect("native accepted queue");
                    let blocked = gate_from.elapsed();
                    for request in &requests {
                        request.note_flush_blocked(blocked);
                    }
                    queue.requests.clear();
                    queue.sealed = false;
                }
                let signal = self.completed.clone();
                drop(requests);
                drop(self);
                signal.send_modify(|completed| *completed = (*completed).max(sequence));
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                self.completed.send_modify(|_| {});
                tracing::error!(%error, "native complete accepted prefix requires recovery");
            }
        }
    }

    fn commit_queue(&self) -> StorageResult<(u64, Vec<Arc<transaction::PendingBatch>>)> {
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
            self.commit_prepared(prepared, &hidden, &requests)?;
        } else {
            let current = self
                .view
                .read()
                .expect("native empty prefix root")
                .current
                .clone();
            self.install_committed(current, &hidden, &requests)?;
        }
        let queue = self.queue.lock().expect("native accepted queue");
        for request in &queue.requests {
            request.pending.finish_publications()?;
        }
        let sequence = queue
            .requests
            .last()
            .map_or(queue.next_sequence, |request| request.sequence);
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
