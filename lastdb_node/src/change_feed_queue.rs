//! Bounded ordered persistence for app change-feed hints.
//!
//! Product mutation truth commits in the resident graph. The durable app feed
//! is a later coordination projection, so it must not delay the T0 response.
//! A caller reserves one bounded queue slot before product commit. A full or
//! closed queue therefore rejects before any product state changes.

use std::sync::Arc;

use fold_db::db_operations::{ChangeFeedEvent, ChangeFeedStore};
use fold_db::fold_db_core::pending_task_tracker::{PendingTask, PendingTaskTracker};

const CHANGE_FEED_QUEUE_CAPACITY: usize = 1_024;

pub(crate) struct ChangeFeedQueue {
    sender: tokio::sync::mpsc::Sender<ChangeFeedJob>,
    pending_tasks: Arc<PendingTaskTracker>,
}

impl ChangeFeedQueue {
    pub(crate) fn new(store: ChangeFeedStore, pending_tasks: Arc<PendingTaskTracker>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(CHANGE_FEED_QUEUE_CAPACITY);
        let worker_gate = Arc::new(tokio::sync::Semaphore::new(1));
        // Process-lifetime ordered projection lane. Jobs own PendingTask
        // guards, so FoldDB shutdown drains submitted feed work before flush.
        tokio::spawn(run_change_feed_queue(
            receiver,
            store,
            Arc::clone(&worker_gate),
        ));
        Self {
            sender,
            pending_tasks,
        }
    }

    pub(crate) fn try_reserve(&self) -> Result<ChangeFeedReservation, &'static str> {
        let permit = self.sender.clone().try_reserve_owned().map_err(|error| {
            if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                "durable change-feed queue is closed; retry before mutation commit"
            } else {
                "durable change-feed queue is full; retry before mutation commit"
            }
        })?;
        Ok(ChangeFeedReservation {
            permit,
            pending_tasks: Arc::clone(&self.pending_tasks),
        })
    }
}

pub(crate) struct ChangeFeedReservation {
    permit: tokio::sync::mpsc::OwnedPermit<ChangeFeedJob>,
    pending_tasks: Arc<PendingTaskTracker>,
}

impl ChangeFeedReservation {
    pub(crate) fn submit(self, events: Vec<ChangeFeedEvent>) {
        let pending_task = self.pending_tasks.begin();
        let _sender = self.permit.send(ChangeFeedJob {
            events,
            _pending_task: pending_task,
        });
    }
}

struct ChangeFeedJob {
    events: Vec<ChangeFeedEvent>,
    _pending_task: PendingTask,
}

async fn run_change_feed_queue(
    mut receiver: tokio::sync::mpsc::Receiver<ChangeFeedJob>,
    store: ChangeFeedStore,
    worker_gate: Arc<tokio::sync::Semaphore>,
) {
    while let Some(job) = receiver.recv().await {
        let Ok(worker_permit) = worker_gate.acquire().await else {
            return;
        };
        for event in job.events {
            if let Err(error) = store.append(event).await {
                tracing::error!(
                    error = %error,
                    "failed to append queued durable app change-feed event"
                );
            }
        }
        drop(worker_permit);
    }
}
