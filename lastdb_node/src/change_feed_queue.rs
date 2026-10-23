//! Bounded ordered persistence for app change-feed hints.
//!
//! Product requests enter the accepted order before their feed jobs. The feed
//! is a later coordination projection, so its durable result must not delay a
//! memory acknowledgement. Native jobs can share the product flush deadline.
//! A caller reserves one bounded queue slot before product commit. A full or
//! closed queue therefore rejects before any product state changes.

use std::sync::Arc;

use fold_db::db_operations::{ChangeFeedEvent, ChangeFeedStore};
use fold_db::fold_db_core::pending_task_tracker::{PendingTask, PendingTaskTracker};

const CHANGE_FEED_QUEUE_CAPACITY: usize = 1_024;
const CHANGE_FEED_BATCH_EVENTS: usize = 1_024;
const CHANGE_FEED_BATCH_BYTES: usize = 32 * 1024 * 1024;

pub(crate) struct ChangeFeedQueue {
    sender: tokio::sync::mpsc::Sender<ChangeFeedJob>,
    pending_tasks: Arc<PendingTaskTracker>,
}

impl ChangeFeedQueue {
    pub(crate) fn new(store: ChangeFeedStore, pending_tasks: Arc<PendingTaskTracker>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(CHANGE_FEED_QUEUE_CAPACITY);
        // Process-lifetime ordered projection lane. Jobs own PendingTask
        // guards, so FoldDB shutdown drains submitted feed work before flush.
        tokio::spawn(run_change_feed_queue(receiver, store));
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
        if events.is_empty() {
            return;
        }
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
) {
    let mut carry = None;
    loop {
        let first = match carry.take() {
            Some(job) => job,
            None => match receiver.recv().await {
                Some(job) => job,
                None => return,
            },
        };
        let batch = FeedBatch::collect(first, &mut receiver);
        carry = batch.carry;
        if let Err(error) = store.append_many(batch.events).await {
            tracing::error!(
                error = %error,
                "failed to append queued durable app change-feed batch"
            );
        }
        // Every original job remains owned until the combined durable result.
        drop(batch.tasks);
    }
}

struct FeedBatch {
    events: Vec<ChangeFeedEvent>,
    tasks: Vec<PendingTask>,
    carry: Option<ChangeFeedJob>,
}

impl FeedBatch {
    fn collect(
        first: ChangeFeedJob,
        receiver: &mut tokio::sync::mpsc::Receiver<ChangeFeedJob>,
    ) -> Self {
        let mut bytes = job_bytes(&first);
        let mut batch = Self {
            events: first.events,
            tasks: vec![first._pending_task],
            carry: None,
        };
        // Snapshot the backlog. Later producers cannot extend this bounded pass.
        let queued = receiver.len().min(CHANGE_FEED_QUEUE_CAPACITY - 1);
        for _ in 0..queued {
            let Ok(job) = receiver.try_recv() else { break };
            let length = job_bytes(&job);
            if batch.events.len().saturating_add(job.events.len()) > CHANGE_FEED_BATCH_EVENTS
                || bytes.saturating_add(length) > CHANGE_FEED_BATCH_BYTES
            {
                batch.carry = Some(job);
                break;
            }
            bytes += length;
            batch.events.extend(job.events);
            batch.tasks.push(job._pending_task);
        }
        batch
    }
}

fn job_bytes(job: &ChangeFeedJob) -> usize {
    job.events.iter().fold(0usize, |total, event| {
        let text = event
            .mutation_id
            .len()
            .saturating_add(event.schema.len())
            .saturating_add(event.operation.len())
            .saturating_add(event.hash.as_ref().map_or(0, String::len))
            .saturating_add(event.range.as_ref().map_or(0, String::len));
        // Six bytes cover the largest JSON escape for each input byte.
        total
            .saturating_add(256)
            .saturating_add(text.saturating_mul(6))
    })
}
