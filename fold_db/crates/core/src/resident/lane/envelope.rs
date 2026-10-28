//! Persist envelopes, batch outcomes and the writer trait for lane workers.

use std::time::Instant;

use async_trait::async_trait;

/// Lane identity: one FIFO per schema instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PersistLaneKey {
    pub storage_prefix: String,
    pub schema_name: String,
}

impl PersistLaneKey {
    pub fn new(storage_prefix: impl Into<String>, schema_name: impl Into<String>) -> Self {
        Self {
            storage_prefix: storage_prefix.into(),
            schema_name: schema_name.into(),
        }
    }
}

/// Background work with reserved queue capacity and completion waiters.
///
/// The current logical engine uses these lanes for purge work. Its accepted
/// value changes belong to the core model and the native disk queue.
/// Optional `completion` and `after_release` run only after the
/// lane drops this envelope's byte charge. A send from inside `write_batch`
/// wakes the next reserve while the charge is still held.
pub struct PersistEnvelope<T> {
    pub commit_id: u64,
    pub bytes: u64,
    pub enqueued_at: Instant,
    pub completion: Option<tokio::sync::oneshot::Sender<()>>,
    /// Runs on the lane worker after admission for this envelope is released.
    pub after_release: Option<Box<dyn FnOnce() + Send>>,
    pub payload: T,
}

/// Result of one persist-lane batch write.
///
/// A writer returns ownership of every failed or untried envelope in original
/// commit order. The lane retries that suffix before any later envelope. This
/// ownership-return contract does not copy a payload.
#[must_use]
pub enum PersistBatchOutcome<T> {
    /// Every envelope is durable, and the lane still owns the waiters.
    ///
    /// The lane releases the byte charge, then runs `after_release` and sends
    /// `completion`. A writer must not send those waiters itself.
    Released(Vec<PersistEnvelope<T>>),
    /// The writer could not make `remaining` durable.
    ///
    /// `remaining` must contain only envelopes from the input batch. It must
    /// contain the first failed envelope and every untried envelope.
    /// `finished` is the durable prefix. The lane notifies it after the
    /// failed suffix is back on the FIFO.
    Retry {
        remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: String,
    },
    /// The first envelope of `remaining` failed with a deterministic error.
    ///
    /// A deterministic error does not change on retry, for example a durable
    /// row that does not decode. The lane retries the head a few times, then
    /// hands it to [`PersistLaneWriter::quarantine`] and continues with the
    /// rest. `remaining` has the same contract as [`Self::Retry`].
    Poisoned {
        remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: String,
    },
}

impl<T> PersistEnvelope<T> {
    pub fn new(payload: T, bytes: u64) -> Self {
        Self {
            commit_id: 0,
            bytes,
            enqueued_at: Instant::now(),
            completion: None,
            after_release: None,
            payload,
        }
    }
}

/// Wake durable waiters after their byte charge is gone.
pub(super) fn notify_released<T>(envelopes: Vec<PersistEnvelope<T>>) {
    for mut envelope in envelopes {
        if let Some(hook) = envelope.after_release.take() {
            hook();
        }
        if let Some(tx) = envelope.completion.take() {
            let _ = tx.send(());
        }
    }
}

/// Why [`PersistLaneSet::reserve`] refused queue capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistLaneFull {
    Entries,
    Bytes,
    /// The lane rejected admission after repeated durable-store failures.
    Unhealthy,
}

/// Writer invoked by a lane worker with one or more adjacent envelopes.
#[async_trait]
pub trait PersistLaneWriter<T: Send + 'static>: Send + Sync + 'static {
    async fn write_batch(&self, batch: Vec<PersistEnvelope<T>>) -> PersistBatchOutcome<T>;

    /// Preserve one poisoned envelope outside the lane and release its turn.
    ///
    /// `Ok(())` means the writer kept a durable record of the envelope and
    /// released every resource that later envelopes wait on. `Err` gives the
    /// envelope back. The lane then keeps it at the head and uses the normal
    /// retry and unhealthy path. The default refuses: a writer that cannot
    /// preserve an envelope must never lose one.
    async fn quarantine(
        &self,
        envelope: PersistEnvelope<T>,
        _error: &str,
    ) -> Result<(), PersistEnvelope<T>> {
        Err(envelope)
    }
}
