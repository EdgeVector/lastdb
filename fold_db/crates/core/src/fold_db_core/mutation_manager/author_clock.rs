//! Durable mutation author-clock allocation and remote observation.

use crate::clock::unix_nanos;
use std::sync::Arc;

use crate::schema::types::author_clock::{
    sign_mutation_author_clock, verify_mutation_author_clock,
};
use crate::schema::types::{Mutation, MutationAuthorClockState};
use crate::schema::SchemaError;

use crate::db_operations::DbOperations;
use crate::fold_db_core::pending_task_tracker::{PendingTask, PendingTaskTracker};

use super::MutationManager;

const AUTHOR_CLOCK_QUEUE_CAPACITY: usize = 1_024;
const AUTHOR_CLOCK_RETRY_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_millis(10);
const AUTHOR_CLOCK_RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

pub(super) struct AuthorClockPersistQueue {
    sender: tokio::sync::mpsc::Sender<AuthorClockPersistJob>,
    pending_tasks: Arc<PendingTaskTracker>,
}

impl AuthorClockPersistQueue {
    pub(super) fn new(
        db_ops: Arc<DbOperations>,
        state_key: String,
        initial_state: MutationAuthorClockState,
        pending_tasks: Arc<PendingTaskTracker>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(AUTHOR_CLOCK_QUEUE_CAPACITY);
        // Process-lifetime ordered metadata lane. Each queued item owns a
        // PendingTask, so graceful shutdown waits for every submitted state.
        tokio::spawn(run_author_clock_persist_queue(
            receiver,
            db_ops,
            state_key,
            initial_state,
        ));
        Self {
            sender,
            pending_tasks,
        }
    }

    fn try_reserve(&self, schema: &str) -> Result<AuthorClockPersistReservation, SchemaError> {
        let permit = self.sender.clone().try_reserve_owned().map_err(|error| {
            SchemaError::PersistQueueFull {
                schema: schema.to_string(),
                kind: if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                    "author_clock_closed".to_string()
                } else {
                    "author_clock_full".to_string()
                },
            }
        })?;
        Ok(AuthorClockPersistReservation {
            permit,
            pending_tasks: Arc::clone(&self.pending_tasks),
        })
    }
}

pub(super) struct AuthorClockPersistReservation {
    permit: tokio::sync::mpsc::OwnedPermit<AuthorClockPersistJob>,
    pending_tasks: Arc<PendingTaskTracker>,
}

/// A cloneable proof that the author clock for one request is durable.
///
/// Each schema lane holds a clone. No lane starts durable data work until the
/// metadata queue publishes this proof.
#[derive(Clone)]
pub(crate) struct AuthorClockPersistBarrier {
    completion: tokio::sync::watch::Receiver<bool>,
}

impl AuthorClockPersistBarrier {
    pub(crate) async fn wait(&self) -> Result<(), SchemaError> {
        let mut completion = self.completion.clone();
        loop {
            if *completion.borrow_and_update() {
                return Ok(());
            }
            completion.changed().await.map_err(|_| {
                SchemaError::InvalidData(
                    "mutation author-clock queue stopped before durable completion".to_string(),
                )
            })?;
        }
    }
}

impl AuthorClockPersistReservation {
    pub(super) async fn submit_and_wait(
        self,
        state: MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        self.submit_with_barrier(state).wait().await
    }

    pub(super) fn submit_with_barrier(
        self,
        state: MutationAuthorClockState,
    ) -> AuthorClockPersistBarrier {
        let (completion, receiver) = tokio::sync::watch::channel(false);
        self.submit_inner(state, Some(completion));
        AuthorClockPersistBarrier {
            completion: receiver,
        }
    }

    fn submit_inner(
        self,
        state: MutationAuthorClockState,
        completion: Option<tokio::sync::watch::Sender<bool>>,
    ) {
        let Self {
            permit,
            pending_tasks,
        } = self;
        let pending_task = pending_tasks.begin();
        let _sender = permit.send(AuthorClockPersistJob {
            state,
            _pending_task: pending_task,
            completion,
            #[cfg(feature = "cloud-sync")]
            mutation_admission: crate::sync::capture::current_mutation_admission(),
        });
    }
}

struct AuthorClockPersistJob {
    state: MutationAuthorClockState,
    _pending_task: PendingTask,
    completion: Option<tokio::sync::watch::Sender<bool>>,
    #[cfg(feature = "cloud-sync")]
    mutation_admission: Option<crate::sync::capture::MutationAdmission>,
}

async fn run_author_clock_persist_queue(
    mut receiver: tokio::sync::mpsc::Receiver<AuthorClockPersistJob>,
    db_ops: Arc<DbOperations>,
    state_key: String,
    mut high_water: MutationAuthorClockState,
) {
    while let Some(first_job) = receiver.recv().await {
        let mut jobs = vec![first_job];
        // Snapshot the backlog. New producers cannot keep this drain loop live
        // without a durable write.
        let queued = receiver
            .len()
            .min(AUTHOR_CLOCK_QUEUE_CAPACITY.saturating_sub(1));
        for _ in 0..queued {
            match receiver.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        // Requests can finish resident commit out of allocation order. Fold
        // every available state into one high-water write. One durable maximum
        // satisfies every barrier in this batch and avoids one flush per row.
        for job in &jobs {
            high_water.observe_remote(job.state.physical_nanos, job.state.logical_counter);
        }
        let mut delay = AUTHOR_CLOCK_RETRY_INITIAL_DELAY;
        let mut attempt = 1_u64;
        #[cfg(feature = "cloud-sync")]
        let mutation_admission = jobs.iter().find_map(|job| job.mutation_admission.clone());
        loop {
            let persist = db_ops.metadata().put_typed_durable(&state_key, &high_water);
            #[cfg(feature = "cloud-sync")]
            let persist = crate::sync::capture::with_existing_mutation_admission(
                mutation_admission.clone(),
                persist,
            );
            match persist.await {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        attempt,
                        retry_delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "failed to persist queued mutation author clock; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(AUTHOR_CLOCK_RETRY_MAX_DELAY);
                    attempt = attempt.saturating_add(1);
                }
            }
        }
        // Keep each shutdown task live until a durable high-water state covers
        // its clock. A transient failure cannot report a false drain.
        for job in jobs {
            if let Some(completion) = job.completion {
                let _ = completion.send(true);
            }
            drop(job._pending_task);
        }
    }
}

impl MutationManager {
    /// Stamp one local operation clock and observe imported author clocks.
    ///
    /// The resident state and a bounded persist slot are reserved before
    /// mutation apply. The caller submits the state before resident apply and
    /// gives its durable barrier to each schema lane. A failed request can
    /// leave a resident counter gap, but it cannot reuse or rewind a counter
    /// within this process. Graceful shutdown drains the queued state and
    /// flushes storage.
    pub(super) fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
    ) -> Result<Option<(AuthorClockPersistReservation, MutationAuthorClockState)>, SchemaError>
    {
        if mutations.is_empty() {
            return Ok(None);
        }
        let schema = mutations[0].schema_name.as_str();
        let reservation = self
            .author_clock_persist
            .as_ref()
            .ok_or_else(|| {
                SchemaError::InvalidData(
                    "mutation author-clock persistence is unavailable".to_string(),
                )
            })?
            .try_reserve(schema)?;
        let local_writer_id = self.signer.public_key_base64();
        let mut resident_state = self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = *resident_state;
        let mut changed = false;

        // One caller batch is one logical operation. Observe every imported
        // clock first, then allocate one local clock value for all local
        // mutations in the operation. The mutation id remains the stable
        // per-item tie-break and stays covered by each signature.
        for mutation in mutations.iter() {
            if let Some(written_at) = mutation.imported_written_at {
                if !verify_mutation_author_clock(mutation) {
                    return Err(SchemaError::InvalidData(format!(
                        "invalid mutation author-clock signature for {}",
                        mutation.uuid
                    )));
                }
                let before = state;
                state.observe_remote(written_at, mutation.logical_counter);
                changed |= state != before;
            }
        }

        let has_local = mutations
            .iter()
            .any(|mutation| mutation.imported_written_at.is_none());
        let local_clock = has_local.then(|| state.advance_local(unix_nanos()));
        for mutation in mutations {
            if mutation.imported_written_at.is_some() {
                continue;
            }
            let (written_at, logical_counter) =
                local_clock.expect("local clock exists when a local mutation exists");
            mutation.imported_written_at = Some(written_at);
            mutation.logical_counter = logical_counter;
            mutation.author_clock_writer_id = local_writer_id.clone();
            mutation.author_clock_signature_version =
                crate::schema::types::MUTATION_AUTHOR_SIGNATURE_VERSION;
            mutation.author_clock_signature =
                sign_mutation_author_clock(mutation, self.signer.as_ref());
            changed = true;
        }

        if changed {
            *resident_state = state;
        }
        // Every accepted nonempty batch gets a barrier, even when an imported
        // clock does not advance the resident high-water value. A prior batch
        // can own the same in-memory value while its metadata retry is still
        // pending. This queued monotonic write keeps later data behind that
        // durable value.
        Ok(Some((reservation, state)))
    }

    /// Observe replay clocks without changing the received signed values.
    #[cfg(any(feature = "cloud-sync", test))]
    pub(super) fn observe_replayed_author_clocks(
        &self,
        mutations: &[Mutation],
    ) -> Result<Option<(AuthorClockPersistReservation, MutationAuthorClockState)>, SchemaError>
    {
        let mut copies = mutations.to_vec();
        self.prepare_mutation_author_clocks(&mut copies)
    }
}
