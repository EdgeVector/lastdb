//! Durable mutation author-clock allocation and remote observation.

use crate::clock::unix_nanos;
use std::sync::Arc;

use crate::schema::mutation_author::{sign_mutation_author_clock, verify_mutation_author_clock};
use crate::schema::types::{Mutation, MutationAuthorClockState};
use crate::schema::SchemaError;

use crate::db_operations::DbOperations;
use crate::fold_db_core::pending_task_tracker::{PendingTask, PendingTaskTracker};

use super::MutationManager;

mod coverage;

const AUTHOR_CLOCK_QUEUE_CAPACITY: usize = 1_024;
const AUTHOR_CLOCK_RETRY_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_millis(10);
const AUTHOR_CLOCK_RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
/// How long a capacity refusal is retried. The maintenance worker makes room in
/// that time when it runs. After it, the waiting requests get the named error,
/// so a node whose worker is off answers with a retry hint instead of a hang.
const AUTHOR_CLOCK_CAPACITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

pub(super) struct AuthorClockPersistQueue {
    sender: tokio::sync::mpsc::Sender<AuthorClockPersistJob>,
    pending_tasks: Arc<PendingTaskTracker>,
    coverage: Option<Arc<coverage::ClockCoverage>>,
}

impl AuthorClockPersistQueue {
    pub(super) fn new(
        db_ops: Arc<DbOperations>,
        state_key: String,
        initial_state: MutationAuthorClockState,
        pending_tasks: Arc<PendingTaskTracker>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(AUTHOR_CLOCK_QUEUE_CAPACITY);
        let coverage = db_ops
            .namespaced_store()
            .disk_pack_runtime()
            .map(|_| Arc::new(coverage::ClockCoverage::default()));
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
            coverage,
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
            coverage: self.coverage.clone(),
        })
    }
}

pub(super) struct AuthorClockPersistReservation {
    permit: tokio::sync::mpsc::OwnedPermit<AuthorClockPersistJob>,
    pending_tasks: Arc<PendingTaskTracker>,
    coverage: Option<Arc<coverage::ClockCoverage>>,
}

/// A cloneable proof that the author clock for one request is durable.
///
/// Each schema lane holds a clone. No lane starts durable data work until the
/// metadata queue publishes this proof.
#[derive(Clone)]
pub(crate) struct AuthorClockPersistBarrier {
    completion: tokio::sync::watch::Receiver<ClockOutcome>,
}

/// What the metadata queue decided for one queued state.
#[derive(Clone)]
enum ClockOutcome {
    Pending,
    Durable,
    /// The home had no part capacity for the state for the whole retry window.
    /// The request that waits on it fails with the named capacity error. No
    /// data work started, so the failure leaves only a counter gap.
    Refused(String),
}

impl AuthorClockPersistBarrier {
    pub(crate) async fn wait(&self) -> Result<(), SchemaError> {
        let mut completion = self.completion.clone();
        loop {
            let outcome = completion.borrow_and_update().clone();
            match outcome {
                ClockOutcome::Durable => return Ok(()),
                ClockOutcome::Refused(detail) => {
                    return Err(SchemaError::WriteResourceLimit { detail })
                }
                ClockOutcome::Pending => {}
            }
            completion.changed().await.map_err(|_| {
                SchemaError::InvalidData(
                    "mutation author-clock queue stopped before durable completion".to_string(),
                )
            })?;
        }
    }

    /// True when the queue refused this state. A refused barrier must not cover later requests.
    fn refused(&self) -> bool {
        matches!(&*self.completion.borrow(), ClockOutcome::Refused(_))
    }
}

impl AuthorClockPersistReservation {
    pub(super) fn submit_with_barrier(
        self,
        state: MutationAuthorClockState,
    ) -> AuthorClockPersistBarrier {
        if let Some(coverage) = self.coverage.clone() {
            return coverage.barrier(state, |reserved| self.submit_uncached(reserved));
        }
        self.submit_uncached(state)
    }

    fn submit_uncached(self, state: MutationAuthorClockState) -> AuthorClockPersistBarrier {
        let (completion, receiver) = tokio::sync::watch::channel(ClockOutcome::Pending);
        self.submit_inner(state, Some(completion));
        AuthorClockPersistBarrier {
            completion: receiver,
        }
    }

    fn submit_inner(
        self,
        state: MutationAuthorClockState,
        completion: Option<tokio::sync::watch::Sender<ClockOutcome>>,
    ) {
        let Self {
            permit,
            pending_tasks,
            coverage: _,
        } = self;
        let pending_task = pending_tasks.begin();
        let _sender = permit.send(AuthorClockPersistJob {
            state,
            _pending_task: pending_task,
            completion,
            mutation_admission: crate::fold_db_core::admission::current_mutation_admission(),
        });
    }
}

struct AuthorClockPersistJob {
    state: MutationAuthorClockState,
    _pending_task: PendingTask,
    completion: Option<tokio::sync::watch::Sender<ClockOutcome>>,
    mutation_admission: Option<crate::fold_db_core::admission::MutationAdmission>,
}

async fn persist_author_clock(
    db_ops: &DbOperations,
    state_key: &str,
    state: &MutationAuthorClockState,
) -> Result<(), SchemaError> {
    if let Some(runtime) = db_ops.namespaced_store().disk_pack_runtime() {
        return crate::storage::disk_pack::transaction::request_scope(Some(runtime), async {
            let mut durable = db_ops
                .metadata()
                .get_typed::<MutationAuthorClockState>(state_key)
                .await?
                .unwrap_or_default();
            durable.observe_remote(state.physical_nanos, state.logical_counter);
            db_ops.metadata().put_typed(state_key, &durable).await?;
            Ok::<(), SchemaError>(())
        })
        .await;
    }
    db_ops.metadata().put_typed_durable(state_key, state).await
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
        let capacity_window_start = std::time::Instant::now();
        let mutation_admission = jobs.iter().find_map(|job| job.mutation_admission.clone());
        let outcome = loop {
            let persist = persist_author_clock(&db_ops, &state_key, &high_water);
            let persist = crate::fold_db_core::admission::with_existing_mutation_admission(
                mutation_admission.clone(),
                persist,
            );
            match persist.await {
                Ok(()) => break ClockOutcome::Durable,
                Err(SchemaError::WriteResourceLimit { detail })
                    if capacity_window_start.elapsed() >= AUTHOR_CLOCK_CAPACITY_WINDOW =>
                {
                    tracing::warn!(
                        %detail,
                        attempt,
                        "mutation author clock refused for part capacity; failing its requests"
                    );
                    break ClockOutcome::Refused(detail);
                }
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
        };
        // A durable state covers every barrier in this batch, and a transient
        // failure cannot report a false drain. A refused state stays in the
        // high-water mark, so the next durable write covers it too.
        for job in jobs {
            if let Some(completion) = job.completion {
                let _ = completion.send(outcome.clone());
            }
            drop(job._pending_task);
        }
    }
}

impl MutationManager {
    /// Stamp local mutations in caller order and observe imported author clocks.
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
        let local_origin_device = self.db_ops.atoms().local_origin_device();
        let mut resident_state = self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = *resident_state;
        let mut changed = false;

        // Observe imported clocks before reserving consecutive local write
        // times in caller order. Local mutations keep one batch counter, so
        // the receipt remains their common revision. Their distinct times
        // prevent a repeated key from choosing its winner by random UUID.
        // Imported signatures and clocks remain unchanged.
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

        let local_count = mutations
            .iter()
            .filter(|mutation| mutation.imported_written_at.is_none())
            .count();
        let mut local_clock = if local_count == 0 {
            None
        } else {
            Some(
                state
                    .advance_local_batch(unix_nanos(), local_count)
                    .ok_or_else(|| {
                        SchemaError::InvalidData("mutation author-clock range exhausted".into())
                    })?,
            )
        };
        for mutation in mutations {
            if mutation.imported_written_at.is_some() {
                continue;
            }
            let (times, logical_counter) = local_clock.as_mut().expect("reserved local clocks");
            let written_at = times.next().expect("one reserved time per local mutation");
            mutation.imported_written_at = Some(written_at);
            mutation.logical_counter = *logical_counter;
            mutation.author_clock_writer_id = local_writer_id.clone();
            mutation.origin_device = local_origin_device.clone();
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
}
