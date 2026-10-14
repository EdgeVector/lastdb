//! The erase step of the TTL sweep: which verb, one admission per batch, and
//! the rest between batches.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::request_phases::{run_with_phases, RequestPhaseTotals};
use fold_db::schema::types::operations::{Mutation, MutationType};
use fold_db::schema::types::KeyValue;

use super::pace::Pace;
use super::{backup_cut_is_held, owner_context};
use crate::host::Host;

/// Environment name: `purge` (default) or `delete`. See [`SweepVerb`].
pub const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

/// How the sweep erases an expired key.
///
/// `purge` (default) is the hard-erasure verb. It deletes the atoms of the key
/// that no other key references. That needs a reachability proof over the whole
/// schema, unless every molecule has a complete reverse-reference manifest.
/// `delete` is the ordinary delete that a client uses: it removes the tips and
/// leaves atom bodies to garbage collection, and needs only the keys of the
/// batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepVerb {
    Purge,
    Delete,
}

impl SweepVerb {
    fn from_env() -> Self {
        match std::env::var(TTL_SWEEP_VERB_ENV)
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some(raw) if raw.eq_ignore_ascii_case("delete") => Self::Delete,
            _ => Self::Purge,
        }
    }

    /// Keys per batch when `LASTDB_TTL_SWEEP_BATCH` is unset. A purge pays a
    /// proof that spans the whole schema for each batch, so it keeps the
    /// largest batch. A delete pays for its own keys only, so it keeps a small
    /// batch and a short stall.
    fn default_batch(self) -> usize {
        match self {
            Self::Purge => 64,
            Self::Delete => 16,
        }
    }

    fn mutation_type(self) -> MutationType {
        match self {
            Self::Purge => MutationType::Purge,
            Self::Delete => MutationType::Delete,
        }
    }

    /// A plain delete acknowledges after its resident tombstone, and the
    /// persist lane erases later. The sweep waits for the lane, so a batch is
    /// done when the call returns and the pacing sees the real batch time.
    fn waits_for_lane(self) -> bool {
        self == Self::Delete
    }
}

/// Erase `expired` in small batches, one admission per batch, with a rest
/// between batches. Stops early, without an error, when the node pauses local
/// writes or holds a backup cut. The next sampler tick selects the rest again.
pub(super) async fn purge_paced(
    host: &Host,
    schema_name: &str,
    expired: &[KeyValue],
    shutdown: &AtomicBool,
) -> Result<usize, String> {
    let verb = SweepVerb::from_env();
    let pace = Pace::from_env(verb.default_batch());
    let mut removed = 0usize;
    let mut chunks = expired.chunks(pace.batch).peekable();
    while let Some(chunk) = chunks.next() {
        if shutdown.load(Ordering::Acquire) || backup_cut_is_held(host).await {
            break;
        }
        let started = Instant::now();
        let admitted = crate::maintenance_write_gate::run(
            host,
            run_with_phases(purge_keys(host, schema_name, chunk, verb)),
        )
        .await;
        let Some((outcome, phases, _counts)) = admitted else {
            break;
        };
        outcome?;
        let took = started.elapsed();
        removed += chunk.len();
        log_batch(schema_name, chunk.len(), took, &phases);
        if chunks.peek().is_some() {
            rest(pace.pause_after(took), shutdown).await;
        }
    }
    Ok(removed)
}

/// Rest for `pause`, and wake early when the node starts to stop.
async fn rest(pause: Duration, shutdown: &AtomicBool) {
    const SLICE: Duration = Duration::from_millis(250);
    let until = tokio::time::Instant::now() + pause;
    while !shutdown.load(Ordering::Acquire) {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        tokio::time::sleep(left.min(SLICE)).await;
    }
}

/// One line per batch with the erase phases, for the operator who asks where a
/// batch spent its time. Phase times are in milliseconds.
fn log_batch(schema_name: &str, keys: usize, took: Duration, phases: &RequestPhaseTotals) {
    tracing::debug!(
        target: "lastdbd::ttl_sweep",
        schema = schema_name,
        keys,
        took_ms = took.as_millis() as u64,
        materialize_ms = phases.purge_materialize_us / 1000,
        trace_ms = phases.purge_trace_us / 1000,
        guard_ms = phases.purge_retention_guard_us / 1000,
        delete_ms = phases.purge_delete_us / 1000,
        finalize_ms = phases.purge_finalize_us / 1000,
        other_commit_ms = phases.purge_commit_us / 1000,
        barrier_wait_ms = phases.purge_barrier_us / 1000,
        "ttl sweep batch erased"
    );
}

async fn purge_keys(
    host: &Host,
    schema_name: &str,
    keys: &[KeyValue],
    verb: SweepVerb,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let mutations: Vec<Mutation> = keys
        .iter()
        .map(|key| {
            let mut mutation = Mutation::new(
                schema_name.to_string(),
                HashMap::new(),
                key.clone(),
                host.public_key(),
                verb.mutation_type(),
            );
            mutation.synchronous = verb.waits_for_lane().then_some(true);
            mutation
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations_with_access(mutations, &owner_context(host))
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
