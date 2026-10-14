//! The erase step of the TTL sweep: which verb, a re-check of each key before
//! its batch, one admission per batch, one batch per schema in turn, and the
//! rest between batches.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::time::{Duration, Instant};

use fold_db::clock::unix_secs;
use fold_db::request_phases::{run_with_phases, RequestPhaseTotals};
use fold_db::schema::types::operations::{Mutation, MutationType};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

use super::backlog::Backlog;
use super::pace::Pace;
use super::{backup_cut_is_held, key_is_expired, owner_context};
use crate::host::Host;

/// Environment name: `delete` (default) or `purge`. See [`SweepVerb`].
pub const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

/// The delete ledger caller of a TTL sweep `delete`. A client delete carries
/// the caller `mutation-pipeline`.
const LEDGER_CALLER: &str = "ttl-sweep-delete";

/// How the sweep erases an expired key.
///
/// `delete` (default) is the ordinary delete that a client uses. It removes the
/// tips, needs only the keys of the batch, and leaves atom bodies to garbage
/// collection. It does not touch history rows or conflict records. `purge` is
/// the hard-erasure verb. It also deletes the atoms of the key that no other
/// key references. That needs a reachability proof over the whole schema,
/// unless every molecule has a complete reverse-reference manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepVerb {
    Delete,
    Purge,
}

impl SweepVerb {
    /// The verb that the node environment names. A value that is neither
    /// `delete` nor `purge` gives the default, and [`warn_on_bad_verb_once`]
    /// says so in the log.
    fn from_env() -> Self {
        match std::env::var(TTL_SWEEP_VERB_ENV)
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some(raw) if raw.eq_ignore_ascii_case("purge") => Self::Purge,
            _ => Self::Delete,
        }
    }

    fn mutation_type(self) -> MutationType {
        match self {
            Self::Delete => MutationType::Delete,
            Self::Purge => MutationType::Purge,
        }
    }

    /// A plain delete acknowledges after its resident tombstone, and the
    /// persist lane erases later. The sweep waits for the lane, so a batch is
    /// done when the call returns and the pacing sees the real batch time.
    fn waits_for_lane(self) -> bool {
        self == Self::Delete
    }
}

/// Log one warning, at the first sampler tick, when the node environment names
/// a verb that the sweep does not know. The sweep then uses `delete`.
pub(super) fn warn_on_bad_verb_once() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        let Ok(raw) = std::env::var(TTL_SWEEP_VERB_ENV) else {
            return;
        };
        let value = raw.trim();
        if !value.is_empty()
            && !value.eq_ignore_ascii_case("delete")
            && !value.eq_ignore_ascii_case("purge")
        {
            tracing::warn!(
                target: "lastdbd::ttl_sweep",
                value,
                "unknown {TTL_SWEEP_VERB_ENV} value; the ttl sweep uses the default verb `delete` (valid: delete, purge)"
            );
        }
    });
}

/// What one pass did.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PassTotals {
    /// Keys that a batch erased.
    pub(super) erased: usize,
    /// Keys that the re-check dropped because they were no longer expired.
    pub(super) dropped: usize,
}

/// Erase the backlogs in rounds: one batch from each schema that still has
/// keys, then the rest, then the next round. Stops early, without an error,
/// when the node stops, pauses local writes or holds a backup cut. The next
/// sampler tick lists the rest again.
pub(super) async fn purge_round_robin(
    host: &Host,
    mut backlogs: Vec<Backlog>,
    shutdown: &AtomicBool,
) -> Result<PassTotals, String> {
    let verb = SweepVerb::from_env();
    let pace = Pace::from_env();
    let mut totals = PassTotals::default();
    while backlogs.iter().any(|backlog| !backlog.is_done()) {
        for index in 0..backlogs.len() {
            if backlogs[index].is_done() {
                continue;
            }
            if shutdown.load(Ordering::Acquire) || backup_cut_is_held(host).await {
                return Ok(totals);
            }
            let chunk = backlogs[index].take(pace.batch);
            let started = Instant::now();
            let admitted = crate::maintenance_write_gate::run(
                host,
                run_with_phases(erase_batch(host, &mut backlogs[index], chunk, verb)),
            )
            .await;
            let Some((outcome, phases, _counts)) = admitted else {
                return Ok(totals);
            };
            let (erased, dropped) = outcome?;
            let took = started.elapsed();
            totals.erased += erased;
            totals.dropped += dropped;
            log_batch(&backlogs[index].schema_name, erased, dropped, took, &phases);
            if backlogs.iter().any(|backlog| !backlog.is_done()) {
                rest(pace.pause_after(took), shutdown).await;
            }
        }
    }
    Ok(totals)
}

/// Re-check `chunk`, then erase the keys that are still expired. Returns the
/// count erased and the count dropped.
async fn erase_batch(
    host: &Host,
    backlog: &mut Backlog,
    chunk: Vec<KeyValue>,
    verb: SweepVerb,
) -> Result<(usize, usize), String> {
    let listed = chunk.len();
    let Some(keys) = still_expired(host, backlog, chunk).await? else {
        backlog.clear();
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = %backlog.schema_name,
            "retention policy is gone; the ttl sweep drops the rest of this schema"
        );
        return Ok((0, listed));
    };
    let dropped = listed - keys.len();
    purge_keys(host, &backlog.schema_name, &keys, verb).await?;
    Ok((keys.len(), dropped))
}

/// The keys of `chunk` that are expired now, under the policy now. `None`
/// means the policy is gone.
///
/// The pass lists its keys once. A paced pass reaches a key long after the
/// listing, and a client can rewrite the key in that time. Range and HashRange
/// keys carry their age in the key, so a rewrite does not change it. Hash and
/// Single keys carry it in the write-time index, which the re-check reads in
/// one batch. A policy that now has a longer TTL protects keys that it
/// protected not when the pass listed them.
async fn still_expired(
    host: &Host,
    backlog: &Backlog,
    chunk: Vec<KeyValue>,
) -> Result<Option<Vec<KeyValue>>, String> {
    let policy = host
        .db
        .db_ops()
        .get_schema_retention_policy(&backlog.schema_name)
        .await
        .map_err(|e| e.to_string())?;
    let Some(policy) = policy else {
        return Ok(None);
    };
    let cutoff = backlog
        .cutoff
        .min(unix_secs().saturating_sub(policy.ttl_seconds));
    match backlog.schema_type {
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => host
            .db
            .db_ops()
            .schemas()
            .retention_keys_still_expired(&backlog.schema_name, &chunk, cutoff)
            .await
            .map(Some)
            .map_err(|e| e.to_string()),
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange => Ok(Some(
            chunk
                .into_iter()
                .filter(|key| key_is_expired(key, &backlog.schema_type, cutoff))
                .collect(),
        )),
    }
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
/// batch spent its time. Phase times are in milliseconds. A batch that dropped
/// keys at the re-check is logged at info.
fn log_batch(
    schema_name: &str,
    erased: usize,
    dropped: usize,
    took: Duration,
    phases: &RequestPhaseTotals,
) {
    if dropped > 0 {
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = schema_name,
            dropped,
            erased,
            "ttl sweep re-check dropped keys that are no longer expired"
        );
    }
    tracing::debug!(
        target: "lastdbd::ttl_sweep",
        schema = schema_name,
        keys = erased,
        dropped,
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
            if verb == SweepVerb::Delete {
                mutation = mutation.with_erase_caller(LEDGER_CALLER);
            }
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
