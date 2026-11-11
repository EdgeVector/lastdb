//! Batch pacing, sweep verb selection, and missing-key retry erasure.

use std::collections::HashMap;
use std::sync::Once;
use std::time::Duration;

use fold_db::schema::types::operations::{Mutation, MutationType};
use fold_db::schema::types::KeyValue;

use crate::host::Host;

const TTL_SWEEP_BATCH_ENV: &str = "LASTDB_TTL_SWEEP_BATCH";
const TTL_SWEEP_DUTY_PCT_ENV: &str = "LASTDB_TTL_SWEEP_DUTY_PCT";
const TTL_SWEEP_MIN_PAUSE_MS_ENV: &str = "LASTDB_TTL_SWEEP_MIN_PAUSE_MS";
const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

const DEFAULT_BATCH: usize = 16;
const MAX_BATCH: usize = 64;
const DEFAULT_DUTY_PCT: u64 = 25;
const DEFAULT_MIN_PAUSE_MS: u64 = 25;
const MAX_PAUSE: Duration = Duration::from_secs(600);

const DELETE_CALLER: &str = "ttl-sweep-delete";
const PURGE_CALLER: &str = "ttl-sweep-purge";

/// The tuned pacing values of one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pace {
    pub(super) batch: usize,
    duty_pct: u64,
    min_pause: Duration,
}

impl Pace {
    pub(super) fn from_env() -> Self {
        Self::new(
            fold_db::env_flag::var_or(TTL_SWEEP_BATCH_ENV, DEFAULT_BATCH),
            fold_db::env_flag::var_or(TTL_SWEEP_DUTY_PCT_ENV, DEFAULT_DUTY_PCT),
            fold_db::env_flag::var_or(TTL_SWEEP_MIN_PAUSE_MS_ENV, DEFAULT_MIN_PAUSE_MS),
        )
    }

    fn new(batch: usize, duty_pct: u64, min_pause_ms: u64) -> Self {
        Self {
            batch: batch.clamp(1, MAX_BATCH),
            duty_pct: duty_pct.clamp(1, 100),
            min_pause: Duration::from_millis(min_pause_ms).min(MAX_PAUSE),
        }
    }

    /// With a duty share of `d` percent, the rest is `blocked * (100 - d) / d`.
    pub(super) fn pause_after(&self, blocked: Duration, others_wrote: bool) -> Duration {
        if !others_wrote {
            return self.min_pause;
        }
        let work_ms = u64::try_from(blocked.as_millis()).unwrap_or(u64::MAX);
        let rest_ms = work_ms.saturating_mul(100 - self.duty_pct) / self.duty_pct;
        Duration::from_millis(rest_ms)
            .max(self.min_pause)
            .min(MAX_PAUSE)
    }
}

/// How the sweep erases an expired key (`purge` by default, or `delete`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepVerb {
    Delete,
    Purge,
}

impl SweepVerb {
    pub(super) fn from_env() -> Self {
        match std::env::var(TTL_SWEEP_VERB_ENV)
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some(raw) if raw.eq_ignore_ascii_case("delete") => Self::Delete,
            _ => Self::Purge,
        }
    }

    pub(super) fn caller(self) -> &'static str {
        match self {
            Self::Delete => DELETE_CALLER,
            Self::Purge => PURGE_CALLER,
        }
    }

    pub(super) fn mutation_type(self) -> MutationType {
        match self {
            Self::Delete => MutationType::Delete,
            Self::Purge => MutationType::Purge,
        }
    }

    pub(super) fn waits_for_lane(self) -> bool {
        self == Self::Delete
    }
}

/// Log one warning when the node environment names an unknown sweep verb.
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
                "unknown {TTL_SWEEP_VERB_ENV} value; the ttl sweep uses the default verb `purge` (valid: purge, delete)"
            );
        }
    });
}

/// Send `keys`, dropping any already-absent keys on a refused `Purge` batch.
pub(super) async fn send_with_missing_retry(
    host: &Host,
    schema_name: &str,
    mut keys: Vec<KeyValue>,
    (verb, write_cutoff_nanos): (SweepVerb, Option<u64>),
) -> Result<(usize, usize), String> {
    let mut missing = 0;
    loop {
        let error = match purge_keys(host, schema_name, &keys, verb, write_cutoff_nanos).await {
            Ok(()) => return Ok((keys.len(), missing)),
            Err(error) => error,
        };
        if verb != SweepVerb::Purge {
            return Err(error);
        }
        let absent = host
            .db
            .mutation_manager()
            .absent_erase_targets(schema_name, &keys)
            .await
            .map_err(|check| format!("{error}; the absent-key check failed: {check}"))?;
        if absent.is_empty() {
            return Err(error);
        }
        missing += absent.len();
        keys.retain(|key| !absent.contains(key));
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = schema_name,
            absent = absent.len(),
            kept = keys.len(),
            "ttl sweep drops keys that are already gone and sends the rest of the batch"
        );
        if keys.is_empty() {
            return Ok((0, missing));
        }
    }
}

async fn purge_keys(
    host: &Host,
    schema_name: &str,
    keys: &[KeyValue],
    verb: SweepVerb,
    write_cutoff_nanos: Option<u64>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let query = fold_db::schema::types::Query::new_with_filter(
        schema_name.to_string(),
        Some(
            fold_db::schema::types::field::HashRangeFilter::HashRangeKeys(
                keys.iter()
                    .map(|key| {
                        (
                            key.hash.clone().unwrap_or_default(),
                            key.range.clone().unwrap_or_default(),
                        )
                    })
                    .collect(),
            ),
        ),
    );
    let mut rows = host
        .db
        .query_executor()
        .query(query)
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() != keys.len() {
        return Err("erasure target disappeared before preparation".into());
    }
    let mutations: Vec<Mutation> = keys
        .iter()
        .map(|key| {
            let record = rows.remove(key).expect("every erasure key was read");
            let target = fold_db::record_replacement::RecordTarget {
                schema: schema_name.to_string(),
                key: key.clone(),
                alternates: record.alternates,
            };
            let mut mutation = Mutation::with_uuid(
                uuid::Uuid::new_v4().to_string(),
                target,
                HashMap::new(),
                host.public_key(),
                verb.mutation_type(),
                fold_db::record_replacement::RecordExpectation::Revision {
                    revision: record.revision,
                },
            );
            mutation.synchronous = verb.waits_for_lane().then_some(true);
            mutation = mutation.with_erase_caller(verb.caller());
            if let Some(nanos) = write_cutoff_nanos {
                mutation = mutation.with_erase_written_before_nanos(nanos);
            }
            mutation
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations(mutations)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
