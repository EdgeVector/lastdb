//! TTL sweep batch pacing, verb selection, expiry re-check, and erasure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};
use std::time::{Duration, Instant};

use fold_db::clock::unix_secs;
use fold_db::db_operations::WriterWatch;
use fold_db::fold_db_core::purge::budget::max_erase_keys;
use fold_db::record_replacement::{RecordExpectation, RecordTarget};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Mutation, MutationType};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{KeyValue, Query};

use super::{fill_page, key_is_expired, Backlog};
use crate::host::Host;

static WATCH: Mutex<Option<WriterWatch>> = Mutex::new(None);
const REST_SLICE: Duration = Duration::from_millis(250);
const MAX_PAUSE: Duration = Duration::from_secs(600);
const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PassTotals {
    pub(super) erased: usize,
    pub(super) dropped: usize,
    pub(super) keys_left: usize,
    pub(super) cut: bool,
    pub(super) schemas: usize,
    pub(super) schemas_failed: usize,
}

#[derive(Clone, Copy)]
struct Pace {
    batch: usize,
    duty_pct: u64,
    min_pause: Duration,
}

impl Pace {
    fn from_env() -> Self {
        let min_ms = fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_MIN_PAUSE_MS", 25u64);
        Self {
            batch: fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_BATCH", 16usize).clamp(1, 64),
            duty_pct: fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_DUTY_PCT", 25u64).clamp(1, 100),
            min_pause: Duration::from_millis(min_ms).min(MAX_PAUSE),
        }
    }

    fn pause_after(&self, blocked: Duration, others_wrote: bool) -> Duration {
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum SweepVerb {
    Delete,
    Purge,
}

pub(super) fn warn_on_bad_verb_once() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        if let Ok(raw) = std::env::var(TTL_SWEEP_VERB_ENV) {
            let v = raw.trim();
            if !v.is_empty()
                && !v.eq_ignore_ascii_case("delete")
                && !v.eq_ignore_ascii_case("purge")
            {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", value = v,
                    "unknown {TTL_SWEEP_VERB_ENV} value; defaulting to `purge`"
                );
            }
        }
    });
}

pub(super) async fn purge_round_robin(
    host: &Host,
    mut backlogs: Vec<Backlog>,
    shutdown: &AtomicBool,
    mut totals: PassTotals,
) -> PassTotals {
    let verb = if std::env::var(TTL_SWEEP_VERB_ENV)
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("delete"))
    {
        SweepVerb::Delete
    } else {
        SweepVerb::Purge
    };
    let pace = Pace::from_env();
    let mut watch = WATCH
        .lock()
        .expect("ttl writer watch")
        .take()
        .unwrap_or_default();
    'rounds: while backlogs.iter().any(|b| !b.is_done()) {
        for backlog in &mut backlogs {
            if backlog.is_done() {
                continue;
            }
            if shutdown.load(Ordering::Acquire) {
                totals.cut = true;
                break 'rounds;
            }
            if let Err(error) =
                fill_page(host, backlog, batch_size(host, backlog, pace.batch, verb)).await
            {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", schema = %backlog.schema_name, error = %error,
                    "ttl discovery failed; skip this schema for this pass"
                );
                backlog.abandon();
                totals.schemas_failed += 1;
                continue;
            }
            if erase_page(
                host,
                backlog,
                shutdown,
                (verb, pace),
                &mut totals,
                &mut watch,
            )
            .await
            {
                totals.cut = true;
                break 'rounds;
            }
            backlog.release_page();
        }
    }
    *WATCH.lock().expect("ttl writer watch") = Some(watch);
    totals.keys_left = backlogs.iter().map(Backlog::left).sum();
    totals
}

async fn erase_page(
    host: &Host,
    backlog: &mut Backlog,
    shutdown: &AtomicBool,
    (verb, pace): (SweepVerb, Pace),
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while backlog.page_left() > 0 {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let chunk = backlog.take(batch_size(host, backlog, pace.batch, verb));
        let taken = chunk.len();
        let started = Instant::now();
        let (outcome, blocked, scopes) = Box::pin(WriterWatch::measure(erase_batch(
            host, backlog, chunk, verb,
        )))
        .await;
        let pause = pace.pause_after(blocked, watch.writer_present(scopes));
        match outcome {
            Ok((erased, dropped, missing)) => {
                totals.erased += erased;
                totals.dropped += dropped;
                if dropped > 0 {
                    tracing::info!(
                        target: "lastdbd::ttl_sweep", schema = %backlog.schema_name, dropped, erased,
                        "ttl sweep re-check dropped keys that are no longer expired"
                    );
                }
                tracing::debug!(
                    target: "lastdbd::ttl_sweep", schema = %backlog.schema_name, keys = erased,
                    dropped, missing, took_ms = started.elapsed().as_millis() as u64,
                    "ttl sweep batch erased"
                );
            }
            Err(error)
                if taken > 1
                    && (error.contains("read resource limit exceeded")
                        || error.contains("complete mutation limit exceeded")
                        || error.contains("write resource limit")) =>
            {
                backlog.shrink_after_limit(taken);
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", schema = %backlog.schema_name, keys = taken, error = %error,
                    "ttl size refusal; retry a smaller batch"
                );
            }
            Err(error) => {
                backlog.shrink_after_limit(taken);
                backlog.abandon();
                totals.schemas_failed += 1;
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", schema = %backlog.schema_name, error = %error,
                    "ttl erase failed; skip this schema for this pass"
                );
                return false;
            }
        }
        if !backlog.is_done() {
            let end = Instant::now() + pause;
            while !shutdown.load(Ordering::Acquire) {
                let left = end.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                tokio::time::sleep(left.min(REST_SLICE)).await;
                if left > REST_SLICE && watch.rest_may_end() {
                    break;
                }
            }
        }
    }
    false
}

fn batch_size(host: &Host, backlog: &Backlog, pace_batch: usize, verb: SweepVerb) -> usize {
    let fits = max_erase_keys(
        verb == SweepVerb::Delete,
        backlog.shape,
        host.db.db_ops().batch_operation_limit(),
    );
    pace_batch.min(fits).min(backlog.batch_cap).max(1)
}

async fn erase_batch(
    host: &Host,
    backlog: &mut Backlog,
    chunk: Vec<KeyValue>,
    verb: SweepVerb,
) -> Result<(usize, usize, usize), String> {
    let listed = chunk.len();
    let Some(policy) = host
        .db
        .db_ops()
        .get_schema_retention_policy(&backlog.schema_name)
        .await
        .map_err(|e| e.to_string())?
    else {
        backlog.clear();
        tracing::info!(
            target: "lastdbd::ttl_sweep", schema = %backlog.schema_name,
            "retention policy is gone; dropping rest of schema"
        );
        return Ok((0, listed, 0));
    };
    let cutoff = backlog
        .cutoff
        .min(unix_secs().saturating_sub(policy.ttl_seconds));
    let hash_or_single = matches!(
        backlog.schema_type,
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
    );
    let mut keys = if hash_or_single {
        host.db
            .db_ops()
            .schemas()
            .retention_keys_still_expired(&backlog.schema_name, &chunk, cutoff)
            .await
            .map_err(|e| e.to_string())?
    } else {
        chunk
            .into_iter()
            .filter(|key| key_is_expired(key, &backlog.schema_type, cutoff))
            .collect()
    };
    let dropped = listed - keys.len();
    let cutoff_nanos = hash_or_single.then(|| cutoff.saturating_mul(1_000_000_000));
    let mut missing = 0;
    loop {
        let Err(error) = purge_keys(host, &backlog.schema_name, &keys, verb, cutoff_nanos).await
        else {
            return Ok((keys.len(), dropped, missing));
        };
        if verb != SweepVerb::Purge {
            return Err(error);
        }
        let absent = host
            .db
            .mutation_manager()
            .absent_erase_targets(&backlog.schema_name, &keys)
            .await
            .map_err(|check| format!("{error}; the absent-key check failed: {check}"))?;
        if absent.is_empty() {
            return Err(error);
        }
        missing += absent.len();
        keys.retain(|key| !absent.contains(key));
        tracing::info!(
            target: "lastdbd::ttl_sweep", schema = %backlog.schema_name,
            absent = absent.len(), kept = keys.len(),
            "ttl sweep dropped already-absent keys and retries remaining batch"
        );
        if keys.is_empty() {
            return Ok((0, dropped, missing));
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
    let pairs = keys
        .iter()
        .map(|k| {
            (
                k.hash.clone().unwrap_or_default(),
                k.range.clone().unwrap_or_default(),
            )
        })
        .collect();
    let query = Query::new_with_filter(
        schema_name.into(),
        Some(HashRangeFilter::HashRangeKeys(pairs)),
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
    let (mutation_type, caller, sync) = match verb {
        SweepVerb::Delete => (MutationType::Delete, "ttl-sweep-delete", Some(true)),
        SweepVerb::Purge => (MutationType::Purge, "ttl-sweep-purge", None),
    };
    let pubkey = host.public_key();
    let mutations: Vec<Mutation> = keys
        .iter()
        .map(|key| {
            let record = rows.remove(key).expect("every erasure key was read");
            let target = RecordTarget {
                schema: schema_name.to_string(),
                key: key.clone(),
                alternates: record.alternates,
            };
            let mut m = Mutation::with_uuid(
                uuid::Uuid::new_v4().to_string(),
                target,
                HashMap::new(),
                pubkey.clone(),
                mutation_type,
                RecordExpectation::Revision {
                    revision: record.revision,
                },
            );
            m.synchronous = sync;
            m = m.with_erase_caller(caller);
            if let Some(nanos) = write_cutoff_nanos {
                m = m.with_erase_written_before_nanos(nanos);
            }
            m
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations(mutations)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}
