//! TTL sweep batch pacing, verb selection, expiry re-check, and erasure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};
use std::time::{Duration, Instant};

use fold_db::alternate_keys::MAX_ALTERNATE_ENTRIES;
use fold_db::clock::unix_secs;
use fold_db::db_operations::WriterWatch;
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

fn batch_size(host: &Host, backlog: &Backlog, pace_batch: usize, is_delete: bool) -> usize {
    let limit = host.db.db_ops().batch_operation_limit();
    let shape = backlog.shape;
    let (op_f, rd_f) = if is_delete { (5, 13) } else { (4, 12) };
    let width = shape.fields.max(1);
    let mut keys = (limit / width.saturating_mul(op_f))
        .min(40_000 / width.saturating_mul(rd_f))
        .min(MAX_ALTERNATE_ENTRIES / shape.alternates.max(1));
    while keys > 0 {
        let chunk_ops = keys.div_ceil(64).saturating_mul(shape.fields + 8);
        let ops = keys * shape.fields * op_f + chunk_ops + keys * (shape.alternates + 2);
        let reads = keys * shape.fields * rd_f + chunk_ops + keys * (shape.alternates * 4 + 2);
        if ops <= limit && reads <= 40_000 && keys * shape.alternates <= MAX_ALTERNATE_ENTRIES {
            break;
        }
        keys -= 1;
    }
    pace_batch.min(keys).min(backlog.batch_cap).max(1)
}

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
        Duration::from_millis(rest_ms).clamp(self.min_pause, MAX_PAUSE)
    }
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
    let is_delete = std::env::var(TTL_SWEEP_VERB_ENV)
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("delete"));
    let pace = Pace::from_env();
    let mut watch = WATCH
        .lock()
        .expect("ttl writer watch")
        .take()
        .unwrap_or_default();
    'rounds: while backlogs.iter().any(|b| !b.is_done()) {
        for b in &mut backlogs {
            if b.is_done() {
                continue;
            }
            if shutdown.load(Ordering::Acquire) {
                totals.cut = true;
                break 'rounds;
            }
            let sz = batch_size(host, b, pace.batch, is_delete);
            if let Err(error) = fill_page(host, b, sz).await {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", schema = %b.schema_name, error = %error,
                    "ttl discovery failed; skip this schema for this pass"
                );
                b.abandon();
                totals.schemas_failed += 1;
                continue;
            }
            if erase_page(
                host,
                b,
                shutdown,
                (is_delete, pace),
                &mut totals,
                &mut watch,
            )
            .await
            {
                totals.cut = true;
                break 'rounds;
            }
            b.release_page();
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
    (is_delete, pace): (bool, Pace),
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while backlog.page_left() > 0 {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let chunk = backlog.take(batch_size(host, backlog, pace.batch, is_delete));
        let taken = chunk.len();
        let started = Instant::now();
        let (outcome, blocked, scopes) = Box::pin(WriterWatch::measure(erase_batch(
            host, backlog, chunk, is_delete,
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
                    && [
                        "read resource limit exceeded",
                        "complete mutation limit exceeded",
                        "write resource limit",
                    ]
                    .iter()
                    .any(|pat| error.contains(pat)) =>
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

async fn erase_batch(
    host: &Host,
    backlog: &mut Backlog,
    chunk: Vec<KeyValue>,
    is_delete: bool,
) -> Result<(usize, usize, usize), String> {
    let listed = chunk.len();
    let Some(policy) = host
        .db
        .db_ops()
        .schemas()
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
            .filter(|k| key_is_expired(k, &backlog.schema_type, cutoff))
            .collect()
    };
    let dropped = listed - keys.len();
    let cutoff_nanos = hash_or_single.then(|| cutoff.saturating_mul(1_000_000_000));
    let mut missing = 0;
    loop {
        let Err(error) =
            purge_keys(host, &backlog.schema_name, &keys, is_delete, cutoff_nanos).await
        else {
            return Ok((keys.len(), dropped, missing));
        };
        if is_delete {
            return Err(error);
        }
        let absent = host
            .db
            .mutation_manager()
            .absent_erase_targets(&backlog.schema_name, &keys)
            .await
            .map_err(|c| format!("{error}; the absent-key check failed: {c}"))?;
        if absent.is_empty() {
            return Err(error);
        }
        missing += absent.len();
        keys.retain(|k| !absent.contains(k));
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
    is_delete: bool,
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
    let (mutation_type, caller, sync) = if is_delete {
        (MutationType::Delete, "ttl-sweep-delete", Some(true))
    } else {
        (MutationType::Purge, "ttl-sweep-purge", None)
    };
    let pubkey = host.public_key();
    let mutations = keys
        .iter()
        .map(|key| {
            let r = rows.remove(key).expect("every erasure key was read");
            let mut m = Mutation::with_uuid(
                uuid::Uuid::new_v4().to_string(),
                RecordTarget {
                    schema: schema_name.into(),
                    key: key.clone(),
                    alternates: r.alternates,
                },
                HashMap::new(),
                pubkey.clone(),
                mutation_type,
                RecordExpectation::Revision {
                    revision: r.revision,
                },
            );
            m.erase_caller = Some(caller);
            m.synchronous = sync;
            m.erase_written_before_nanos = write_cutoff_nanos;
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
